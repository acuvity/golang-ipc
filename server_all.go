package ipc

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"sort"
	"sync"
	"time"
)

// ServerConnectionClosed is the error returned by Read() when the server has been closed.
var ServerConnectionClosed = errors.New("server has closed the connection")

var handshakeTimeout = 10 * time.Second

const eventQueueSize = 16

// StartServer - starts the ipc server.
//
// ipcName - is the name of the unix socket or named pipe that will be created, the client needs to use the same name
func StartServer(ctx context.Context, ipcName string, config *ServerConfig) (*Server, error) {

	if err := checkIpcName(ipcName); err != nil {
		return nil, err
	}

	subctx, cancel := context.WithCancel(ctx)

	s := &Server{
		name:         ipcName,
		status:       NotConnected,
		cancel:       cancel,
		maxMsgSize:   maxMsgSize,
		encryption:   true,
		maxConns:     DefaultMaxConnections,
		writeTimeout: DefaultWriteTimeout,
		received:     make(chan *Message),
		conns:        map[uint64]*Conn{},
	}

	if config != nil {
		if config.MaxMsgSize >= 1024 && config.MaxMsgSize < maxMsgSize {
			s.maxMsgSize = config.MaxMsgSize
		}

		if config.MaxConnections > 0 {
			s.maxConns = config.MaxConnections
		}

		if config.WriteTimeout > 0 {
			s.writeTimeout = config.WriteTimeout
		}

		s.encryption = config.Encryption
		s.unMask = config.UnmaskPermissions
		s.admit = config.AdmitClient
	}

	// Returned even on error so callers can defer Close().
	if err := s.run(subctx); err != nil {
		cancel()
		return s, err
	}

	return s, nil
}

func (s *Server) sendMessage(ctx context.Context, msg *Message) {

	select {
	case s.received <- msg:
	case <-ctx.Done():
	}
}

func (s *Server) serve(ctx context.Context) {

	stop := context.AfterFunc(ctx, func() { _ = s.listen.Close() })

	s.acceptLoop(ctx)

	stop()
	s.shutdown()

	s.setStatusCode(Closed)

	// The context has been canceled so let's give these messages a chance
	// to be sent but don't wait long
	subctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
	defer cancel()
	s.sendMessage(subctx, &Message{Status: s.Status(), MsgType: -1})
	s.sendMessage(subctx, &Message{Err: ServerConnectionClosed, MsgType: -1})
}

func (s *Server) acceptLoop(ctx context.Context) {

	var delay time.Duration

	for {
		nc, err := s.listen.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}

			// Temporary errors (e.g. out of file descriptors) must not stop the server.
			delay = min(max(2*delay, 5*time.Millisecond), time.Second)
			slog.Error("Unable to accept next connection to listener, retrying", "err", err, "delay", delay)

			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return
			}
			continue
		}

		delay = 0

		// Under the lock so that Close either waits for this connection or it is refused.
		s.Lock()
		if s.status == Closing || s.status == Closed || ctx.Err() != nil {
			s.Unlock()
			_ = nc.Close()
			return
		}
		if s.openConns >= s.maxConns {
			s.Unlock()
			slog.Warn("Rejecting connection as the maximum number of connections is reached", "max", s.maxConns)
			_ = nc.Close()
			continue
		}
		s.openConns++
		s.nextID++
		id := s.nextID
		s.wg.Add(1)
		s.Unlock()

		go func() {
			defer s.wg.Done()
			s.serveConn(ctx, id, nc)
		}()
	}
}

func (s *Server) serveConn(ctx context.Context, id uint64, nc net.Conn) {

	release := sync.OnceFunc(func() {
		s.Lock()
		s.openConns--
		s.Unlock()
	})
	defer release()

	// Before the handshake: the PID is only reliable while the connecting process is alive.
	peer, peerErr := peerInfo(nc)

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Completed before returning so Close releases every handle: Windows cannot recreate an open pipe.
	closeConn := sync.OnceFunc(func() { _ = nc.Close() })
	stop := context.AfterFunc(connCtx, closeConn)
	defer func() {
		stop()
		closeConn()
	}()

	_ = nc.SetDeadline(time.Now().Add(handshakeTimeout))

	enc, err := s.handshake(nc)
	if err != nil {
		// Free the handle before waiting for the application to read the error.
		closeConn()
		release()
		if ctx.Err() == nil {
			slog.Error("Closing connection due to handshake error", "err", err)
			s.sendMessage(ctx, &Message{Err: err, MsgType: -1})
		}
		return
	}

	_ = nc.SetDeadline(time.Time{})

	if peerErr != nil {
		slog.Warn("Unable to identify the client of the connection", "err", peerErr)
	}

	c := &Conn{
		id:      id,
		server:  s,
		conn:    nc,
		enc:     enc,
		peer:    peer,
		peerErr: peerErr,
		toWrite: make(chan *Message),
		events:  make(chan *Message, eventQueueSize),
		ctx:     connCtx,
		cancel:  cancel,
		status:  Connected,
	}

	if s.admit != nil {
		if err := s.admit(c); err != nil {
			slog.Warn("Client was not admitted", "id", id, "uid", peer.UID, "pid", peer.PID, "err", err)
			return
		}
	}

	if connCtx.Err() != nil {
		return
	}

	// Keeps the connection read (and departures noticed) while the application is not reading.
	deliverDone := make(chan struct{})
	go func() {
		defer close(deliverDone)
		s.deliver(ctx, c)
	}()

	// The queue is empty, so this cannot block.
	c.events <- &Message{Status: c.Status(), MsgType: -1, Conn: c}

	s.addConn(c)

	slog.Debug("Client connected", "id", id, "uid", peer.UID, "pid", peer.PID, "session", peer.SessionID)

	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		c.write(connCtx)
	}()

	c.read(connCtx)

	cancel()
	closeConn()
	<-writerDone
	release()
	s.removeConn(c)

	status := Disconnected
	if ctx.Err() != nil || c.closedByServer.Load() {
		status = Closed
	}
	c.setStatusCode(status)

	// On shutdown serve sends a single Closed event instead.
	if ctx.Err() == nil {
		slog.Debug("Client disconnected", "id", id, "status", c.Status())
		select {
		case c.events <- &Message{Status: c.Status(), MsgType: -1, Conn: c}:
		case <-ctx.Done():
		}
	}

	close(c.events)
	<-deliverDone
}

func (s *Server) deliver(ctx context.Context, c *Conn) {

	for m := range c.events {
		select {
		case s.received <- m:
		case <-ctx.Done():
			// Drain so serveConn can finish.
			for range c.events {
			}
			return
		}
	}
}

func (s *Server) addConn(c *Conn) {

	s.Lock()
	defer s.Unlock()

	s.conns[c.id] = c

	switch s.status {
	case Closing, Closed:
	default:
		s.status = Connected
	}
}

func (s *Server) removeConn(c *Conn) {

	s.Lock()
	defer s.Unlock()

	delete(s.conns, c.id)

	if len(s.conns) == 0 && s.status == Connected {
		s.status = Disconnected
	}
}

// Connections - returns the connected clients, ordered by ID.
func (s *Server) Connections() []*Conn {

	s.RLock()
	defer s.RUnlock()

	conns := make([]*Conn, 0, len(s.conns))
	for _, c := range s.conns {
		conns = append(conns, c)
	}

	sort.Slice(conns, func(i, j int) bool { return conns[i].id < conns[j].id })

	return conns
}

// Read - blocking function, reads each message received from any client.
// if MsgType is a negative number its an internal message.
// Errors caused by a single client are a *ConnError.
func (s *Server) Read() (*Message, error) {

	m, ok := <-s.received
	if !ok {
		return nil, ChannelClosedReceived
	}

	return readResult(m)
}

// ReadWithContext - blocking function, but it reacts to end of context, reads each message received
// if MsgType is a negative number it's an internal message
// Errors caused by a single client are a *ConnError.
func (s *Server) ReadWithContext(ctx context.Context) (*Message, error) {
	select {
	case m, ok := <-s.received:
		if !ok {
			return nil, ChannelClosedReceived
		}

		return readResult(m)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func readResult(m *Message) (*Message, error) {

	if m.Err == nil {
		return m, nil
	}

	if m.Conn != nil {
		return nil, &ConnError{Conn: m.Conn, Err: m.Err}
	}

	return nil, m.Err
}

// Broadcast - sends a message to every connected client. Use Message.Conn to reply to one client.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
func (s *Server) Broadcast(msgType int, message []byte) error {
	return s.BroadcastWithContext(context.Background(), msgType, message)
}

// BroadcastWithContext - sends a message to every connected client, but it reacts to end of context.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
// The returned error joins the errors of every client that could not be written to.
func (s *Server) BroadcastWithContext(ctx context.Context, msgType int, message []byte) error {

	if err := checkWrite(msgType, message, s.maxMsgSize); err != nil {
		return err
	}

	if s.StatusCode() != Connected {
		return errors.New(s.Status())
	}

	conns := s.Connections()
	if len(conns) == 0 {
		return errors.New(s.Status())
	}

	data := bytes.Clone(message)

	errs := make([]error, len(conns))

	var wg sync.WaitGroup
	for i, c := range conns {
		wg.Go(func() {
			errs[i] = c.enqueue(ctx, &Message{MsgType: msgType, Data: data})
		})
	}
	wg.Wait()

	return errors.Join(errs...)
}

func checkWrite(msgType int, message []byte, maxSize int) error {

	if msgType == 0 {
		return errors.New("message type 0 is reserved")
	}

	if len(message) > maxSize {
		return errors.New("message exceeds maximum message length")
	}

	return nil
}

// setStatusCode - sets the current server status
func (s *Server) setStatusCode(status Status) {
	s.Lock()
	defer s.Unlock()
	s.status = status
}

// StatusCode - returns the current server status.
// It is Connected while at least one client is connected.
func (s *Server) StatusCode() Status {
	s.RLock()
	defer s.RUnlock()
	return s.status
}

// Status - returns the current server status as a string
func (s *Server) Status() string {
	s.RLock()
	defer s.RUnlock()
	return s.status.String()
}

// Close - stops the server and closes every client connection.
// It returns once everything is released, so a server with the same name can be started at once.
func (s *Server) Close() {

	s.shutdown()

	if s.removeSocket != nil {
		s.removeSocket()
	}
}

func (s *Server) shutdown() {

	s.Lock()
	if s.status != Closed {
		s.status = Closing
	}
	s.Unlock()

	if s.cancel != nil {
		s.cancel()
	}

	if s.listen != nil {
		_ = s.listen.Close()
	}

	s.wg.Wait()
}
