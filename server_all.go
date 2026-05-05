package ipc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"
)

// ServerConnectionClosed is the error returned by Read() when the connection has closed.
var ServerConnectionClosed = errors.New("server has closed the connection")

// StartServer - starts the ipc server.
//
// ipcName - is the name of the unix socket or named pipe that will be created, the client needs to use the same name
func StartServer(ctx context.Context, ipcName string, config *ServerConfig) (*Server, error) {

	if err := checkIpcName(ipcName); err != nil {
		return nil, err
	}

	subctx, cancel := context.WithCancel(ctx)

	s := &Server{
		name:       ipcName,
		status:     NotConnected,
		cancel:     cancel,
		maxMsgSize: maxMsgSize,
		encryption: true,
		received:   make(chan *Message),
		toWrite:    make(chan *Message),
	}

	if config != nil {
		if config.MaxMsgSize >= 1024 && config.MaxMsgSize < maxMsgSize {
			s.maxMsgSize = config.MaxMsgSize
		}

		s.encryption = config.Encryption
		s.unMask = config.UnmaskPermissions
	}

	return s, s.run(subctx)
}

func (s *Server) sendMessage(ctx context.Context, sendTo chan *Message, msg *Message) {

	select {
	case sendTo <- msg:
	case <-ctx.Done():
	}
}

func (s *Server) acceptLoop(ctx context.Context) {

	defer func() { _ = s.listen.Close() }()

	for {
		conn, err := s.listen.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Error("Unable to accept next connection to listener", "err", err)
			}
			return
		}

		switch s.StatusCode() {
		case Listening, Disconnected:
		case Closing, Closed:
			return
		default:
			slog.Warn("Closing connection as status is not ready", "status", s.Status())
			_ = conn.Close()
			continue
		}

		if err = s.handshake(conn); err != nil {
			slog.Error("Closing connection due to handshake error", "err", err)
			s.sendMessage(ctx, s.received, &Message{Err: err, MsgType: -1})
			_ = conn.Close()
			continue
		}

		go s.startReadWrite(ctx, conn)
	}
}

func (s *Server) startReadWrite(ctx context.Context, conn net.Conn) {

	defer func() { _ = conn.Close() }()

	// This is to support the scenario where main context
	// is still active but the connection closes.
	readctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		s.read(ctx, conn)
		cancel()
	}()
	go s.write(readctx, conn)

	s.setStatusCode(Connected)
	s.sendMessage(ctx, s.received, &Message{Status: s.Status(), MsgType: -1})

	select {
	case <-ctx.Done():
	case <-readctx.Done():
	}
}

func (s *Server) read(ctx context.Context, conn net.Conn) {

	slog.Debug("Starting read for new connection")

	bLen := make([]byte, 4)

	for {
		if res := s.readData(ctx, conn, bLen); !res {
			return
		}

		mLen := bytesToInt(bLen)

		msgRecvd := make([]byte, mLen)

		if res := s.readData(ctx, conn, msgRecvd); !res {
			return
		}

		if s.encryption {
			msgFinal, err := decrypt(*s.enc.cipher, msgRecvd)
			if err != nil {
				s.sendMessage(ctx, s.received, &Message{Err: err, MsgType: -1})
				continue
			}

			if bytesToInt(msgFinal[:4]) != 0 {
				s.sendMessage(ctx, s.received, &Message{Data: msgFinal[4:], MsgType: bytesToInt(msgFinal[:4])})
			}
			continue
		}

		if bytesToInt(msgRecvd[:4]) != 0 {
			s.sendMessage(ctx, s.received, &Message{Data: msgRecvd[4:], MsgType: bytesToInt(msgRecvd[:4])})
		}
	}
}

func (s *Server) readData(ctx context.Context, conn net.Conn, buff []byte) bool {

	if _, err := io.ReadFull(conn, buff); err != nil {

		if s.StatusCode() == Closing || errors.Is(err, net.ErrClosed) {
			slog.Debug("Stopping read due to closing connection")
			s.setStatusCode(Closed)
			// The context has been canceled so let's give these messages a chance
			// to be sent but don't wait long
			subctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			s.sendMessage(subctx, s.received, &Message{Status: s.Status(), MsgType: -1})
			s.sendMessage(subctx, s.received, &Message{Err: ServerConnectionClosed, MsgType: -1})
			return false
		}

		if errors.Is(err, io.EOF) {
			slog.Debug("Stopping read due to EOF", "status", s.Status())
			s.setStatusCode(Disconnected)
			s.sendMessage(ctx, s.received, &Message{Status: s.Status(), MsgType: -1})
			return false
		}

		slog.Error("Unable to read data from client", "err", err)
	}

	return true
}

// Read - blocking function, reads each message received
// if MsgType is a negative number its an internal message
func (s *Server) Read() (*Message, error) {

	m, ok := <-s.received
	if !ok {
		return nil, ChannelClosedReceived
	}

	if m.Err != nil {
		return nil, m.Err
	}

	return m, nil
}

// ReadWithContext - blocking function, but it reacts to end of context, reads each message received
// if MsgType is a negative number it's an internal message
func (s *Server) ReadWithContext(ctx context.Context) (*Message, error) {
	select {
	case m, ok := <-s.received:
		if !ok {
			return nil, ChannelClosedReceived
		}

		if m.Err != nil {
			return nil, m.Err
		}

		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Write - writes a message to the ipc connection
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
func (s *Server) Write(msgType int, message []byte) error {

	if msgType == 0 {
		return errors.New("message type 0 is reserved")
	}

	if len(message) > s.maxMsgSize {
		return errors.New("message exceeds maximum message length")
	}

	if s.StatusCode() != Connected {
		return errors.New(s.Status())
	}

	s.toWrite <- &Message{MsgType: msgType, Data: message}

	return nil
}

// WriteWithContext - writes a message to the ipc connection, but it reacts to end of context.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
func (s *Server) WriteWithContext(ctx context.Context, msgType int, message []byte) error {

	if msgType == 0 {
		return errors.New("message type 0 is reserved")
	}

	if len(message) > s.maxMsgSize {
		return errors.New("message exceeds maximum message length")
	}

	if s.StatusCode() != Connected {
		return errors.New(s.Status())
	}

	select {
	case s.toWrite <- &Message{MsgType: msgType, Data: message}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (s *Server) write(ctx context.Context, conn net.Conn) {
	var m *Message
	var ok bool

	slog.Debug("Starting write for new connection")

	for {
		select {
		case <-ctx.Done():
			slog.Debug("Stopping write due to context cancellation", "status", s.Status())
			return
		case m, ok = <-s.toWrite:
		}

		if !ok {
			slog.Info("Stopping write as channel is closed")
			return
		}

		toSend := intToBytes(m.MsgType)

		writer := bufio.NewWriter(conn)

		if s.encryption {
			toSend = append(toSend, m.Data...)
			toSendEnc, err := encrypt(*s.enc.cipher, toSend)
			if err != nil {
				slog.Error("Unable to encrypt data", "err", err)
				continue
			}
			toSend = toSendEnc
		} else {
			toSend = append(toSend, m.Data...)
		}

		_, _ = writer.Write(intToBytes(len(toSend)))
		_, _ = writer.Write(toSend)

		if err := writer.Flush(); err != nil {
			slog.Error("Unable to flush data", "err", err)
			continue
		}

		time.Sleep(2 * time.Millisecond)
	}
}

// setStatusCode - sets the current connection status
func (s *Server) setStatusCode(status Status) {
	s.Lock()
	defer s.Unlock()
	s.status = status
}

// StatusCode - returns the current connection status
func (s *Server) StatusCode() Status {
	s.RLock()
	defer s.RUnlock()
	return s.status
}

// Status - returns the current connection status as a string
func (s *Server) Status() string {
	s.RLock()
	defer s.RUnlock()
	return s.status.String()
}

// Close - closes the connection
func (s *Server) Close() {

	s.setStatusCode(Closing)

	if s.listen != nil {
		_ = s.listen.Close()
	}

	if s.cancel != nil {
		s.cancel()
	}
}
