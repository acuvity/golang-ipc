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
func StartServer(ipcName string, config *ServerConfig) (*Server, error) {

	if err := checkIpcName(ipcName); err != nil {
		return nil, err
	}

	s := &Server{
		name:     ipcName,
		status:   NotConnected,
		received: make(chan *Message),
		toWrite:  make(chan *Message),
	}

	s.maxMsgSize = maxMsgSize
	s.encryption = true

	if config != nil {
		if config.MaxMsgSize >= 1024 && config.MaxMsgSize < maxMsgSize {
			s.maxMsgSize = config.MaxMsgSize
		}

		s.encryption = config.Encryption
		s.unMask = config.UnmaskPermissions
	}

	return s, s.run()
}

func (s *Server) acceptLoop() {

	defer func() { _ = s.listen.Close() }()

	for {
		conn, err := s.listen.Accept()
		if err != nil {
			if !errors.Is(err, net.ErrClosed) {
				slog.Error("Unable to accept next connection to listener", "err", err)
			}
			return
		}

		if s.StatusCode() != Listening && s.StatusCode() != Disconnected {
			slog.Warn("Closing connection as status is not ready", "status", s.Status())
			_ = conn.Close()
			continue
		}

		s.conn = conn

		if err = s.handshake(); err != nil {
			slog.Error("Closing connection due to handshake error", "err", err)
			s.received <- &Message{Err: err, MsgType: -1}
			_ = s.conn.Close()
			continue
		}

		go s.read()
		go s.write()

		s.setStatusCode(Connected)
		s.received <- &Message{Status: s.Status(), MsgType: -1}
	}
}

func (s *Server) read() {

	defer func() { _ = s.conn.Close() }()

	bLen := make([]byte, 4)

	for {
		if res := s.readData(bLen); !res {
			return
		}

		mLen := bytesToInt(bLen)

		msgRecvd := make([]byte, mLen)

		if res := s.readData(msgRecvd); !res {
			return
		}

		if s.encryption {
			msgFinal, err := decrypt(*s.enc.cipher, msgRecvd)
			if err != nil {
				s.received <- &Message{Err: err, MsgType: -1}
				continue
			}

			if bytesToInt(msgFinal[:4]) != 0 {
				s.received <- &Message{Data: msgFinal[4:], MsgType: bytesToInt(msgFinal[:4])}
			}
			continue
		}

		if bytesToInt(msgRecvd[:4]) != 0 {
			s.received <- &Message{Data: msgRecvd[4:], MsgType: bytesToInt(msgRecvd[:4])}
		}
	}
}

func (s *Server) readData(buff []byte) bool {

	if _, err := io.ReadFull(s.conn, buff); err != nil {

		if s.StatusCode() == Closing {
			s.setStatusCode(Closed)
			s.received <- &Message{Status: s.Status(), MsgType: -1}
			s.received <- &Message{Err: ServerConnectionClosed, MsgType: -1}
			return false
		}

		if err == io.EOF {
			s.setStatusCode(Disconnected)
			s.received <- &Message{Status: s.Status(), MsgType: -1}
			return false
		}

		slog.Error("Unable to read data", "err", err)
	}

	return true
}

// Read - blocking function, reads each message received
// if MsgType is a negative number its an internal message
func (s *Server) Read() (*Message, error) {

	m, ok := <-s.received
	if !ok {
		return nil, errors.New("the received channel has been closed")
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
			return nil, errors.New("the received channel has been closed")
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

func (s *Server) write() {

	for {
		m, ok := <-s.toWrite
		if !ok {
			slog.Info("Closing write channel")
			return
		}

		toSend := intToBytes(m.MsgType)

		writer := bufio.NewWriter(s.conn)

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

		writer.Write(intToBytes(len(toSend)))
		writer.Write(toSend)

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

	if s.conn != nil {
		_ = s.conn.Close()
	}
}
