package ipc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"time"
)

// ConnectionClosed is returned (wrapped) when writing to a Conn whose client is gone.
var ConnectionClosed = errors.New("the connection has been closed")

// ID - returns the connection's ID, unique for the lifetime of the Server.
func (c *Conn) ID() uint64 {
	return c.id
}

// Peer - returns the client's identity. On error treat the client as unauthenticated.
func (c *Conn) Peer() (PeerInfo, error) {
	return c.peer, c.peerErr
}

// Write - writes a message to this client only.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
func (c *Conn) Write(msgType int, message []byte) error {
	return c.WriteWithContext(context.Background(), msgType, message)
}

// WriteWithContext - writes a message to this client only, but it reacts to end of context.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
// The message is copied, so the caller may reuse it.
func (c *Conn) WriteWithContext(ctx context.Context, msgType int, message []byte) error {

	if err := checkWrite(msgType, message, c.server.maxMsgSize); err != nil {
		return err
	}

	return c.enqueue(ctx, &Message{MsgType: msgType, Data: bytes.Clone(message)})
}

// m must not be modified afterwards.
func (c *Conn) enqueue(ctx context.Context, m *Message) error {

	if c.StatusCode() != Connected {
		return fmt.Errorf("%w (%s)", ConnectionClosed, c.Status())
	}

	select {
	case c.toWrite <- m:
	case <-c.ctx.Done():
		return ConnectionClosed
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

// Close - closes this client connection only.
func (c *Conn) Close() {
	c.drop()
}

func (c *Conn) drop() {
	c.closedByServer.Store(true)
	c.cancel()
}

// StatusCode - returns Connected, then Disconnected if the client left or Closed if the server ended it.
func (c *Conn) StatusCode() Status {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status
}

// Status - returns the status of this connection as a string
func (c *Conn) Status() string {
	c.statusMu.RLock()
	defer c.statusMu.RUnlock()
	return c.status.String()
}

func (c *Conn) setStatusCode(status Status) {
	c.statusMu.Lock()
	defer c.statusMu.Unlock()
	c.status = status
}

func (c *Conn) queue(ctx context.Context, m *Message) bool {

	m.Conn = c

	select {
	case c.events <- m:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *Conn) read(ctx context.Context) {

	slog.Debug("Starting read for new connection", "id", c.id)

	for {
		msgType, data, err := readFrame(c.conn, c.enc, c.server.maxMsgSize)

		switch {
		case errors.Is(err, errUndecryptable):
			if !c.queue(ctx, &Message{Err: err, MsgType: -1}) {
				return
			}
		case errors.Is(err, errFrameTooLarge), errors.Is(err, errFrameTooShort):
			slog.Error("Closing connection due to an invalid message", "id", c.id, "err", err)
			c.closedByServer.Store(true)
			return
		case err != nil:
			c.readError(ctx, err)
			return
		case deliverable(msgType):
			if !c.queue(ctx, &Message{Data: data, MsgType: int(msgType)}) {
				return
			}
		}
	}
}

func (c *Conn) readError(ctx context.Context, err error) {

	switch {
	case ctx.Err() != nil || errors.Is(err, net.ErrClosed):
		slog.Debug("Stopping read due to closing connection", "id", c.id)
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		slog.Debug("Stopping read due to EOF", "id", c.id)
	default:
		slog.Error("Unable to read data from client", "id", c.id, "err", err)
	}
}

func (c *Conn) write(ctx context.Context) {

	slog.Debug("Starting write for new connection", "id", c.id)

	for {
		var m *Message

		select {
		case <-ctx.Done():
			slog.Debug("Stopping write due to context cancellation", "id", c.id)
			return
		case m = <-c.toWrite:
		}

		_ = c.conn.SetWriteDeadline(time.Now().Add(c.server.writeTimeout))

		if err := writeFrame(c.conn, c.enc, m.MsgType, m.Data); err != nil {
			// A partial frame cannot be recovered, and a client that does not read must not hold up writers.
			if ctx.Err() == nil {
				slog.Error("Closing connection as data could not be written", "id", c.id, "err", err)
			}
			c.drop()
			return
		}
	}
}
