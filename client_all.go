package ipc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"time"
)

// ClientConnectionClosed is the error returned by Read() when the connection has closed.
var ClientConnectionClosed = errors.New("client has closed the connection")

// Connections shorter than this count as dropped straight away.
const quickDrop = time.Second

// StartClient - start the ipc client.
// ipcName = is the name of the unix socket or named pipe that the client will try and connect to.
func StartClient(ctx context.Context, ipcName string, config *ClientConfig) (*Client, error) {

	if err := checkIpcName(ipcName); err != nil {
		return nil, err

	}

	cc := &Client{
		Name:     ipcName,
		status:   NotConnected,
		received: make(chan *Message),
		toWrite:  make(chan *Message),
	}

	cc.timeout = 0
	cc.retryTimer = 20 * time.Second
	cc.encryptionReq = true

	if config != nil {
		cc.timeout = config.Timeout
		if cc.timeout < 0 {
			cc.timeout = 0
		}

		cc.retryTimer = config.RetryTimer
		if cc.retryTimer < time.Second {
			cc.retryTimer = time.Second
		}

		cc.encryptionReq = config.Encryption
		cc.verifyServer = config.VerifyServer
	}

	// Cancelled by Close, to also stop dialing and reconnecting.
	subctx, cancel := context.WithCancel(ctx)
	cc.cancel = cancel

	go startClient(subctx, cc)

	return cc, nil
}

func startClient(ctx context.Context, c *Client) {

	c.setStatusCode(Connecting)
	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})

	conn, err := c.dial(ctx)
	if err != nil {
		c.sendMessage(ctx, c.received, &Message{Err: err, MsgType: -1})
		return
	}

	if !c.connected(conn) {
		return
	}

	go c.read(ctx, conn)
	go c.write()

	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})
}

func (c *Client) connected(conn net.Conn) bool {

	c.Lock()
	defer c.Unlock()

	if c.status == Closing || c.status == Closed {
		_ = conn.Close()
		return false
	}

	c.conn = conn
	c.connectedAt = time.Now()
	c.status = Connected

	return true
}

func (c *Client) currentConn() net.Conn {
	c.RLock()
	defer c.RUnlock()
	return c.conn
}

func (c *Client) sendMessage(ctx context.Context, sendTo chan *Message, msg *Message) {

	select {
	case sendTo <- msg:
	case <-ctx.Done():
	}
}

func (c *Client) read(ctx context.Context, conn net.Conn) {

	slog.Debug("Starting read for new connection")

	enc := c.frameEncryption()

	for {
		msgType, data, err := readFrame(conn, enc, c.maxMsgSize)

		switch {
		case errors.Is(err, errUndecryptable):
			slog.Error("Unable to decrypt message", "err", err)
		case err != nil:
			c.readFailed(ctx, conn, err)
			return
		case deliverable(msgType):
			c.sendMessage(ctx, c.received, &Message{Data: data, MsgType: int(msgType)})
		}
	}
}

func (c *Client) readFailed(ctx context.Context, conn net.Conn, err error) {

	_ = conn.Close()

	if c.StatusCode() == Closing || c.StatusCode() == Closed || ctx.Err() != nil {
		slog.Debug("Stopping read due to closing connection")
		c.setStatusCode(Closed)
		// The context has been canceled so let's give these messages a chance
		// to be sent but don't wait long
		subctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 500*time.Millisecond)
		defer cancel()
		c.sendMessage(subctx, c.received, &Message{Status: c.Status(), MsgType: -1})
		c.sendMessage(subctx, c.received, &Message{Err: ClientConnectionClosed, MsgType: -2})
		return
	}

	// Any other error means the connection is gone.
	if errors.Is(err, io.EOF) {
		slog.Debug("Stopping read due to EOF", "status", c.Status())
	} else {
		slog.Error("Unable to read data from server", "err", err)
	}

	go c.reconnect(ctx)
}

// frameEncryption - enc can be left over from a previous, encrypted, connection.
func (c *Client) frameEncryption() *encryption {
	if !c.encryption {
		return nil
	}
	return c.enc
}

func (c *Client) reconnect(ctx context.Context) {

	slog.Info("Attempting to reconnect IPC channel")

	c.Lock()
	if c.status == Closing || c.status == Closed {
		c.Unlock()
		return
	}
	c.status = Reconnecting

	// Back off while connections keep dropping straight away, to avoid a reconnection storm.
	var delay time.Duration
	if time.Since(c.connectedAt) < quickDrop {
		c.consecutiveQuickDrops++
		delay = min(c.retryTimer, time.Second<<min(c.consecutiveQuickDrops-1, 16))
	} else {
		c.consecutiveQuickDrops = 0
	}
	c.Unlock()

	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return
		}
	}

	// connect to the pipe
	conn, err := c.dial(ctx)
	if err != nil {
		if err.Error() == "timed out trying to connect" {
			c.setStatusCode(Timeout)
			c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})
			c.sendMessage(ctx, c.received, &Message{Err: errors.New("timed out trying to re-connect"), MsgType: -1})
			return
		}

		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			slog.Error("Unable to dial to the pipe", "err", err)
			go c.reconnect(ctx)
		}

		return
	}

	if !c.connected(conn) {
		return
	}

	slog.Info("Reconnected IPC channel")

	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})

	go c.read(ctx, conn)
}

// Read - blocking function that receives messages
// if MsgType is a negative number its an internal message
func (c *Client) Read() (*Message, error) {

	m, ok := <-c.received
	if !ok {
		return nil, ChannelClosedReceived
	}

	if m.Err != nil {
		if c.StatusCode() == Timeout {
			close(c.received)
			close(c.toWrite)
		}
		return nil, m.Err
	}

	return m, nil
}

// ReadWithContext - blocking function, but it reacts to end of context, reads each message received
// if MsgType is a negative number it's an internal message
func (c *Client) ReadWithContext(ctx context.Context) (*Message, error) {
	select {
	case m, ok := <-c.received:
		if !ok {
			return nil, ChannelClosedReceived
		}

		if m.Err != nil {
			if c.StatusCode() == Timeout {
				close(c.received)
				close(c.toWrite)
			}
			return nil, m.Err
		}

		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Write - writes a message to the ipc connection.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
// The message is copied, so the caller may reuse it.
func (c *Client) Write(msgType int, message []byte) error {
	return c.WriteWithContext(context.Background(), msgType, message)
}

// WriteWithContext - writes a message to the ipc connection, but it reacts to end of context.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
// The message is copied, so the caller may reuse it.
func (c *Client) WriteWithContext(ctx context.Context, msgType int, message []byte) error {

	if msgType == 0 {
		return errors.New("Message type 0 is reserved")
	}

	if c.StatusCode() != Connected {
		return errors.New(c.Status())
	}

	if len(message) > c.maxMsgSize {
		return errors.New("Message exceeds maximum message length")
	}

	select {
	case c.toWrite <- &Message{MsgType: msgType, Data: bytes.Clone(message)}:
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}

func (c *Client) write() {

	slog.Debug("Starting write for new connection")

	for {
		m, ok := <-c.toWrite
		if !ok {
			slog.Info("Stopping write as channel is closed")
			return
		}

		if err := writeFrame(c.currentConn(), c.frameEncryption(), m.MsgType, m.Data); err != nil {
			slog.Error("Unable to write data", "err", err)
		}
	}
}

// setStatusCode - sets the current connection status
func (c *Client) setStatusCode(status Status) {
	c.Lock()
	defer c.Unlock()
	c.status = status
}

// StatusCode - returns the current connection status
func (c *Client) StatusCode() Status {
	c.RLock()
	defer c.RUnlock()
	return c.status
}

// Status - returns the current connection status as a string
func (c *Client) Status() string {
	c.RLock()
	defer c.RUnlock()
	return c.status.String()
}

// Close - closes the connection and stops reconnecting.
func (c *Client) Close() {

	c.Lock()
	c.status = Closing
	conn := c.conn
	c.Unlock()

	if conn != nil {
		_ = conn.Close()
	}

	if c.cancel != nil {
		c.cancel()
	}
}
