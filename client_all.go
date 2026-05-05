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

// ClientConnectionClosed is the error returned by Read() when the connection has closed.
var ClientConnectionClosed = errors.New("client has closed the connection")

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
	}

	go startClient(ctx, cc)

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

	c.conn = conn

	go c.read(ctx, conn)
	go c.write()

	c.setStatusCode(Connected)
	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})
}

func (c *Client) sendMessage(ctx context.Context, sendTo chan *Message, msg *Message) {

	select {
	case sendTo <- msg:
	case <-ctx.Done():
	}
}

func (c *Client) read(ctx context.Context, conn net.Conn) {

	slog.Debug("Starting read for new connection")

	bLen := make([]byte, 4)

	for {
		if res := c.readData(ctx, conn, bLen); !res {
			return
		}

		mLen := bytesToInt(bLen)

		msgRecvd := make([]byte, mLen)

		if res := c.readData(ctx, conn, msgRecvd); !res {
			return
		}

		if c.encryption {
			msgFinal, err := decrypt(*c.enc.cipher, msgRecvd)
			if err != nil {
				slog.Error("Unable to decrypt message", "err", err)
				continue
			}

			if bytesToInt(msgFinal[:4]) != 0 {
				c.sendMessage(ctx, c.received, &Message{Data: msgFinal[4:], MsgType: bytesToInt(msgFinal[:4])})
			}
			continue
		}

		if bytesToInt(msgRecvd[:4]) != 0 {
			c.sendMessage(ctx, c.received, &Message{Data: msgRecvd[4:], MsgType: bytesToInt(msgRecvd[:4])})
		}
	}
}

func (c *Client) readData(ctx context.Context, conn net.Conn, buff []byte) bool {

	if _, err := io.ReadFull(conn, buff); err != nil {

		if errors.Is(err, io.EOF) { // the connection has been closed by the client.
			slog.Debug("Stopping read due to EOF", "status", c.Status())
			_ = conn.Close()

			if c.StatusCode() != Closing || c.StatusCode() == Closed {
				go c.reconnect(ctx)
				return false
			}

			return false
		}

		if c.StatusCode() == Closing || errors.Is(err, net.ErrClosed) {
			slog.Debug("Stopping read due to closing connection")
			c.setStatusCode(Closed)
			// The context has been canceled so let's give these messages a chance
			// to be sent but don't wait long
			subctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			c.sendMessage(subctx, c.received, &Message{Status: c.Status(), MsgType: -1})
			c.sendMessage(subctx, c.received, &Message{Err: ClientConnectionClosed, MsgType: -2})
			return false
		}

		slog.Error("Unable to read data from server", "err", err)

		// other read error
		return false
	}

	return true

}

func (c *Client) reconnect(ctx context.Context) {

	slog.Info("Attempting to reconnect IPC channel")

	c.setStatusCode(Reconnecting)
	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})

	// connect to the pipe
	conn, err := c.dial(ctx)
	if err != nil {
		if err.Error() == "timed out trying to connect" {
			c.setStatusCode(Timeout)
			c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})
			c.sendMessage(ctx, c.received, &Message{Err: errors.New("timed out trying to re-connect"), MsgType: -1})
		}

		if !errors.Is(err, context.Canceled) {
			slog.Error("Unable to dial to the pipe", "err", err)
			go c.reconnect(ctx)
		}

		return
	}

	slog.Info("Reconnected IPC channel")

	c.conn = conn

	c.setStatusCode(Connected)
	c.sendMessage(ctx, c.received, &Message{Status: c.Status(), MsgType: -1})

	go c.read(ctx, conn)
}

// Read - blocking function that receives messages
// if MsgType is a negative number its an internal message
func (c *Client) Read() (*Message, error) {

	m, ok := <-c.received
	if !ok {
		return nil, errors.New("the received channel has been closed")
	}

	if m.Err != nil {
		close(c.received)
		close(c.toWrite)
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
			return nil, errors.New("the received channel has been closed")
		}

		if m.Err != nil {
			close(c.received)
			close(c.toWrite)
			return nil, m.Err
		}

		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Write - writes a message to the ipc connection.
// msgType - denotes the type of data being sent. 0 is a reserved type for internal messages and errors.
func (c *Client) Write(msgType int, message []byte) error {

	if msgType == 0 {
		return errors.New("Message type 0 is reserved")
	}

	if c.StatusCode() != Connected {
		return errors.New(c.Status())
	}

	if len(message) > c.maxMsgSize {
		return errors.New("Message exceeds maximum message length")
	}

	c.toWrite <- &Message{MsgType: msgType, Data: message}

	return nil
}

// WriteWithContext - writes a message to the ipc connection, but it reacts to end of context.
// if MsgType is a negative number it's an internal message
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
	case c.toWrite <- &Message{MsgType: msgType, Data: message}:
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

		toSend := intToBytes(m.MsgType)

		writer := bufio.NewWriter(c.conn)

		if c.encryption {
			toSend = append(toSend, m.Data...)
			toSendEnc, err := encrypt(*c.enc.cipher, toSend)
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

// Close - closes the connection
func (c *Client) Close() {

	c.setStatusCode(Closing)

	if c.conn != nil {
		_ = c.conn.Close()
	}
}
