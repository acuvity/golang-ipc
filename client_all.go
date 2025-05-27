package ipc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"time"
)

// ClientConnectionClosed is the error returned by Read() when the connection has closed.
var ClientConnectionClosed = errors.New("client has closed the connection")

// StartClient - start the ipc client.
// ipcName = is the name of the unix socket or named pipe that the client will try and connect to.
func StartClient(ipcName string, config *ClientConfig) (*Client, error) {

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

	go startClient(cc)

	return cc, nil
}

func startClient(c *Client) {

	c.setStatusCode(Connecting)
	c.received <- &Message{Status: c.Status(), MsgType: -1}

	if err := c.dial(); err != nil {
		c.received <- &Message{Err: err, MsgType: -1}
		return
	}

	c.setStatusCode(Connected)
	c.received <- &Message{Status: c.Status(), MsgType: -1}

	go c.read()
	go c.write()
}

func (c *Client) read() {
	bLen := make([]byte, 4)

	for {
		if res := c.readData(bLen); !res {
			return
		}

		mLen := bytesToInt(bLen)

		msgRecvd := make([]byte, mLen)

		if res := c.readData(msgRecvd); !res {
			return
		}

		if c.encryption {
			msgFinal, err := decrypt(*c.enc.cipher, msgRecvd)
			if err != nil {
				slog.Error("Unable to decrypt message", "err", err)
				continue
			}

			if bytesToInt(msgFinal[:4]) != 0 {
				c.received <- &Message{Data: msgFinal[4:], MsgType: bytesToInt(msgFinal[:4])}
			}
			continue
		}

		if bytesToInt(msgRecvd[:4]) != 0 {
			c.received <- &Message{Data: msgRecvd[4:], MsgType: bytesToInt(msgRecvd[:4])}
		}
	}
}

func (c *Client) readData(buff []byte) bool {

	if _, err := io.ReadFull(c.conn, buff); err != nil {
		if strings.Contains(err.Error(), "EOF") { // the connection has been closed by the client.
			_ = c.conn.Close()

			if c.StatusCode() != Closing || c.StatusCode() == Closed {
				go c.reconnect()
				return false
			}

			slog.Error("Read channel closed unexpectedly", "status", c.Status(), "err", err)
			return false
		}

		if c.StatusCode() == Closing {
			c.setStatusCode(Closed)
			c.received <- &Message{Status: c.Status(), MsgType: -1}
			c.received <- &Message{Err: ClientConnectionClosed, MsgType: -2}
			return false
		}

		slog.Error("Unable to read data", "err", err)

		// other read error
		return false
	}

	return true

}

func (c *Client) reconnect() {

	slog.Info("Attempting to reconnect IPC read channel")

	c.setStatusCode(Reconnecting)
	c.received <- &Message{Status: c.Status(), MsgType: -1}

	// connect to the pipe
	if err := c.dial(); err != nil {
		if err.Error() == "timed out trying to connect" {
			c.setStatusCode(Timeout)
			c.received <- &Message{Status: c.Status(), MsgType: -1}
			c.received <- &Message{Err: errors.New("timed out trying to re-connect"), MsgType: -1}
		}

		slog.Error("Unable to dial to the pipe", "err", err)

		return
	}

	slog.Info("Reconnected IPC read channel")

	c.setStatusCode(Connected)
	c.received <- &Message{Status: c.Status(), MsgType: -1}

	go c.read()
}

// Read - blocking function that receices messages
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

	for {
		m, ok := <-c.toWrite
		if !ok {
			slog.Info("Closing write channel")
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

		writer.Write(intToBytes(len(toSend)))
		writer.Write(toSend)

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
