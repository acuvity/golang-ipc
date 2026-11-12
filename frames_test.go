package ipc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"
)

// rawClient returns a handshaken connection for writing arbitrary frames, and the Client holding its cipher.
func rawClient(ctx context.Context, t *testing.T, name string, encryption bool) (*Client, net.Conn) {
	t.Helper()

	c := &Client{
		Name:          name,
		status:        NotConnected,
		retryTimer:    time.Second,
		encryptionReq: encryption,
		received:      make(chan *Message, 8),
	}

	conn, err := c.dial(ctx)
	if err != nil {
		t.Fatalf("unable to dial server: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return c, conn
}

func writeRawFrame(t *testing.T, conn net.Conn, length uint32, payload []byte) {
	t.Helper()

	if _, err := conn.Write(binary.BigEndian.AppendUint32(nil, length)); err != nil {
		t.Fatalf("unable to write frame length: %v", err)
	}
	if len(payload) == 0 {
		return
	}
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("unable to write frame: %v", err)
	}
}

func encryptedFrame(t *testing.T, c *Client, plain []byte) []byte {
	t.Helper()

	enc, err := encrypt(*c.enc.cipher, plain)
	if err != nil {
		t.Fatal(err)
	}

	return enc
}

func expectServerDrop(ctx context.Context, t *testing.T, s *Server, conn net.Conn, id uint64) {
	t.Helper()

	m := readServer(ctx, t, s, isStatus("Closed"))
	if m.Conn.ID() != id {
		t.Fatalf("expected connection %d to be dropped, got %d", id, m.Conn.ID())
	}

	_ = conn.SetReadDeadline(time.Now().Add(multiTimeout))
	if _, err := conn.Read(make([]byte, 1)); err == nil {
		t.Fatal("expected the server to close the connection")
	}

	if len(s.Connections()) != 0 {
		t.Fatal("dropped connection is still registered")
	}
}

func startServerWith(ctx context.Context, t *testing.T, config *ServerConfig) (*Server, string) {
	t.Helper()

	name := fmt.Sprintf("frames_%s_%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())

	s, err := StartServer(ctx, name, config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	return s, name
}

func TestServerDropsOversizedFrame(t *testing.T) {

	for _, encryption := range []bool{true, false} {
		t.Run(fmt.Sprintf("encryption=%t", encryption), func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, name := startServerWith(ctx, t, &ServerConfig{Encryption: encryption, MaxMsgSize: 2048})

			_, conn := rawClient(ctx, t, name, encryption)
			id := readServer(ctx, t, s, isStatus("Connected")).Conn.ID()

			// No body: the server must refuse the length alone.
			writeRawFrame(t, conn, 0xFFFFFFFF, nil)

			expectServerDrop(ctx, t, s, conn, id)
		})
	}
}

func TestServerDropsFrameJustOverLimit(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: false, MaxMsgSize: 2048})

	_, conn := rawClient(ctx, t, name, false)
	id := readServer(ctx, t, s, isStatus("Connected")).Conn.ID()

	writeRawFrame(t, conn, uint32(2048+maxFrameOverhead+1), nil)

	expectServerDrop(ctx, t, s, conn, id)
}

func TestServerAcceptsMaxSizedMessage(t *testing.T) {

	for _, encryption := range []bool{true, false} {
		t.Run(fmt.Sprintf("encryption=%t", encryption), func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, name := startServerWith(ctx, t, &ServerConfig{Encryption: encryption, MaxMsgSize: 2048})

			c, err := StartClient(ctx, name, &ClientConfig{Encryption: encryption, RetryTimer: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)

			waitClientStatus(ctx, t, c, "Connected")
			readServer(ctx, t, s, isStatus("Connected"))

			data := make([]byte, 2048)
			for i := range data {
				data[i] = byte(i)
			}

			if err := c.Write(9, data); err != nil {
				t.Fatal(err)
			}

			m := readServer(ctx, t, s, isData)
			if m.MsgType != 9 || len(m.Data) != len(data) {
				t.Fatalf("unexpected message type %d with %d bytes", m.MsgType, len(m.Data))
			}
		})
	}
}

func TestServerDropsShortFrame(t *testing.T) {

	t.Run("plain", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s, name := startServerWith(ctx, t, &ServerConfig{Encryption: false})

		_, conn := rawClient(ctx, t, name, false)
		id := readServer(ctx, t, s, isStatus("Connected")).Conn.ID()

		writeRawFrame(t, conn, 2, []byte{0, 1})

		expectServerDrop(ctx, t, s, conn, id)
	})

	t.Run("encrypted", func(t *testing.T) {

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		s, name := startServerWith(ctx, t, &ServerConfig{Encryption: true})

		c, conn := rawClient(ctx, t, name, true)
		id := readServer(ctx, t, s, isStatus("Connected")).Conn.ID()

		payload := encryptedFrame(t, c, []byte{0, 1})
		writeRawFrame(t, conn, uint32(len(payload)), payload)

		expectServerDrop(ctx, t, s, conn, id)
	})
}

func TestServerReportsUndecryptableFrame(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: true})

	c, conn := rawClient(ctx, t, name, true)
	readServer(ctx, t, s, isStatus("Connected"))

	garbage := make([]byte, 40)
	writeRawFrame(t, conn, uint32(len(garbage)), garbage)

	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()

	if _, err := s.ReadWithContext(subctx); err == nil || !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("expected a decryption error, got %v", err)
	}

	payload := encryptedFrame(t, c, append(intToBytes(6), []byte("after")...))
	writeRawFrame(t, conn, uint32(len(payload)), payload)

	m := readServer(ctx, t, s, isData)
	if m.MsgType != 6 || string(m.Data) != "after" {
		t.Fatalf("unexpected message %q (type %d)", m.Data, m.MsgType)
	}

	if len(s.Connections()) != 1 {
		t.Fatal("connection should still be registered")
	}
}

func TestConnWriteValidation(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: true, MaxMsgSize: 2048})

	_, conns := connectClients(ctx, t, s, name, 1)
	conn := conns[0]

	if err := conn.Write(0, []byte("x")); err == nil || err.Error() != "message type 0 is reserved" {
		t.Fatalf("expected reserved type error, got %v", err)
	}

	if err := conn.Write(1, make([]byte, 2049)); err == nil || err.Error() != "message exceeds maximum message length" {
		t.Fatalf("expected maximum length error, got %v", err)
	}

	if err := conn.Write(1, make([]byte, 2048)); err != nil {
		t.Fatalf("a maximum sized message should be accepted: %v", err)
	}
}

func TestConnWriteContextAndClose(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: false})

	// The raw client never reads, so this keeps the writer busy.
	_, _ = rawClient(ctx, t, name, false)
	conn := readServer(ctx, t, s, isStatus("Connected")).Conn

	if err := conn.Write(1, make([]byte, maxMsgSize)); err != nil {
		t.Fatal(err)
	}

	wctx, wcancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer wcancel()

	if err := conn.WriteWithContext(wctx, 1, []byte("late")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}

	result := make(chan error, 1)
	go func() { result <- conn.Write(1, []byte("pending")) }()

	time.Sleep(200 * time.Millisecond)
	conn.Close()

	select {
	case err := <-result:
		if !errors.Is(err, ConnectionClosed) {
			t.Fatalf("expected ConnectionClosed, got %v", err)
		}
	case <-time.After(multiTimeout):
		t.Fatal("pending write was not released by Close")
	}

	readServer(ctx, t, s, isStatus("Closed"))

	if err := conn.Write(1, []byte("after close")); !errors.Is(err, ConnectionClosed) {
		t.Fatalf("expected an error wrapping ConnectionClosed, got %v", err)
	}
}
