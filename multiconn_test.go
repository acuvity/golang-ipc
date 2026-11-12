package ipc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/user"
	"strings"
	"testing"
	"time"
)

const multiTimeout = 10 * time.Second

type testClient struct {
	*Client
	status chan string
	data   chan *Message
}

func startTestClient(ctx context.Context, t *testing.T, name string) *testClient {
	t.Helper()

	c, err := StartClient(ctx, name, &ClientConfig{Encryption: true, RetryTimer: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	return wrapTestClient(ctx, c)
}

func wrapTestClient(ctx context.Context, c *Client) *testClient {

	tc := &testClient{Client: c, status: make(chan string, 32), data: make(chan *Message, 128)}

	go func() {
		for {
			m, err := c.ReadWithContext(ctx)
			if err != nil {
				if ctx.Err() != nil || errors.Is(err, ClientConnectionClosed) || errors.Is(err, ChannelClosedReceived) {
					return
				}
				continue
			}
			if m.MsgType < 0 {
				tc.status <- m.Status
				continue
			}
			tc.data <- m
		}
	}()

	return tc
}

func (tc *testClient) waitStatus(t *testing.T, want string) {
	t.Helper()

	deadline := time.After(multiTimeout)
	for {
		select {
		case s := <-tc.status:
			if s == want {
				return
			}
		case <-deadline:
			t.Fatalf("client did not reach status %q", want)
		}
	}
}

func (tc *testClient) waitData(t *testing.T) *Message {
	t.Helper()

	select {
	case m := <-tc.data:
		return m
	case <-time.After(multiTimeout):
		t.Fatal("client did not receive data")
		return nil
	}
}

func (tc *testClient) expectNoData(t *testing.T, wait time.Duration) {
	t.Helper()

	select {
	case m := <-tc.data:
		t.Fatalf("client received unexpected data %q", m.Data)
	case <-time.After(wait):
	}
}

func readServer(ctx context.Context, t *testing.T, s *Server, match func(*Message) bool) *Message {
	t.Helper()

	subctx, cancel := context.WithTimeout(ctx, multiTimeout)
	defer cancel()

	for {
		m, err := s.ReadWithContext(subctx)
		if err != nil {
			if subctx.Err() != nil {
				t.Fatal("timed out waiting for server message")
			}
			continue
		}
		if match(m) {
			return m
		}
	}
}

func isStatus(status string) func(*Message) bool {
	return func(m *Message) bool { return m.MsgType == -1 && m.Status == status && m.Conn != nil }
}

func isData(m *Message) bool { return m.MsgType > 0 }

func startTestServer(ctx context.Context, t *testing.T) (*Server, string) {
	t.Helper()

	name := fmt.Sprintf("multi_%s_%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano())

	s, err := StartServer(ctx, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	return s, name
}

func connectClients(ctx context.Context, t *testing.T, s *Server, name string, n int) ([]*testClient, []*Conn) {
	t.Helper()

	clients := make([]*testClient, n)
	conns := make([]*Conn, n)

	for i := range n {
		clients[i] = startTestClient(ctx, t, name)
		t.Cleanup(clients[i].Close)

		conns[i] = readServer(ctx, t, s, isStatus("Connected")).Conn
		clients[i].waitStatus(t, "Connected")
	}

	return clients, conns
}

func TestMultiConnRouting(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	const n = 4
	clients, conns := connectClients(ctx, t, s, name, n)

	ids := map[uint64]bool{}
	for _, c := range conns {
		if ids[c.ID()] {
			t.Fatalf("duplicate connection id %d", c.ID())
		}
		ids[c.ID()] = true
	}

	if got := len(s.Connections()); got != n {
		t.Fatalf("expected %d connections, got %d", n, got)
	}

	if s.StatusCode() != Connected {
		t.Fatalf("expected server to be connected, got %s", s.Status())
	}

	for i, c := range clients {
		go func() {
			if err := c.Write(7, fmt.Appendf(nil, "client-%d", i)); err != nil {
				t.Error(err)
			}
		}()
	}

	byConn := map[uint64]string{}
	for range n {
		m := readServer(ctx, t, s, isData)
		if m.Conn == nil {
			t.Fatal("data message without connection")
		}
		byConn[m.Conn.ID()] = string(m.Data)
		if err := m.Conn.Write(8, append([]byte("echo-"), m.Data...)); err != nil {
			t.Fatal(err)
		}
	}

	for i, c := range conns {
		if want := fmt.Sprintf("client-%d", i); byConn[c.ID()] != want {
			t.Fatalf("connection %d: expected %q, got %q", c.ID(), want, byConn[c.ID()])
		}
	}

	for i, c := range clients {
		m := c.waitData(t)
		if want := fmt.Sprintf("echo-client-%d", i); string(m.Data) != want || m.MsgType != 8 {
			t.Fatalf("client %d: expected %q, got %q (type %d)", i, want, m.Data, m.MsgType)
		}
		c.expectNoData(t, 50*time.Millisecond)
	}
}

func TestMultiConnBroadcast(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	clients, _ := connectClients(ctx, t, s, name, 3)

	if err := s.Broadcast(3, []byte("hello all")); err != nil {
		t.Fatal(err)
	}

	for i, c := range clients {
		if m := c.waitData(t); string(m.Data) != "hello all" || m.MsgType != 3 {
			t.Fatalf("client %d: unexpected broadcast %q (type %d)", i, m.Data, m.MsgType)
		}
	}
}

func TestMultiConnDisconnectKeepsOthers(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	clients, conns := connectClients(ctx, t, s, name, 2)

	clients[0].Close()

	m := readServer(ctx, t, s, isStatus("Disconnected"))
	if m.Conn.ID() != conns[0].ID() {
		t.Fatalf("expected connection %d to disconnect, got %d", conns[0].ID(), m.Conn.ID())
	}

	if err := conns[0].Write(2, []byte("gone")); err == nil {
		t.Fatal("expected an error writing to a disconnected connection")
	}

	if s.StatusCode() != Connected {
		t.Fatalf("server should still be connected, got %s", s.Status())
	}

	if got := s.Connections(); len(got) != 1 || got[0].ID() != conns[1].ID() {
		t.Fatalf("expected only connection %d to remain, got %v", conns[1].ID(), len(got))
	}

	if err := s.Broadcast(2, []byte("still here")); err != nil {
		t.Fatal(err)
	}

	if m := clients[1].waitData(t); string(m.Data) != "still here" {
		t.Fatalf("unexpected data %q", m.Data)
	}

	if err := clients[1].Write(4, []byte("ping")); err != nil {
		t.Fatal(err)
	}

	if m := readServer(ctx, t, s, isData); m.Conn.ID() != conns[1].ID() || string(m.Data) != "ping" {
		t.Fatalf("unexpected message %q from %d", m.Data, m.Conn.ID())
	}
}

func TestMultiConnServerSideClose(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	clients, conns := connectClients(ctx, t, s, name, 2)

	conns[0].Close()

	m := readServer(ctx, t, s, isStatus("Closed"))
	if m.Conn.ID() != conns[0].ID() {
		t.Fatalf("expected connection %d to close, got %d", conns[0].ID(), m.Conn.ID())
	}

	if err := conns[0].Write(2, []byte("x")); err == nil {
		t.Fatal("expected an error writing to a closed connection")
	}

	clients[0].waitStatus(t, "Reconnecting")
	m = readServer(ctx, t, s, isStatus("Connected"))
	if m.Conn.ID() == conns[0].ID() || m.Conn.ID() == conns[1].ID() {
		t.Fatalf("expected a new connection id, got %d", m.Conn.ID())
	}

	if err := conns[1].Write(2, []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if m := clients[1].waitData(t); string(m.Data) != "ok" {
		t.Fatalf("unexpected data %q", m.Data)
	}
}

func TestMultiConnPeerIdentity(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	_, conns := connectClients(ctx, t, s, name, 1)

	peer, err := conns[0].Peer()
	if err != nil {
		t.Fatal(err)
	}

	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	if peer.UID != me.Uid {
		t.Fatalf("expected peer uid %q, got %q", me.Uid, peer.UID)
	}

	if peer.PID != os.Getpid() {
		t.Fatalf("expected peer pid %d, got %d", os.Getpid(), peer.PID)
	}
}

func TestMultiConnCloseAndRestart(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := fmt.Sprintf("multi_restart_%d", time.Now().UnixNano())

	s, err := StartServer(ctx, name, nil)
	if err != nil {
		t.Fatal(err)
	}

	_, _ = connectClients(ctx, t, s, name, 2)

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		s.Close()
	}()

	readServer(ctx, t, s, func(m *Message) bool { return m.MsgType == -1 && m.Status == "Closed" && m.Conn == nil })

	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()
	if _, err := s.ReadWithContext(subctx); !errors.Is(err, ServerConnectionClosed) {
		t.Fatalf("expected ServerConnectionClosed, got %v", err)
	}

	<-closed

	if got := len(s.Connections()); got != 0 {
		t.Fatalf("expected no connections after close, got %d", got)
	}

	s2, err := StartServer(ctx, name, nil)
	if err != nil {
		t.Fatalf("unable to restart server with the same name: %v", err)
	}
	s2.Close()
}

// Fails on Windows if Close leaves a pipe handle open.
func TestMultiConnRestartImmediately(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := fmt.Sprintf("multi_restart_now_%d", time.Now().UnixNano())

	for i := range 20 {
		s, err := StartServer(ctx, name, nil)
		if err != nil {
			t.Fatalf("iteration %d: unable to start server: %v", i, err)
		}

		clients, _ := connectClients(ctx, t, s, name, 2)

		s.Close()

		for _, c := range clients {
			c.Close()
		}
	}
}

func TestMultiConnCloseWithoutClients(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, _ := startTestServer(ctx, t)

	go s.Close()

	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()

	for {
		_, err := s.ReadWithContext(subctx)
		if errors.Is(err, ServerConnectionClosed) {
			break
		}
		if subctx.Err() != nil {
			t.Fatal("did not receive ServerConnectionClosed")
		}
	}

	if s.StatusCode() != Closed {
		t.Fatalf("expected status closed, got %s", s.Status())
	}
}
