package ipc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestStuckClientDoesNotBlockBroadcast(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: false, WriteTimeout: 300 * time.Millisecond})

	good := startTestClientWith(ctx, t, name, false)
	readServer(ctx, t, s, isStatus("Connected"))
	good.waitStatus(t, "Connected")

	_, _ = rawClient(ctx, t, name, false)
	stuck := readServer(ctx, t, s, isStatus("Connected")).Conn

	big := make([]byte, maxMsgSize)

	done := make(chan error, 1)
	go func() {
		var err error
		for range 3 {
			err = errors.Join(err, s.Broadcast(5, big))
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Logf("broadcast errors: %v", err)
	case <-time.After(multiTimeout):
		t.Fatal("broadcast is blocked by a client that does not read")
	}

	m := readServer(ctx, t, s, isStatus("Closed"))
	if m.Conn.ID() != stuck.ID() {
		t.Fatalf("expected the stuck client %d to be dropped, got %d", stuck.ID(), m.Conn.ID())
	}

	for i := range 3 {
		if m := good.waitData(t); len(m.Data) != maxMsgSize {
			t.Fatalf("message %d: unexpected size %d", i, len(m.Data))
		}
	}
}

func TestMaxConnections(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: true, MaxConnections: 2})

	clients, _ := connectClients(ctx, t, s, name, 2)

	third := startTestClient(ctx, t, name)
	t.Cleanup(third.Close)

	time.Sleep(500 * time.Millisecond)
	if got := len(s.Connections()); got != 2 {
		t.Fatalf("expected 2 connections, got %d", got)
	}

	clients[0].Close()
	readServer(ctx, t, s, isStatus("Disconnected"))

	readServer(ctx, t, s, isStatus("Connected"))
	third.waitStatus(t, "Connected")
}

func TestAdmitClient(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls atomic.Int32
	var allow atomic.Bool

	s, name := startServerWith(ctx, t, &ServerConfig{
		Encryption: true,
		AdmitClient: func(c *Conn) error {
			calls.Add(1)
			if !allow.Load() {
				return errors.New("not welcome yet")
			}
			if _, err := c.Peer(); err != nil {
				return err
			}
			return nil
		},
	})

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(time.Millisecond):
				_ = s.Broadcast(4, []byte("for admitted clients"))
			}
		}
	}()

	rejected := startTestClient(ctx, t, name)
	t.Cleanup(rejected.Close)

	deadline := time.Now().Add(multiTimeout)
	for calls.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("AdmitClient was not called")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	rejected.Close()

	allow.Store(true)

	accepted := startTestClient(ctx, t, name)
	t.Cleanup(accepted.Close)

	m := readServer(ctx, t, s, isStatus("Connected"))
	accepted.waitStatus(t, "Connected")
	accepted.waitData(t)

	if conns := s.Connections(); len(conns) != 1 || conns[0].ID() != m.Conn.ID() {
		t.Fatalf("expected only the admitted connection, got %d", len(conns))
	}

	select {
	case d := <-rejected.data:
		t.Fatalf("rejected client received %q", d.Data)
	default:
	}
}

func TestDepartureNoticedWithoutRead(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	clients, conns := connectClients(ctx, t, s, name, 2)

	clients[0].Close()

	deadline := time.Now().Add(multiTimeout)
	for len(s.Connections()) != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("departed client still listed: %d connections", len(s.Connections()))
		}
		time.Sleep(10 * time.Millisecond)
	}

	if conns[0].StatusCode() != Disconnected {
		t.Fatalf("expected the departed connection to be disconnected, got %s", conns[0].Status())
	}

	readServer(ctx, t, s, isStatus("Disconnected"))
}

func TestBurstOfMessages(t *testing.T) {

	for _, encryption := range []bool{true, false} {
		t.Run(fmt.Sprintf("encryption=%t", encryption), func(t *testing.T) {

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			s, name := startServerWith(ctx, t, &ServerConfig{Encryption: encryption})

			c := startTestClientWith(ctx, t, name, encryption)
			conn := readServer(ctx, t, s, isStatus("Connected")).Conn
			c.waitStatus(t, "Connected")

			const count = 1000

			payload := func(i int) []byte {
				b := make([]byte, (i*37)%8192)
				for j := range b {
					b[j] = byte(i + j)
				}
				return b
			}

			errs := make(chan error, 1)
			go func() {
				for i := range count {
					if err := conn.Write(1+i%100, payload(i)); err != nil {
						errs <- err
						return
					}
				}
				errs <- nil
			}()

			for i := range count {
				m := c.waitData(t)
				if m.MsgType != 1+i%100 || !bytes.Equal(m.Data, payload(i)) {
					t.Fatalf("message %d: unexpected type %d with %d bytes", i, m.MsgType, len(m.Data))
				}
			}

			if err := <-errs; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWriteCopiesMessage(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	clients, conns := connectClients(ctx, t, s, name, 1)

	buf := make([]byte, 16)
	for i := range 50 {
		copy(buf, fmt.Sprintf("message-%04d....", i))
		if err := conns[0].Write(1, buf); err != nil {
			t.Fatal(err)
		}
		copy(buf, "overwritten.....")
	}

	for i := range 50 {
		if got, want := string(clients[0].waitData(t).Data), fmt.Sprintf("message-%04d....", i); got != want {
			t.Fatalf("expected %q, got %q", want, got)
		}
	}
}

func TestDecryptErrorIdentifiesConn(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startServerWith(ctx, t, &ServerConfig{Encryption: true})

	_, conn := rawClient(ctx, t, name, true)
	id := readServer(ctx, t, s, isStatus("Connected")).Conn.ID()

	garbage := make([]byte, 40)
	writeRawFrame(t, conn, uint32(len(garbage)), garbage)

	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()

	_, err := s.ReadWithContext(subctx)

	var connErr *ConnError
	if !errors.As(err, &connErr) {
		t.Fatalf("expected a *ConnError, got %v", err)
	}
	if connErr.Conn.ID() != id {
		t.Fatalf("expected connection %d, got %d", id, connErr.Conn.ID())
	}
}

func TestClientCloseStopsVerificationRetries(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, name := startTestServer(ctx, t)

	var calls atomic.Int32

	c, err := StartClient(ctx, name, &ClientConfig{
		Encryption: true,
		RetryTimer: time.Second,
		VerifyServer: func(PeerInfo) error {
			calls.Add(1)
			return errors.New("not this one")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()

	for {
		_, err := c.ReadWithContext(subctx)
		if subctx.Err() != nil {
			t.Fatal("expected a verification failure")
		}
		if err != nil {
			if !errors.Is(err, ServerVerificationFailed) {
				t.Fatalf("expected a verification failure, got %v", err)
			}
			break
		}
	}

	c.Close()
	before := calls.Load()

	time.Sleep(2500 * time.Millisecond)

	if after := calls.Load(); after != before {
		t.Fatalf("client kept retrying after Close: %d more attempts", after-before)
	}

	if c.StatusCode() == Connected {
		t.Fatal("closed client should not be connected")
	}
}

func TestClientRetriesFailedHandshake(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, name := startServerWith(ctx, t, &ServerConfig{Encryption: false})

	c, err := StartClient(ctx, name, &ClientConfig{Encryption: true, RetryTimer: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()

	failures := 0
	for failures < 2 {
		_, err := c.ReadWithContext(subctx)
		if subctx.Err() != nil {
			t.Fatalf("expected 2 handshake failures, got %d", failures)
		}
		if err != nil {
			if err.Error() != "server tried to connect without encryption" {
				t.Fatalf("unexpected error %v", err)
			}
			failures++
		}
	}
}

func TestClientReconnectsAfterCutFrame(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	clients, conns := connectClients(ctx, t, s, name, 1)

	if err := conns[0].Write(1, make([]byte, maxMsgSize)); err != nil {
		t.Fatal(err)
	}
	conns[0].Close()

	clients[0].waitStatus(t, "Reconnecting")
	readServer(ctx, t, s, isStatus("Connected"))
	clients[0].waitStatus(t, "Connected")
}

func TestCloseRemovesSocket(t *testing.T) {

	if runtime.GOOS == "windows" {
		t.Skip("named pipes are not files")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	name := fmt.Sprintf("multi_remove_%d", time.Now().UnixNano())

	s, err := StartServer(ctx, name, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat("/tmp/" + name + ".sock"); err != nil {
		t.Fatalf("socket missing while running: %v", err)
	}

	s.Close()

	if _, err := os.Stat("/tmp/" + name + ".sock"); !os.IsNotExist(err) {
		t.Fatalf("socket still present after Close: %v", err)
	}
}

func TestOldServerDoesNotRemoveNewSocket(t *testing.T) {

	if runtime.GOOS == "windows" {
		t.Skip("named pipes are not files")
	}

	name := fmt.Sprintf("multi_unlink_%d", time.Now().UnixNano())

	ctx1, cancel1 := context.WithCancel(context.Background())
	s1, err := StartServer(ctx1, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s1.Close)

	cancel1()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s2, err := StartServer(ctx, name, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s2.Close)

	time.Sleep(300 * time.Millisecond)

	if _, err := os.Stat("/tmp/" + name + ".sock"); err != nil {
		t.Fatalf("socket of the new server is gone: %v", err)
	}

	c := startTestClient(ctx, t, name)
	t.Cleanup(c.Close)
	c.waitStatus(t, "Connected")
}

func startTestClientWith(ctx context.Context, t *testing.T, name string, encryption bool) *testClient {
	t.Helper()

	c, err := StartClient(ctx, name, &ClientConfig{Encryption: encryption, RetryTimer: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	return wrapTestClient(ctx, c)
}
