package ipc

import (
	"context"
	"errors"
	"os"
	"os/user"
	"sync/atomic"
	"testing"
	"time"
)

func TestVerifyServerAccepts(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	var seen atomic.Pointer[PeerInfo]

	c, err := StartClient(ctx, name, &ClientConfig{
		Encryption: true,
		RetryTimer: time.Second,
		VerifyServer: func(p PeerInfo) error {
			seen.Store(&p)
			return VerifyServerUID("someone-else", me.Uid)(p)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	waitClientStatus(ctx, t, c, "Connected")

	p := seen.Load()
	if p == nil {
		t.Fatal("verifier was not called")
	}
	if p.UID != me.Uid || p.PID != os.Getpid() {
		t.Fatalf("unexpected server identity %+v, expected uid %q pid %d", *p, me.Uid, os.Getpid())
	}

	readServer(ctx, t, s, isStatus("Connected"))

	if err := c.Write(3, []byte("verified")); err != nil {
		t.Fatal(err)
	}
	if m := readServer(ctx, t, s, isData); string(m.Data) != "verified" {
		t.Fatalf("unexpected data %q", m.Data)
	}
}

func TestVerifyServerRejects(t *testing.T) {

	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	if me.Uid == PrivilegedUID {
		t.Skip("test must not run as the privileged account")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, name := startTestServer(ctx, t)

	var calls atomic.Int32

	c, err := StartClient(ctx, name, &ClientConfig{
		Encryption: true,
		RetryTimer: time.Second,
		VerifyServer: func(p PeerInfo) error {
			calls.Add(1)
			return VerifyServerUID(PrivilegedUID)(p)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	rejections := 0
	subctx, subcancel := context.WithTimeout(ctx, multiTimeout)
	defer subcancel()

	for rejections < 2 {
		m, err := c.ReadWithContext(subctx)
		if subctx.Err() != nil {
			t.Fatalf("expected 2 rejections, got %d", rejections)
		}
		if err != nil {
			if !errors.Is(err, ServerVerificationFailed) {
				t.Fatalf("unexpected error %v", err)
			}
			rejections++
			continue
		}
		if m.Status == "Connected" {
			t.Fatal("client connected to a rejected server")
		}
	}

	if calls.Load() < 2 {
		t.Fatalf("expected the verifier to be called at least twice, got %d", calls.Load())
	}

	if got := len(s.Connections()); got != 0 {
		t.Fatalf("expected no server connections, got %d", got)
	}

	if c.StatusCode() == Connected {
		t.Fatal("client should not be connected")
	}
}

func TestVerifyServerUID(t *testing.T) {

	v := VerifyServerUID("a", "b")

	if err := v(PeerInfo{UID: "b"}); err != nil {
		t.Fatalf("expected b to be accepted: %v", err)
	}

	if err := v(PeerInfo{UID: "c"}); err == nil {
		t.Fatal("expected c to be rejected")
	}

	if err := VerifyServerUID()(PeerInfo{UID: ""}); err == nil {
		t.Fatal("expected an empty allow list to reject everything")
	}
}

func TestVerifyServerPrivileged(t *testing.T) {

	v := VerifyServerPrivileged()

	for _, uid := range privilegedUIDs {
		if err := v(PeerInfo{UID: uid}); err != nil {
			t.Fatalf("expected %q to be accepted: %v", uid, err)
		}
	}

	me, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	if me.Uid != PrivilegedUID {
		if err := v(PeerInfo{UID: me.Uid}); err == nil {
			t.Fatalf("expected the test user %q to be rejected", me.Uid)
		}
	}

	if err := v(PeerInfo{}); err == nil {
		t.Fatal("expected an unidentified server to be rejected")
	}
}

func waitClientStatus(ctx context.Context, t *testing.T, c *Client, want string) {
	t.Helper()

	subctx, cancel := context.WithTimeout(ctx, multiTimeout)
	defer cancel()

	for {
		m, err := c.ReadWithContext(subctx)
		if subctx.Err() != nil {
			t.Fatalf("client did not reach status %q", want)
		}
		if err != nil {
			t.Fatalf("unexpected client error: %v", err)
		}
		if m.MsgType < 0 && m.Status == want {
			return
		}
	}
}
