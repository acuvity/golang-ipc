//go:build linux || darwin
// +build linux darwin

package ipc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	unixSockBase   = "/tmp/"
	unixSockSuffix = ".sock"
)

// Server create a unix socket and start listening connections - for unix and linux
func (s *Server) run(ctx context.Context) error {

	if err := os.RemoveAll(unixSockBase + s.name + unixSockSuffix); err != nil {
		return err
	}

	var oldUmask int
	if s.unMask {
		oldUmask = syscall.Umask(0)
	}

	listen, err := net.Listen("unix", unixSockBase+s.name+unixSockSuffix)

	if s.unMask {
		syscall.Umask(oldUmask)
	}

	if err != nil {
		return err
	}

	path := unixSockBase + s.name + unixSockSuffix

	// The listener is also closed when the context ends, possibly after a new server took the path.
	// Only Close removes the socket, before it returns.
	if ul, ok := listen.(*net.UnixListener); ok {
		ul.SetUnlinkOnClose(false)
	}

	created, statErr := os.Lstat(path)
	s.removeSocket = sync.OnceFunc(func() {
		// Best effort only: ext4 reuses inode numbers, so this cannot tell a replaced socket apart.
		if statErr != nil {
			return
		}
		if current, err := os.Lstat(path); err == nil && os.SameFile(created, current) {
			_ = os.Remove(path)
		}
	})

	s.listen = listen
	s.setStatusCode(Listening)

	go s.serve(ctx)

	return nil
}

// Client connect to the unix socket created by the server -  for unix and linux
func (c *Client) dial(ctx context.Context) (net.Conn, error) {

	socketPath := unixSockBase + c.Name + unixSockSuffix

	startTime := time.Now()

	for i := 0; ; i++ {
		if c.timeout != 0 {
			if time.Since(startTime).Seconds() > c.timeout {
				c.setStatusCode(Closed)
				return nil, errors.New("timed out trying to connect")
			}
		}

		conn, err := net.Dial("unix", socketPath)
		if err != nil {
			if !strings.Contains(err.Error(), "connect: no such file or directory") &&
				!strings.Contains(err.Error(), "connect: connection refused") {
				c.sendMessage(ctx, c.received, &Message{Err: err, MsgType: -1})
			}
			if i%30 == 29 {
				slog.Debug("Waiting for client dial to succeed", "err", err)
			}
		} else if c.setup(ctx, conn) {
			return conn, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.retryTimer):
		}
	}

}
