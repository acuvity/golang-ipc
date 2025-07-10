package ipc

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
)

const pipeBase = `\\.\pipe\`

// Server function
// Create the named pipe (if it doesn't already exist) and start listening for a client to connect.
// when a client connects and connection is accepted the read function is called on a go routine.
func (s *Server) run(ctx context.Context) error {

	var config *winio.PipeConfig

	if s.unMask {
		config = &winio.PipeConfig{SecurityDescriptor: "D:P(A;;GA;;;AU)"}
	}

	listen, err := winio.ListenPipe(pipeBase+s.name, config)
	if err != nil {
		return err
	}

	s.listen = listen
	s.setStatusCode(Listening)

	go s.acceptLoop(ctx)

	return nil
}

// Client function
// dial - attempts to connect to a named pipe created by the server
func (c *Client) dial(ctx context.Context) (net.Conn, error) {

	pipePath := pipeBase + c.Name

	startTime := time.Now()

	for i := 0; ; i++ {
		if c.timeout != 0 {
			if time.Since(startTime).Seconds() > c.timeout {
				c.setStatusCode(Closed)
				return nil, errors.New("timed out trying to connect")
			}
		}

		pn, err := winio.DialPipe(pipePath, nil)
		if err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "the system cannot find the file specified.") {
				return nil, err
			}
			if i%30 == 29 {
				slog.Debug("Waiting for client dial to succeed", "err", err)
			}
		} else {
			return pn, c.handshake(pn)
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.retryTimer):
		}
	}
}
