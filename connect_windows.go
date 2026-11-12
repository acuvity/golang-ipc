package ipc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/Microsoft/go-winio"
	"golang.org/x/sys/windows"
)

const pipeBase = `\\.\pipe\`

// sharedPipeClientAccess leaves out FILE_CREATE_PIPE_INSTANCE (part of FILE_GENERIC_WRITE), so other
// users can connect but cannot add their own instance of the pipe to intercept clients.
const sharedPipeClientAccess = windows.FILE_READ_DATA | windows.FILE_WRITE_DATA |
	windows.FILE_READ_EA | windows.FILE_WRITE_EA |
	windows.FILE_READ_ATTRIBUTES | windows.FILE_WRITE_ATTRIBUTES |
	windows.READ_CONTROL | windows.SYNCHRONIZE

// sharedPipeSDDL is used when UnmaskPermissions is set. The owner keeps full control as the server
// needs FILE_CREATE_PIPE_INSTANCE to accept further clients.
var sharedPipeSDDL = fmt.Sprintf("D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;0x%x;;;AU)", sharedPipeClientAccess)

// Server function
// Create the named pipe (if it doesn't already exist) and start listening for a client to connect.
// when a client connects and connection is accepted the read function is called on a go routine.
func (s *Server) run(ctx context.Context) error {

	var config *winio.PipeConfig

	if s.unMask {
		config = &winio.PipeConfig{SecurityDescriptor: sharedPipeSDDL}
	}

	listen, err := winio.ListenPipe(pipeBase+s.name, config)
	if err != nil {
		return err
	}

	s.listen = listen
	s.setStatusCode(Listening)

	go s.serve(ctx)

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
		} else if c.setup(ctx, pn) {
			return pn, nil
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(c.retryTimer):
		}
	}
}
