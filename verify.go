package ipc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"slices"
)

// ServerVerificationFailed is delivered (wrapped) by Read when ClientConfig.VerifyServer rejects the server.
var ServerVerificationFailed = errors.New("server verification failed")

// VerifyServerPrivileged only accepts a server run by root, or on Windows by a LocalSystem service.
func VerifyServerPrivileged() func(PeerInfo) error {
	return VerifyServerUID(privilegedUIDs...)
}

// VerifyServerUID only accepts a server whose PeerInfo.UID is one of uids.
func VerifyServerUID(uids ...string) func(PeerInfo) error {
	return func(peer PeerInfo) error {
		if !slices.Contains(uids, peer.UID) {
			return fmt.Errorf("server is running as %q (pid %d)", peer.UID, peer.PID)
		}
		return nil
	}
}

// setup - returns false, after reporting the error, if the dial loop should retry.
func (c *Client) setup(ctx context.Context, conn net.Conn) bool {

	err := c.checkServer(conn)
	if err != nil {
		slog.Error("Rejecting IPC server", "err", err)
	} else if err = c.handshake(conn); err != nil {
		slog.Error("IPC handshake failed", "err", err)
	}

	if err == nil {
		return true
	}

	_ = conn.Close()
	c.sendMessage(ctx, c.received, &Message{Err: err, MsgType: -1})

	return false
}

func (c *Client) checkServer(conn net.Conn) error {

	if c.verifyServer == nil {
		return nil
	}

	peer, err := serverPeerInfo(conn)
	if err != nil {
		return fmt.Errorf("%w: unable to identify server: %w", ServerVerificationFailed, err)
	}

	if err := c.verifyServer(peer); err != nil {
		return fmt.Errorf("%w: %w", ServerVerificationFailed, err)
	}

	return nil
}
