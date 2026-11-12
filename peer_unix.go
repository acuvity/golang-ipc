//go:build linux || darwin

package ipc

import "net"

// PrivilegedUID is root.
const PrivilegedUID = "0"

var privilegedUIDs = []string{PrivilegedUID}

func serverPeerInfo(conn net.Conn) (PeerInfo, error) {
	return peerInfo(conn)
}
