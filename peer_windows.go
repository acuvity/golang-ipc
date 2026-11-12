package ipc

import (
	"errors"
	"fmt"
	"net"

	"golang.org/x/sys/windows"
)

// PrivilegedUID is LocalSystem, the default account of Windows services.
const PrivilegedUID = "S-1-5-18"

// A LocalSystem pipe is owned by LocalSystem or Administrators, depending on the token's default owner.
var privilegedUIDs = []string{PrivilegedUID, "S-1-5-32-544"}

func peerInfo(conn net.Conn) (PeerInfo, error) {

	h, err := pipeHandle(conn)
	if err != nil {
		return PeerInfo{}, err
	}

	var pid uint32
	if err := windows.GetNamedPipeClientProcessId(h, &pid); err != nil {
		return PeerInfo{}, fmt.Errorf("unable to get client process id: %w", err)
	}

	peer := PeerInfo{PID: int(pid)}

	if err := windows.ProcessIdToSessionId(pid, &peer.SessionID); err != nil {
		return peer, fmt.Errorf("unable to get client session id: %w", err)
	}

	proc, err := windows.OpenProcess(windows.PROCESS_QUERY_LIMITED_INFORMATION, false, pid)
	if err != nil {
		return peer, fmt.Errorf("unable to open client process: %w", err)
	}
	defer func() { _ = windows.CloseHandle(proc) }()

	var token windows.Token
	if err := windows.OpenProcessToken(proc, windows.TOKEN_QUERY, &token); err != nil {
		return peer, fmt.Errorf("unable to open client process token: %w", err)
	}
	defer func() { _ = token.Close() }()

	user, err := token.GetTokenUser()
	if err != nil {
		return peer, fmt.Errorf("unable to get client token user: %w", err)
	}

	peer.UID = user.User.Sid.String()

	return peer, nil
}

// An unprivileged client cannot open a service's process, so the pipe owner, which an
// unprivileged user cannot set to another account, identifies the server instead.
func serverPeerInfo(conn net.Conn) (PeerInfo, error) {

	h, err := pipeHandle(conn)
	if err != nil {
		return PeerInfo{}, err
	}

	sd, err := windows.GetSecurityInfo(h, windows.SE_KERNEL_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return PeerInfo{}, fmt.Errorf("unable to read pipe owner: %w", err)
	}

	owner, _, err := sd.Owner()
	if err != nil || owner == nil {
		return PeerInfo{}, fmt.Errorf("unable to read pipe owner: %w", err)
	}

	peer := PeerInfo{UID: owner.String()}

	var pid uint32
	if err := windows.GetNamedPipeServerProcessId(h, &pid); err == nil {
		peer.PID = int(pid)
		// Best effort: may be denied to an unprivileged client.
		_ = windows.ProcessIdToSessionId(pid, &peer.SessionID)
	}

	return peer, nil
}

func pipeHandle(conn net.Conn) (windows.Handle, error) {

	fd, ok := conn.(interface{ Fd() uintptr })
	if !ok {
		return 0, errors.New("connection does not expose a pipe handle")
	}

	return windows.Handle(fd.Fd()), nil
}
