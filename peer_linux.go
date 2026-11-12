package ipc

import (
	"errors"
	"fmt"
	"net"
	"strconv"

	"golang.org/x/sys/unix"
)

func peerInfo(conn net.Conn) (PeerInfo, error) {

	uc, ok := conn.(*net.UnixConn)
	if !ok {
		return PeerInfo{}, errors.New("connection is not a unix socket")
	}

	raw, err := uc.SyscallConn()
	if err != nil {
		return PeerInfo{}, fmt.Errorf("unable to access socket: %w", err)
	}

	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED)
	}); err != nil {
		return PeerInfo{}, fmt.Errorf("unable to access socket: %w", err)
	}

	if credErr != nil {
		return PeerInfo{}, fmt.Errorf("unable to get peer credentials: %w", credErr)
	}

	return PeerInfo{
		UID: strconv.FormatUint(uint64(cred.Uid), 10),
		PID: int(cred.Pid),
	}, nil
}
