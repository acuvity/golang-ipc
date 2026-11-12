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

	var cred *unix.Xucred
	var pid int
	var credErr, pidErr error
	if err := raw.Control(func(fd uintptr) {
		cred, credErr = unix.GetsockoptXucred(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERCRED)
		pid, pidErr = unix.GetsockoptInt(int(fd), unix.SOL_LOCAL, unix.LOCAL_PEERPID)
	}); err != nil {
		return PeerInfo{}, fmt.Errorf("unable to access socket: %w", err)
	}

	if credErr != nil {
		return PeerInfo{}, fmt.Errorf("unable to get peer credentials: %w", credErr)
	}

	peer := PeerInfo{UID: strconv.FormatUint(uint64(cred.Uid), 10)}

	if pidErr != nil {
		return peer, fmt.Errorf("unable to get peer pid: %w", pidErr)
	}

	peer.PID = pid

	return peer, nil
}
