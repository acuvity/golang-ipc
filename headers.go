package ipc

import (
	"encoding/binary"
)

func intToBytes(mLen int) []byte {

	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(mLen))

	return b

}
