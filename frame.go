package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

var (
	errFrameTooLarge = errors.New("message received is too large")
	errFrameTooShort = errors.New("message received is too short")
	errUndecryptable = errors.New("message received cannot be decrypted")
)

// Bodies grow as data arrives, so announcing a large message without sending it costs little.
const bodyChunk = 64 * 1024

// readFrame reads one message. errUndecryptable leaves the stream usable, errFrameTooLarge and
// errFrameTooShort do not.
func readFrame(r io.Reader, enc *encryption, maxMsgSize int) (uint32, []byte, error) {

	var bLen [4]byte
	if _, err := io.ReadFull(r, bLen[:]); err != nil {
		return 0, nil, err
	}

	// Unsigned, so a huge length cannot turn negative on 32 bit platforms.
	mLen := binary.BigEndian.Uint32(bLen[:])
	if uint64(mLen) > uint64(maxMsgSize+maxFrameOverhead) {
		return 0, nil, fmt.Errorf("%w (%d bytes)", errFrameTooLarge, mLen)
	}

	body := bytes.NewBuffer(make([]byte, 0, min(int(mLen), bodyChunk)))
	if _, err := io.CopyN(body, r, int64(mLen)); err != nil {
		if errors.Is(err, io.EOF) {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}

	msg := body.Bytes()

	if enc != nil {
		plain, err := decrypt(*enc.cipher, msg)
		if err != nil {
			return 0, nil, fmt.Errorf("%w: %w", errUndecryptable, err)
		}
		msg = plain
	}

	if len(msg) < 4 {
		return 0, nil, fmt.Errorf("%w (%d bytes)", errFrameTooShort, len(msg))
	}

	return binary.BigEndian.Uint32(msg[:4]), msg[4:], nil
}

// writeFrame writes one message with a single Write call.
func writeFrame(w io.Writer, enc *encryption, msgType int, data []byte) error {

	msg := append(intToBytes(msgType), data...)

	if enc != nil {
		var err error
		if msg, err = encrypt(*enc.cipher, msg); err != nil {
			return err
		}
	}

	_, err := w.Write(append(intToBytes(len(msg)), msg...))

	return err
}

// deliverable reports whether a received message type is for the application:
// 0 is reserved and negative types are only used for internal events.
func deliverable(msgType uint32) bool {
	return msgType != 0 && msgType <= math.MaxInt32
}
