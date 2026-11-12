package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"
)

func testEncryption(t *testing.T) *encryption {
	t.Helper()

	gcm, err := createCipher([32]byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}

	return &encryption{cipher: gcm}
}

func TestFrameRoundTrip(t *testing.T) {

	for _, enc := range []*encryption{nil, testEncryption(t)} {
		t.Run(fmt.Sprintf("encryption=%t", enc != nil), func(t *testing.T) {

			var buf bytes.Buffer
			messages := [][]byte{nil, []byte("hello"), bytes.Repeat([]byte{7}, 200*1024)}

			for i, m := range messages {
				if err := writeFrame(&buf, enc, i+1, m); err != nil {
					t.Fatal(err)
				}
			}

			// Frames split into single bytes must still be read whole.
			r := iotest.OneByteReader(&buf)

			for i, want := range messages {
				msgType, data, err := readFrame(r, enc, maxMsgSize)
				if err != nil {
					t.Fatalf("message %d: %v", i, err)
				}
				if msgType != uint32(i+1) || !bytes.Equal(data, want) {
					t.Fatalf("message %d: unexpected type %d with %d bytes", i, msgType, len(data))
				}
			}

			if _, _, err := readFrame(r, enc, maxMsgSize); !errors.Is(err, io.EOF) {
				t.Fatalf("expected EOF, got %v", err)
			}
		})
	}
}

func TestFrameErrors(t *testing.T) {

	frame := func(body []byte) *bytes.Buffer {
		return bytes.NewBuffer(append(binary.BigEndian.AppendUint32(nil, uint32(len(body))), body...))
	}

	if _, _, err := readFrame(bytes.NewReader([]byte{0xFF, 0xFF, 0xFF, 0xFF}), nil, maxMsgSize); !errors.Is(err, errFrameTooLarge) {
		t.Fatalf("expected too large, got %v", err)
	}

	if _, _, err := readFrame(frame([]byte{0, 1}), nil, maxMsgSize); !errors.Is(err, errFrameTooShort) {
		t.Fatalf("expected too short, got %v", err)
	}

	cut := frame([]byte{0, 0, 0, 1, 2, 3})
	cut.Truncate(cut.Len() - 2)
	if _, _, err := readFrame(cut, nil, maxMsgSize); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("expected unexpected EOF, got %v", err)
	}

	// An undecryptable frame is skipped and the next one is still read.
	enc := testEncryption(t)
	stream := frame(make([]byte, 40))
	if err := writeFrame(stream, enc, 5, []byte("after")); err != nil {
		t.Fatal(err)
	}

	if _, _, err := readFrame(stream, enc, maxMsgSize); !errors.Is(err, errUndecryptable) {
		t.Fatalf("expected undecryptable, got %v", err)
	}

	if msgType, data, err := readFrame(stream, enc, maxMsgSize); err != nil || msgType != 5 || string(data) != "after" {
		t.Fatalf("unexpected frame after the bad one: %d %q %v", msgType, data, err)
	}
}

func TestDeliverable(t *testing.T) {

	for msgType, want := range map[uint32]bool{0: false, 1: true, 1 << 31: false, 1<<31 - 1: true, 0xFFFFFFFF: false} {
		if got := deliverable(msgType); got != want {
			t.Errorf("deliverable(%d) = %t, want %t", msgType, got, want)
		}
	}
}
