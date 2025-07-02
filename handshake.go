package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// 1st message sent from the server
// byte 0 = protocol version no.
// byte 1 = whether encryption is to be used - 0 no , 1 = encryption
func (sc *Server) handshake(conn net.Conn) error {

	if err := sc.one(conn); err != nil {
		return err
	}

	if sc.encryption {
		if err := sc.startEncryption(conn); err != nil {
			return err
		}
	}

	return sc.msgLength(conn)

}

func (sc *Server) one(conn net.Conn) error {

	buff := make([]byte, 2)

	buff[0] = byte(version)

	if sc.encryption {
		buff[1] = byte(1)
	} else {
		buff[1] = byte(0)
	}

	if _, err := conn.Write(buff); err != nil {
		return fmt.Errorf("unable to send handshake: %w", err)
	}

	recv := make([]byte, 1)
	if _, err := conn.Read(recv); err != nil {
		return fmt.Errorf("failed to receive handshake reply: %w", err)
	}

	switch result := recv[0]; result {
	case 0:
		return nil
	case 1:
		return errors.New("client has a different version number")
	case 2:
		return errors.New("client is enforcing encryption")
	case 3:
		return errors.New("server failed to get handshake reply")
	default:
		return errors.New("other error - handshake failed")
	}
}

func (sc *Server) startEncryption(conn net.Conn) error {

	shared, err := sc.keyExchange(conn)
	if err != nil {
		return err
	}

	gcm, err := createCipher(shared)
	if err != nil {
		return err
	}

	sc.enc = &encryption{
		keyExchange: "ecdsa",
		encryption:  "AES-GCM-256",
		cipher:      gcm,
	}

	return nil

}

func (sc *Server) msgLength(conn net.Conn) error {

	toSend := make([]byte, 4)

	buff := make([]byte, 4)
	binary.BigEndian.PutUint32(buff, uint32(sc.maxMsgSize))

	if sc.encryption {
		maxMsg, err := encrypt(*sc.enc.cipher, buff)
		if err != nil {
			return err
		}

		binary.BigEndian.PutUint32(toSend, uint32(len(maxMsg)))
		toSend = append(toSend, maxMsg...)
	} else {
		binary.BigEndian.PutUint32(toSend, uint32(len(buff)))
		toSend = append(toSend, buff...)
	}

	if _, err := conn.Write(toSend); err != nil {
		return fmt.Errorf("unable to send max message length: %w", err)
	}

	reply := make([]byte, 1)

	if _, err := conn.Read(reply); err != nil {
		return fmt.Errorf("did not received message length reply: %w", err)
	}

	return nil

}

// 1st message received by the client
func (cc *Client) handshake(conn net.Conn) error {

	if err := cc.one(conn); err != nil {
		return err
	}

	if cc.encryption {
		if err := cc.startEncryption(conn); err != nil {
			return err
		}
	}

	return cc.msgLength(conn)

}

func (cc *Client) one(conn net.Conn) error {

	recv := make([]byte, 2)
	if _, err := conn.Read(recv); err != nil {
		return fmt.Errorf("failed to receive handshake message: %w", err)
	}

	if recv[0] != version {
		cc.handshakeSendReply(conn, 1)
		return errors.New("server has sent a different version number")
	}

	if recv[1] != 1 && cc.encryptionReq {
		cc.handshakeSendReply(conn, 2)
		return errors.New("server tried to connect without encryption")
	}

	cc.encryption = recv[1] != 0
	cc.handshakeSendReply(conn, 0) // 0 is ok

	return nil

}

func (cc *Client) startEncryption(conn net.Conn) error {

	shared, err := cc.keyExchange(conn)
	if err != nil {
		return err
	}

	gcm, err := createCipher(shared)
	if err != nil {
		return err
	}

	cc.enc = &encryption{
		keyExchange: "ECDSA",
		encryption:  "AES-GCM-256",
		cipher:      gcm,
	}

	return nil
}

func (cc *Client) msgLength(conn net.Conn) error {

	buff := make([]byte, 4)

	if _, err := conn.Read(buff); err != nil {
		return fmt.Errorf("failed to receive max message length 1: %w", err)
	}

	var msgLen uint32
	if err := binary.Read(bytes.NewReader(buff), binary.BigEndian, &msgLen); err != nil { // message length
		return fmt.Errorf("unable to read message length: %w", err)
	}

	buff = make([]byte, int(msgLen))

	_, err := conn.Read(buff)
	if err != nil {
		return fmt.Errorf("failed to receive max message length 2: %w", err)
	}
	var buff2 []byte
	if cc.encryption {
		if buff2, err = decrypt(*cc.enc.cipher, buff); err != nil {
			return fmt.Errorf("failed to receive max message length 3: %w", err)
		}
	} else {
		buff2 = buff
	}

	var maxMsgSize uint32
	if err = binary.Read(bytes.NewReader(buff2), binary.BigEndian, &maxMsgSize); err != nil { // message length
		return fmt.Errorf("unable to read message length: %w", err)
	}

	cc.maxMsgSize = int(maxMsgSize)
	cc.handshakeSendReply(conn, 0)

	return nil

}

func (cc *Client) handshakeSendReply(conn net.Conn, result byte) {

	buff := make([]byte, 1)
	buff[0] = result

	_, _ = conn.Write(buff)
}
