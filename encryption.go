package ipc

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
)

func (sc *Server) keyExchange() ([32]byte, error) {
	var shared [32]byte

	curve := ecdh.X25519()

	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return shared, fmt.Errorf("unable to generate key: %w", err)
	}
	pub := priv.PublicKey()

	// Send server's public key
	if err := sendPublic(sc.conn, pub.Bytes()); err != nil {
		return shared, fmt.Errorf("unable to send public server key: %w", err)
	}

	// Receive client's public key
	pubBytes, err := recvPublic(sc.conn)
	if err != nil {
		return shared, fmt.Errorf("unable to receive client key: %w", err)
	}

	clientPub, err := curve.NewPublicKey(pubBytes)
	if err != nil {
		return shared, fmt.Errorf("unable to validate client key: %w", err)
	}

	secret, err := priv.ECDH(clientPub)
	if err != nil {
		return shared, fmt.Errorf("unable to get secret from key: %w", err)
	}

	shared = sha256.Sum256(secret)

	return shared, nil
}

func (cc *Client) keyExchange() ([32]byte, error) {
	var shared [32]byte

	curve := ecdh.X25519()

	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		return shared, fmt.Errorf("unable to generate key: %w", err)
	}
	pub := priv.PublicKey()

	// Receive server's public key
	pubRecvd, err := recvPublic(cc.conn)
	if err != nil {
		return shared, fmt.Errorf("unable to receive public server key: %w", err)
	}

	serverPub, err := curve.NewPublicKey(pubRecvd)
	if err != nil {
		return shared, fmt.Errorf("unable to validate server key: %w", err)
	}

	// Send client's public key
	if err := sendPublic(cc.conn, pub.Bytes()); err != nil {
		return shared, fmt.Errorf("unable to send public client key: %w", err)
	}

	// Derive shared secret
	secret, err := priv.ECDH(serverPub)
	if err != nil {
		return shared, fmt.Errorf("unable to get secret from key: %w", err)
	}

	shared = sha256.Sum256(secret)

	return shared, nil
}

func sendPublic(conn net.Conn, pub []byte) error {

	length := len(pub)

	if length == 0 || length > 255 {
		return fmt.Errorf("invalid public key length (%d)", length)
	}

	buf := append([]byte{byte(length)}, pub...)

	n, err := conn.Write(buf)
	if err != nil || n != len(buf) {
		return fmt.Errorf("unable to send public key (%d vs %d): %w", n, len(buf), err)
	}

	return nil
}

func recvPublic(conn net.Conn) ([]byte, error) {

	lenBuf := make([]byte, 1)
	if _, err := io.ReadFull(conn, lenBuf); err != nil {
		return nil, fmt.Errorf("unable to read public key length: %w", err)
	}

	length := int(lenBuf[0])
	if length <= 0 || length > 255 {
		return nil, fmt.Errorf("invalid public key length received (%d)", length)
	}

	pub := make([]byte, length)
	if _, err := io.ReadFull(conn, pub); err != nil {
		return nil, fmt.Errorf("unable to read public key bytes: %w", err)
	}

	return pub, nil
}

func createCipher(shared [32]byte) (*cipher.AEAD, error) {

	b, err := aes.NewCipher(shared[:])
	if err != nil {
		return nil, fmt.Errorf("unable to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(b)
	if err != nil {
		return nil, fmt.Errorf("unable to create GCM: %w", err)
	}

	return &gcm, nil
}

func encrypt(g cipher.AEAD, data []byte) ([]byte, error) {

	nonce := make([]byte, g.NonceSize())

	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("unable to read nonce: %w", err)
	}

	return g.Seal(nonce, nonce, data, nil), nil
}

func decrypt(g cipher.AEAD, recdData []byte) ([]byte, error) {

	nonceSize := g.NonceSize()
	if len(recdData) < nonceSize {
		return nil, errors.New("not enough data to decrypt")
	}

	nonce, recdData := recdData[:nonceSize], recdData[nonceSize:]
	plain, err := g.Open(nil, nonce, recdData, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to decrypt and authenticate ciphertext: %w", err)
	}

	return plain, nil
}
