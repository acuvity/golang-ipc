package ipc

import (
	"context"
	"crypto/cipher"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Server - holds the details of the server listener & config.
// Each accepted client is a *Conn with its own encryption key, status and peer identity.
type Server struct {
	name         string
	listen       net.Listener
	removeSocket func()
	cancel       context.CancelFunc
	status       Status
	received     chan (*Message)
	timeout      time.Duration
	encryption   bool
	maxMsgSize   int
	unMask       bool
	maxConns     int
	writeTimeout time.Duration
	admit        func(*Conn) error
	conns        map[uint64]*Conn
	openConns    int
	nextID       uint64
	wg           sync.WaitGroup
	sync.RWMutex
}

// Conn - a single client connection accepted by a Server.
type Conn struct {
	id             uint64
	server         *Server
	conn           net.Conn
	enc            *encryption
	peer           PeerInfo
	peerErr        error
	toWrite        chan *Message
	events         chan *Message
	ctx            context.Context
	cancel         context.CancelFunc
	closedByServer atomic.Bool
	status         Status
	statusMu       sync.RWMutex
}

// PeerInfo - the identity of the other end of a connection, as reported by the operating system.
//
// UID is the user's SID on Windows and the effective uid on Linux/macOS, except on the client
// on Windows where it is the owner of the pipe. SessionID is the Windows session (0 for
// services and on other platforms).
type PeerInfo struct {
	UID       string
	PID       int
	SessionID uint32
}

// ConnError - an error caused by a specific client, returned by Server.Read.
type ConnError struct {
	Conn *Conn
	Err  error
}

func (e *ConnError) Error() string { return e.Err.Error() }

func (e *ConnError) Unwrap() error { return e.Err }

// Client - holds the details of the client connection and config.
type Client struct {
	Name                  string
	conn                  net.Conn
	cancel                context.CancelFunc
	status                Status
	timeout               float64
	retryTimer            time.Duration // number of seconds before trying to connect again
	verifyServer          func(PeerInfo) error
	connectedAt           time.Time
	consecutiveQuickDrops int
	received              chan (*Message)
	toWrite               chan (*Message)
	encryption            bool
	encryptionReq         bool
	maxMsgSize            int
	enc                   *encryption
	sync.RWMutex
}

// Message - contains the received message.
// On a server, Conn is the client the message relates to.
type Message struct {
	Err     error  // details of any error
	MsgType int    // 0 = reserved , -1 is an internal message (disconnection or error etc), all messages received will be > 0
	Data    []byte // message data received
	Status  string // the status of the connection
	Conn    *Conn
}

// Status - Status of the connection
type Status int

const (

	// NotConnected - 0
	NotConnected Status = iota
	// Listening - 1
	Listening
	// Connecting - 2
	Connecting
	// Connected - 3
	Connected
	// Reconnecting - 4
	Reconnecting
	// Closed - 5
	Closed
	// Closing - 6
	Closing
	// Error - 7
	Error
	// Timeout - 8
	Timeout
	// Disconnected - 9
	Disconnected
)

// ServerConfig - used to pass configuation overrides to ServerStart()
//
// AdmitClient runs before a client can receive anything; returning an error closes it.
// A client that does not read for WriteTimeout is disconnected.
type ServerConfig struct {
	MaxMsgSize        int
	Encryption        bool
	UnmaskPermissions bool
	MaxConnections    int
	WriteTimeout      time.Duration
	AdmitClient       func(*Conn) error
}

// DefaultMaxConnections is used when ServerConfig.MaxConnections is 0.
const DefaultMaxConnections = 64

// DefaultWriteTimeout is used when ServerConfig.WriteTimeout is 0.
const DefaultWriteTimeout = 10 * time.Second

// ClientConfig - used to pass configuation overrides to ClientStart()
//
// VerifyServer is called with the server's identity before the handshake. An error closes the
// connection and the client retries. It must return quickly.
type ClientConfig struct {
	Timeout      float64
	RetryTimer   time.Duration
	Encryption   bool
	VerifyServer func(PeerInfo) error
}

// Encryption - encryption settings
type encryption struct {
	keyExchange string
	encryption  string
	cipher      *cipher.AEAD
}
