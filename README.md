# golang-ipc
 Golang Inter-process communication library for Window, Mac and Linux.


 ### Overview
 
 A simple to use package that uses unix sockets on Macos/Linux and named pipes on Windows to create a communication channel between two go processes.

### Intergration

As well as using this library just for go processes it was also designed to work with other languages, with the go process as the server and the other languages processing being the client.


#### NodeJs

I currently use this library to comunicate between a ElectronJS GUI and a go program.

Below is a link to the nodeJS client library:

https://github.com/james-barrow/node-ipc-client

#### Python 

To do

## Usage

Create a server with the default configuation and start listening for the client:

```go

	s, err := ipc.StartServer("<name of socket or pipe>", nil)
	if err != nil {
		log.Println(err)
		return
	}

```
Create a client and connect to the server:

```go

	c, err := ipc.StartClient("<name of socket or pipe>", nil)
	if err != nil {
		log.Println(err)
		return
	}

```

### Read messages 

Read each message sent:

```go

    for {

        // message, err := s.Read() server 
        message, err := c.Read() // client

        if err == nil {
            // handle error
        }

        // do something with the received messages
    }

```

All received messages are formated into the type Message

```go

type Message struct {
	Err     error  // details of any error
	MsgType int    // 0 = reserved , -1 is an internal message (disconnection or error etc), all messages received will be > 0
	Data    []byte // message data received
	Status  string // the status of the connection
}

```

### Write a message


From a client:

```go

    err := c.Write(1, []byte("<Message for server"))
    if err != nil {
        // handle error
    }

```

A server can have several clients, so it always says who a message is for:

```go

    // reply to the client a message came from
    err := msg.Conn.Write(1, []byte("<Message for that client"))

    // notify every connected client
    err = s.Broadcast(2, []byte("<Message for all clients"))

```

`Write`/`WriteWithContext` on the server were removed in favour of these two, so code written for a
single client fails to compile rather than silently sending replies to every client.

### Multiple clients

A server accepts several concurrent clients, for example a system service serving one client
process per logged in user. Each client gets its own connection, encryption key and status.

Messages read from the server carry the connection they relate to in `Message.Conn`:
data messages and the status events `Connected`, `Disconnected` (the client went away) and
`Closed` (the server ended the connection: `conn.Close()`, a client that stopped reading or sent
an invalid message). Use it to reply to that client only:

```go

    for {
        msg, err := s.ReadWithContext(ctx)
        if err != nil {
            if errors.Is(err, ipc.ServerConnectionClosed) {
                return
            }
            var connErr *ipc.ConnError // an error caused by one client, e.g. undecryptable data
            if errors.As(err, &connErr) {
                connErr.Conn.Close()
            }
            continue
        }

        switch {
        case msg.MsgType == -1 && msg.Conn != nil:
            log.Println("client", msg.Conn.ID(), msg.Status)
        case msg.MsgType > 0:
            _ = msg.Conn.WriteWithContext(ctx, 5, []byte("reply to this client only"))
        }
    }

```

`s.Broadcast` sends to every connected client. `s.Connections()` returns the currently connected
clients and `conn.Close()` disconnects a single client while the server keeps running.
`s.StatusCode()` is `Connected` while at least one client is connected. A client that goes away is
noticed (and removed from `Connections()`) even while the application is not reading.

Writes copy the message, so the caller may reuse its buffer as soon as `Write` returns.

#### Limits and admission

```go

    config := &ipc.ServerConfig{
        Encryption:     true,
        MaxConnections: 64,               // default 64, further connections are closed straight away
        WriteTimeout:   10 * time.Second, // default 10s, a client that does not read for longer is dropped
        AdmitClient: func(c *ipc.Conn) error { // optional
            peer, err := c.Peer()
            if err != nil {
                return err // unidentified clients are not admitted
            }
            return allowUser(peer.UID)
        },
    }

```

`AdmitClient` runs after the handshake, before the client can receive anything (including
broadcasts) and before its `Connected` event. A rejected client is closed without any event.

#### Client identity

`conn.Peer()` returns the operating system identity of the client process, as reported by the
operating system rather than by the client:

| Field       | Windows                                                        | Linux / macOS |
|-------------|----------------------------------------------------------------|---------------|
| `UID`       | user SID (`S-1-5-21-...`)                                      | numeric effective uid |
| `PID`       | client process ID                                              | client process ID |
| `SessionID` | Windows session: one per interactive logon (console, remote desktop, fast user switching), 0 for services | always 0 |

`UID` matches `os/user.Current().Uid` in the client process (unless it is a setuid program). The
identity is taken as soon as the connection is accepted. If `Peer()` returns an error the client
could not be identified and should be treated as unauthenticated. On Windows, identifying clients
that run as other users requires the server to run with sufficient privileges (e.g. as a
service / LocalSystem).

 ## Advanced Configuaration

Server options:

```go

    config := &ipc.ServerConfig{
		Encryption: (bool),        // allows encryption to be switched off (bool - default is true)
        MaxMsgSize: (int) ,        // the maximum size in bytes of each message ( default is 3145728 / 3Mb)
	    UnmaskPermissions: (bool), // make the socket writeable for other users (default is false)
    }


```

Client options:

```go

	config := ClientConfig  {
		Encryption (bool),          // allows encryption to be switched off (bool - default is true)
		Timeout    (float64),       // number of seconds to wait before timing out trying to connect/reconnect (default is 0 no timeout)
		RetryTimer (time.Duration), // number of seconds to wait before connection retry (default is 20)
		
	}

```

 ### Encryption

 By default the connection established will be encypted, ECDH384 is used for the key exchange and AES 256 GCM is used for the cipher.

 Encryption can be swithed off by passing in a custom configuation to the server & client start function:

```go
	Encryption: false
```

 ### Unix Socket Permissions

 Under most configurations, a socket created by a user will by default not be writable by another user, making it impossible for the client and server to communicate if being run by separate users.

 The permission mask can be dropped during socket creation by passing a custom configuration to the server start function.  **This will make the socket writable for any user.**

```go
	UnmaskPermissions: true	
```
 Note: Tested on Linux, not tested on Mac.

 On Windows `UnmaskPermissions` applies this security descriptor to the named pipe:

 ```
 D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;0x12019b;;;AU)
 ```

 LocalSystem, Administrators and the pipe's owner get full control. Any other authenticated user
 can connect, read and write as a client, but cannot create an instance of the pipe
 (`FILE_CREATE_PIPE_INSTANCE`). That stops another logged-in user from adding their own instance and
 receiving other users' client connections. Remote (network) clients are always rejected.

 ### Verifying the server

 Pipe/socket permissions only protect a server that is already running. While it is not running,
 another local user could create the pipe (or `/tmp/<name>.sock`) themselves and wait for clients.
 To guard against this a client can check who is serving the pipe/socket before sending anything:

```go
	c, err := ipc.StartClient(ctx, "<name>", &ipc.ClientConfig{
		Encryption:   true,
		VerifyServer: ipc.VerifyServerPrivileged(), // a LocalSystem service on Windows, root elsewhere
	})
```

 `VerifyServer` receives the server's `PeerInfo`, which comes from the operating system. On Windows
 its `UID` is the owner of the named pipe: an unprivileged client is not allowed to inspect a
 service's process, but it can read the pipe's owner, which the server sets when creating the pipe
 and an unprivileged user cannot set to another account. A pipe created by a LocalSystem service is
 owned by LocalSystem or the Administrators group, which `VerifyServerPrivileged` accepts. On
 Linux/macOS it is the uid from the socket's peer credentials. For a server that runs as the user
 (e.g. in development) use `ipc.VerifyServerUID(<the user's uid/SID>)`.

 If the check fails, the connection is closed before the handshake. An error wrapping
 `ipc.ServerVerificationFailed` is delivered through `Read`, and the client keeps retrying every
 `RetryTimer` until `Close` is called. The check must return quickly, as the server only waits a few
 seconds for the handshake to start.
 


 ## Testing

 The package has been tested on Mac, Windows and Linux and has extensive test coverage.

## Licence

MIT
