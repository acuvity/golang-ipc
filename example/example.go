package main

import (
	"context"
	"log"

	ipc "github.com/james-barrow/golang-ipc"
)

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go server(ctx)

	c, err := ipc.StartClient(ctx, "example1", nil)
	if err != nil {
		log.Println(err)
		return
	}

	for {

		message, err := c.Read()

		if err == nil {

			if message.MsgType == -1 {

				log.Println("client status", c.Status())

				if message.Status == "Reconnecting" {
					c.Close()
					return
				}

			} else {

				log.Println("Client received: "+string(message.Data)+" - Message type: ", message.MsgType)
				_ = c.Write(5, []byte("Message from client - PONG"))

			}

		} else {
			log.Println(err)
			break
		}
	}

}

func server(ctx context.Context) {

	s, err := ipc.StartServer(ctx, "example1", nil)
	if err != nil {
		log.Println("server error", err)
		return
	}

	log.Println("server status", s.Status())

	for {

		message, err := s.Read()

		if err == nil {

			if message.MsgType == -1 {

				if message.Status == "Connected" {

					log.Println("server status", s.Status())
					_ = s.Write(1, []byte("server - PING"))

				}

			} else {

				log.Println("Server received: "+string(message.Data)+" - Message type: ", message.MsgType)
				s.Close()
				return
			}

		} else {
			break
		}
	}

}
