package mq_server

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/fcy-nienan/go_mq/mq_common"
	"io"
	"log"
	"net"
	"reflect"
	"strings"
	"time"
)

var Qs = mq_common.NewQueueStore()

func HandleDecodeError(err error) {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		log.Printf("Server: Connection closed or incomplete data: %v", err)
	case strings.Contains(err.Error(), "connection reset by peer"):
		log.Printf("Server: Connection reset by peer: %v", err)
	default:
		log.Printf("Server: Decode error: %v", err)
	}
}
func HandleEncodeError(err error) {
	switch {
	case errors.Is(err, io.EOF):
		log.Println("Client: Peer closed the connection")
	case errors.Is(err, io.ErrClosedPipe):
		log.Println("Client: Write to closed pipe")
	case strings.Contains(err.Error(), "connection reset by peer"):
		log.Println("Client: Connection reset by peer")
	case strings.Contains(err.Error(), "use of closed network connection"):
		log.Println("Client: Attempted write on closed connection")
	case strings.Contains(reflect.TypeOf(err).String(), "MarshalerError"):
		log.Printf("Client: Marshal error: %v", err)
	case strings.Contains(reflect.TypeOf(err).String(), "UnsupportedTypeError"):
		log.Printf("Client: Unsupported type in encode: %v", err)
	default:
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			log.Println("Client: Write timeout:", err)
		}
	}
}
func handleConnection(conn net.Conn, qs *mq_common.QueueStore) {
	defer conn.Close()

	fmt.Println("-------处理新客户端----")
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	for {
		var req mq_common.Request
		err := decoder.Decode(&req)
		if err != nil {
			HandleDecodeError(err)
		}
		switch req.Type {
		case "PRODUCER":
			msg := mq_common.Message{
				Id:        time.Now().UnixNano(),
				Body:      req.Body,
				Timestamp: time.Now(),
			}
			qs.Enqueue(req.Topic, msg)
			_ = encoder.Encode(mq_common.Response{Code: 200, Body: []byte("ok")})
		case "CONSUMER":
			msg := qs.Dequeue(req.Topic)

			_ = encoder.Encode(mq_common.Response{Code: 200, Body: msg.Body})
		default:
			_ = encoder.Encode(mq_common.Response{Code: 400, Body: []byte("unknown request type")})

		}
	}
}

func internalStartServer(address string, qs *mq_common.QueueStore) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Println(err)
		fmt.Println("服务启动失败！")
		return
	}

	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			fmt.Println(err)
		}
	}(listener)

	fmt.Println("MQ 服务启动在 :8888")
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println(err)
			continue
		}
		go handleConnection(conn, qs)
	}
}

func StartServer(address string) {
	go internalStartServer(address, &Qs)
}
