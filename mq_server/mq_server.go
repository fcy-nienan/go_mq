package mq_server

import (
	"encoding/json"
	"fmt"
	"github.com/fcy-nienan/go_mq/mq_common"
	"net"
	"time"
)

var Qs = mq_common.NewQueueStore()

func handleConnection(conn net.Conn, qs *mq_common.QueueStore) {
	defer conn.Close()

	fmt.Println("-------处理新客户端----")
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	var req mq_common.Request
	for {
		err := decoder.Decode(&req)
		if err != nil {
			fmt.Println(err)
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
