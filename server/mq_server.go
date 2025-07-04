package server

import (
	"encoding/json"
	"fmt"
	"go_mq/message"
	"net"
	"time"
)



func handleConnection(conn net.Conn, qs *message.QueueStore) {
	defer conn.Close()

	fmt.Println("-------处理新客户端----")
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)
	var req message.Request
	for {
		err := decoder.Decode(&req)
		if err != nil {
			fmt.Println(err)
		}
		switch req.Type {
		case "PRODUCER":
			msg := message.Message{
				Id: time.Now().UnixNano(),
				Body: req.Body,
				Timestamp: time.Now(),
			}
			qs.Enqueue(req.Topic, msg)
			_ = encoder.Encode(message.Response{Code: 200, Body: []byte("ok")})
		case "CONSUMER":
			msg := qs.Dequeue(req.Topic)

			_ = encoder.Encode(message.Response{Code: 200, Body: msg.Body})
		default:
			_ = encoder.Encode(message.Response{Code: 400, Body: []byte("unknown request type")})

		}
	}
}
func StartServer(qs *message.QueueStore) {
	listener, err := net.Listen("tcp", "127.0.0.1:8888")
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
