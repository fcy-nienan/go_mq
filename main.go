package main

import (
	"fmt"
	"mq_client"
	"mq_server"
	"time"
)

func main() {
	go mq_server.StartServer("127.0.0.1:8888")

	time.Sleep(4 * time.Second)

	for i := 0; i < 100; i++ {
		go func() {
			producer := mq_client.Client{Address: "127.0.0.1:8888"}
			producer.ConnectServer()
			for i := 0; i < 100; i++ {
				msgStr := "http://www.baidu.com"
				producer.Send("url", []byte(msgStr))
				fmt.Println("生产者发送了一条消息：" + msgStr)
				time.Sleep(100 * time.Millisecond)
			}
		}()
	}

	go func() {
		consumer := mq_client.Client{Address: "127.0.0.1:8888"}
		consumer.ConnectServer()
		count := 0
		for {
			msg := consumer.Receive("url")
			if msg == nil {
				return
			}
			count++
			fmt.Printf("消费者消费了%d条消息:%s\r\n", count, msg)
			time.Sleep(2 * time.Second)
		}
	}()

	time.Sleep(100000 * time.Second)
}
