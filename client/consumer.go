package client

import (
	"encoding/json"
	"fmt"
	"go_mq/message"
	"net"
)

type Client struct {
	conn net.Conn
	Address string
	encoder *json.Encoder
	decoder *json.Decoder
}

func (client *Client) ConnectServer(){
	conn, err := net.Dial("tcp", client.Address)
	if err != nil {
		fmt.Println(err)
		return
	}
	client.conn = conn
	client.encoder = json.NewEncoder(client.conn)
	client.decoder = json.NewDecoder(client.conn)
}
func (client *Client) Receive(topic string) []byte{
	req := message.Request{
		Type: "CONSUMER",
		Topic: topic,
	}

	err := client.encoder.Encode(req)
	if err != nil {
		panic(err)
	}

	var resp message.Response
	err = client.decoder.Decode(&resp)
	if err != nil {
		fmt.Println(err)
		return nil
	}
	if resp.Code == 200{
		return resp.Body
	}
	return nil
}
