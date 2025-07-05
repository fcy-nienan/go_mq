package mq_client

import (
	"fmt"
	"go_mq/message"
)

func (client *Client) Send(topic string, body []byte) bool {
	request := message.Request{
		Type:  "PRODUCER",
		Topic: topic,
		Body:  body,
	}
	err := client.encoder.Encode(request)
	if err != nil {
		fmt.Println(err)
		return false
	}

	var resp message.Response
	err = client.decoder.Decode(&resp)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return resp.Code == 200
}
