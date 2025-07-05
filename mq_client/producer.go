package mq_client

import (
	"fmt"
	"github.com/fcy-nienan/go_mq/mq_common"
)

func (client *Client) Send(topic string, body []byte) bool {
	request := mq_common.Request{
		Type:  "PRODUCER",
		Topic: topic,
		Body:  body,
	}
	err := client.encoder.Encode(request)
	if err != nil {
		fmt.Println(err)
		return false
	}

	var resp mq_common.Response
	err = client.decoder.Decode(&resp)
	if err != nil {
		fmt.Println(err)
		return false
	}

	return resp.Code == 200
}
