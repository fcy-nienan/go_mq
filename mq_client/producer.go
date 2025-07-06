package mq_client

import (
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
		HandleEncodeError(err)
		return false
	}

	var resp mq_common.Response
	err = client.decoder.Decode(&resp)
	if err != nil {
		HandleDecodeError(err)
		return false
	}

	return resp.Code == 200
}
