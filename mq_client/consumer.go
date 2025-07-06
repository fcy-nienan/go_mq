package mq_client

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

type Client struct {
	Conn    net.Conn
	Address string
	encoder *json.Encoder
	decoder *json.Decoder
}

func (client *Client) ConnectServer() {
	conn, err := net.Dial("tcp", client.Address)
	if err != nil {
		fmt.Println("客户端连接服务器成功！")
		fmt.Println(err)
		return
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(true)
	tcpConn.SetKeepAlivePeriod(30 * time.Second)
	client.Conn = conn
	client.encoder = json.NewEncoder(client.Conn)
	client.decoder = json.NewDecoder(client.Conn)
}
func HandleDecodeError(err error) {
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		log.Printf("Client: Connection closed or incomplete data: %v", err)
	case strings.Contains(err.Error(), "connection reset by peer"):
		log.Printf("Client: Connection reset by peer: %v", err)
	default:
		log.Printf("Client: Decode error: %v", err)
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
func (client *Client) Receive(topic string) []byte {
	req := mq_common.Request{
		Type:  "CONSUMER",
		Topic: topic,
	}

	err := client.encoder.Encode(req)
	if err != nil {
		HandleDecodeError(err)
		return nil
	}

	var resp mq_common.Response
	err = client.decoder.Decode(&resp)
	if err != nil {
		HandleEncodeError(err)
		return nil
	}
	if resp.Code == 200 {
		return resp.Body
	}
	return nil
}
