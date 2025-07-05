package mq_common

import (
	"sync"
	"time"
)

type Message struct {
	Id        int64
	Body      []byte
	Timestamp time.Time
}

type Topic struct {
	Name     string
	Messages []Message
}

type QueueStore struct {
	Topics map[string]*Topic

	locks map[string]*sync.Mutex
	mu    sync.Mutex
}

type Request struct {
	Type  string `json:"type"`
	Topic string `json:"topic"`
	Body  []byte `json:"body"`
}
type Response struct {
	Code int    `json:"code"`
	Body []byte `json:"body"`
}

func NewQueueStore() QueueStore {
	return QueueStore{
		Topics: make(map[string]*Topic),
		locks:  make(map[string]*sync.Mutex),
	}
}

func (qs *QueueStore) getLocker(topic string) *sync.Mutex {
	qs.mu.Lock()
	defer qs.mu.Unlock()

	if qs.locks[topic] == nil {
		qs.locks[topic] = &sync.Mutex{}
	}
	return qs.locks[topic]
}

func (qs *QueueStore) Enqueue(topicName string, message Message) {
	locker := qs.getLocker(topicName)
	locker.Lock()
	defer locker.Unlock()

	_, ok := qs.Topics[topicName]
	if !ok {
		qs.Topics[topicName] = &Topic{}
		//topic, _ = qs.Topics[topicName]
	}
	qs.Topics[topicName].Messages = append(qs.Topics[topicName].Messages, message)
	//
	//topic.Messages = append(topic.Messages, message)
	//qs.Topics[topicName] = topic
}
func (qs *QueueStore) Dequeue(topicName string) Message {
	locker := qs.getLocker(topicName)
	locker.Lock()
	defer locker.Unlock()

	topic, ok := qs.Topics[topicName]
	if !ok {
		return Message{}
	}
	if len(topic.Messages) == 0 {
		return Message{}
	}

	message := topic.Messages[0]
	topic.Messages = topic.Messages[1:]
	qs.Topics[topicName] = topic

	return message
}
