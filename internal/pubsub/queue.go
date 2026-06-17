package pubsub

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType int

const (
	Unknown SimpleQueueType = iota
	DurableQueue
	TransientQueue
)

var stateName = map[SimpleQueueType]string{
	DurableQueue:   "durable",
	TransientQueue: "transient",
}

func (t SimpleQueueType) String() string {
	if s, ok := stateName[t]; ok {
		return s
	}
	return "unknown"
}

func DeclareAndBind(conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType) (*amqp.Channel, amqp.Queue, error) {

	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, fmt.Errorf("could not create channel: %v", err)
	}

	log.Println("declaring:", queueName, "type:", queueType.String())

	queue, err := channel.QueueDeclare(queueName, queueType == DurableQueue, queueType == TransientQueue, queueType == TransientQueue, false, amqp.Table{"x-dead-letter-exchange": "peril_dlx"})
	if err != nil {
		channel.Close()
		return nil, amqp.Queue{}, fmt.Errorf("could not create queue: %v", err)
	}

	err = channel.QueueBind(queueName, key, exchange, false, nil)
	if err != nil {
		channel.Close()
		return nil, amqp.Queue{}, fmt.Errorf("could not bind queue: %v", err)
	}

	return channel, queue, nil

}
