package pubsub

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack = iota
	NackRequeue
	NackDiscard
)

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)

}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {

	unmarshaller := func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		var target T
		decoder := gob.NewDecoder(buffer)
		err := decoder.Decode(&target)
		return target, err
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, unmarshaller)

}

func subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType, unmarshaller func([]byte) (T, error)) error {
	_, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not bind to exchange: %v", err)
	}

	fmt.Println("Queue declared and bound to:", queue)

	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("could not create channel: %v", err)
	}
	err = channel.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("could not set QoS: %v", err)
	}
	delCh, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume channel: %v", err)
	}

	go func() {
		defer channel.Close()
		for msg := range delCh {
			target, err := unmarshaller(msg.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			switch handler(target) {
			case Ack:
				msg.Ack(false)
				fmt.Println("Ack")
			case NackDiscard:
				msg.Nack(false, false)
				fmt.Println("NackDiscard")
			case NackRequeue:
				msg.Nack(false, true)
				fmt.Println("NackRequeue")
			}
		}
	}()
	return nil
}
