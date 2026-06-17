package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {

	const connectionString = "amqp://guest:guest@localhost:5672"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal("Could not connect to RabbitMQ")
	}
	fmt.Println("Connection to RabbitMQ successful...")

	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		fmt.Printf("Error creating channel: %v", err)

	}
	pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})

	_, queue, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilTopic, routing.GameLogSlug, routing.GameLogSlug+".*", pubsub.DurableQueue)
	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, queue.Name, routing.GameLogSlug+".*", pubsub.DurableQueue, func(log routing.GameLog) pubsub.AckType {
		defer fmt.Println("> ")
		gamelogic.WriteLog(log)
		return pubsub.Ack
	})

	if err != nil {
		fmt.Errorf("count not subscribe to logs %s", err)
	}

	fmt.Printf("Queue %v declared and bound", queue)

	fmt.Println("Starting Peril server...")

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			fmt.Println("Sending resume message...")
			pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Invalid command")

		}

	}

}
