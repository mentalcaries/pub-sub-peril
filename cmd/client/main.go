package main

import (
	"fmt"
	"log"
	"strconv"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	const connectionString = "amqp://guest:guest@localhost:5672"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatal("Could not connect to RabbitMQ")
	}
	fmt.Println("Connection to RabbitMQ successful...")
	defer connection.Close()

	publishChannel, err := connection.Channel()
	if err != nil {
		log.Fatal("could not create channel")
	}

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println("could not get username")
	}

	_, queue, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.TransientQueue)
	fmt.Printf("Queue %v declared and bound\n", queue.Name)

	if err != nil {
		log.Fatalf("could not subscribe to pause: %v", err)
	}

	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, routing.PauseKey+"."+username, routing.PauseKey, pubsub.TransientQueue, handlerPause(gameState))
	if err != nil {
		log.Fatalf("could not subscribe to exhange: %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.ArmyMovesPrefix+"."+username, routing.ArmyMovesPrefix+".*", pubsub.TransientQueue, HandleMoveMessage(gameState, publishChannel))
	if err != nil {
		log.Fatalf("could not subscribe to moves: %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, "war", routing.WarRecognitionsPrefix+".*", pubsub.DurableQueue, handlerWar(gameState, publishChannel))

	for {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}
		switch input[0] {
		case "move":
			mv, err := gameState.CommandMove(input)
			if err != nil {
				fmt.Println(err)
				continue
			}

			err = pubsub.PublishJSON(
				publishChannel,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+mv.Player.Username,
				mv,
			)
			if err != nil {
				fmt.Printf("error: %s\n", err)
				continue
			}
			fmt.Printf("Moved %v units to %s\n", len(mv.Units), mv.ToLocation)
		case "spawn":
			err = gameState.CommandSpawn(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(input) < 2 {
				fmt.Println("Enter number of to spam")
				continue
			}

			num, err := strconv.Atoi(input[1])
			if err != nil {
				fmt.Println("second argument needs to be a number")
				continue
			}
			for range num {
				msg := gamelogic.GetMaliciousLog()
				publishGameLog(publishChannel, username, msg)
			}
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Invalid command")
		}
	}

}
