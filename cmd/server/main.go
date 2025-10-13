package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

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

	fmt.Println("Starting Peril server...")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("")
	fmt.Println("Shutting down server. Goodbye...")

}
