package main

import (
	"fmt"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerWar(gs *gamelogic.GameState, channel *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(decWar gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(decWar)
		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.Ack
		case gamelogic.WarOutcomeOpponentWon:
			if err := publishGameLog(channel, gs.GetUsername(), fmt.Sprintf("%s won a war against %s", winner, loser)); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			if err := publishGameLog(channel, gs.GetUsername(), fmt.Sprintf("%s won a war against %s", winner, loser)); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			if err := publishGameLog(channel, gs.GetUsername(), fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)); err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Printf("error: could not process move. war is evil")
		return pubsub.NackDiscard
	}
}

func publishGameLog(ch *amqp.Channel, username, message string) error {
	routingKey := routing.GameLogSlug + "." + username
	gameLog := routing.GameLog{Username: username, Message: message, CurrentTime: time.Now()}

	return pubsub.PublishGob(ch, routing.ExchangePerilTopic, routingKey, gameLog)

}
