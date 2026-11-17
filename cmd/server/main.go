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
	const rabbitConnString = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(rabbitConnString)
	if err != nil {
		log.Fatalf("could not connect to RabbitMQ: %v", err)
	}
	defer conn.Close()
	fmt.Println("Peril game server connected to RabbitMQ!")

	mqChannel, err := conn.Channel()
	if err != nil {
		log.Fatalf("Could not make RabbitMQ channel: %v", err)
	}

	gamelogic.PrintServerHelp()

	_, topicQueue, err := pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)
	if err != nil {
		log.Fatalf("could not bind topic: %v", err)
	}
	fmt.Printf("Queue %v declared and bound!\n", topicQueue.Name)

userInputLoop:
	for {
		s := gamelogic.GetInput()
		if len(s) == 0 {
			continue
		}
		fw := s[0]
		switch fw {
		case "pause":
			fmt.Println("Sending Pause Message")
			err = pubsub.PublishJSON(mqChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Fatalf("Could not publish json to RabbitMQ channel: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume Message")
			err = pubsub.PublishJSON(mqChannel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Could not publish json to RabbitMQ channel: %v", err)
			}
		case "quit", "exit":
			fmt.Println("Exiting")
			break userInputLoop
		default:
			fmt.Println("Unknown command")
		}
	}
}
