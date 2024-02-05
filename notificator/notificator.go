package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
	"golang.org/x/sync/errgroup"
)

type NotificationMessage struct {
	ID     int64 `json:"id"`
	Notify bool  `json:"notify"`
}

type ConfirmationMessage struct {
	ID      int64 `json:"id"`
	Confirm bool  `json:"confirm"`
}

func main() {

	eg := errgroup.Group{}

	eg.Go(func() error {

		// to produce messages
		topic := "notify"
		partition := 0

		conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err.Error())
		}
		var t time.Time
		conn.SetWriteDeadline(t)
		for {
			message, err := json.Marshal(NotificationMessage{
				ID:     rand.Int63(),
				Notify: true,
			})
			if err != nil {
				return err
			}

			_, err = conn.WriteMessages(
				kafka.Message{Value: message},
			)
			if err != nil {
				log.Fatal("failed to write messages:", err.Error())
			}

			time.Sleep(1 * time.Second)
		}
		return nil
	})

	eg.Go(func() error {
		// to consume messages
		topic := "confirm"
		partition := 0

		conn, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", topic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err.Error())

		}
		var t time.Time
		var confirmationMessage ConfirmationMessage
		conn.SetReadDeadline(t)
		for {
			message, err := conn.ReadMessage(1e3)
			if err != nil {
				log.Fatal("failed to read message:", err.Error())
			}
			err = json.Unmarshal(message.Value, &confirmationMessage)
			if err != nil {
				return err
			}
			if confirmationMessage.Confirm {
				fmt.Printf("Message with ID: %d was confirmed. \n", confirmationMessage.ID)
			}

			time.Sleep(1 * time.Second)
		}
	})

	err := eg.Wait()
	if err != nil {
		log.Fatal(err.Error())
	}
}
