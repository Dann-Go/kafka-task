package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
		// to consume messages
		readerTopic := "notify"
		writerTopic := "confirm"
		partition := 0

		reader, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", readerTopic, partition)
		writer, err := kafka.DialLeader(context.Background(), "tcp", "kafka:9092", writerTopic, partition)
		if err != nil {
			log.Fatal("failed to dial leader:", err)

		}
		var t time.Time
		var notificationMessage NotificationMessage
		reader.SetReadDeadline(t)
		for {
			message, err := reader.ReadMessage(1e3)
			if err != nil {
				log.Fatal("failed to read message:", err)
			}
			err = json.Unmarshal(message.Value, &notificationMessage)
			if err != nil {
				return err
			}
			if notificationMessage.Notify {

				if notificationMessage.Notify {
					fmt.Printf("Message with ID: %d come for confirmation. \n", notificationMessage.ID)
				}
				confirmationMessage, err := json.Marshal(ConfirmationMessage{
					ID:      notificationMessage.ID,
					Confirm: true,
				})
				if err != nil {
					return err
				}

				_, err = writer.WriteMessages(kafka.Message{Value: confirmationMessage})
				if err != nil {
					return err
				}
			}

			time.Sleep(1 * time.Second)
		}
		return nil
	})

	eg.Wait()
}
