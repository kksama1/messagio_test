package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type Producer interface {
	SendMsg(id int64, msgContent string) error
}

type message struct {
	ID      int64  `json:"id"`
	Content string `json:"content"`
}

func logf(msg string, a ...interface{}) {
	log.Printf(msg, a...)
	log.Println()
}

func SendMsg(id int64, msgContent string) error {

	msgToSend := message{ID: id, Content: msgContent}
	log.Println("SendMsg:", msgToSend)
	byteMsg, err := json.Marshal(&msgToSend)
	if err != nil {
		log.Printf("failed to marshal message: %v", err)
		return err
	}

	w := kafka.Writer{
		Addr:                   kafka.TCP("0.0.0.0:9092"),
		Topic:                  "my-topic",
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
		//Logger:                 kafka.LoggerFunc(logf),
		//ErrorLogger:            kafka.LoggerFunc(logf),
	}

	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = w.WriteMessages(ctx, kafka.Message{Key: []byte("toConsumer"), Value: byteMsg})
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Printf("failed to write messages: %v\n", err)
			return err
		}
		break
	}

	if err = w.Close(); err != nil {
		log.Printf("failed to close writer: %v", err)
	}

	return nil
}
