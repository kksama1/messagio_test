package kafka

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"messaggio/internal/db/postgre"
	"sync"
)

type Consumer interface {
	ReadMsg(ctx context.Context, wg *sync.WaitGroup, ch chan<- []byte)
}

type Consume struct {
	DB *postgre.DatabaseDriver
}

func ReadMsg(ctx context.Context, wg *sync.WaitGroup, ch chan<- []byte) {

	defer wg.Done()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{"kafka:9093"},
		Topic:       "my-topic",
		Partition:   0,
		MaxBytes:    10e6,
		Logger:      kafka.LoggerFunc(logf),
		ErrorLogger: kafka.LoggerFunc(logf),
	})

	defer r.Close()

	for {
		select {
		case <-ctx.Done():
			log.Println("stopped reading")
			return

		default:
			m, err := r.ReadMessage(ctx)
			log.Println(m.Value)
			if err != nil {
				log.Println("failed to read message", err)
				break
			}
			ch <- m.Value
			m.Value = []byte{}
		}
	}
}
