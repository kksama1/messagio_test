package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"messaggio/config"
	"messaggio/internal/db/postgre"
	"messaggio/internal/kafka"
	"messaggio/internal/model"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

func printMsg(ctx context.Context, wg *sync.WaitGroup, ch <-chan []byte, pool *sql.DB) {
	defer wg.Done()
	var msg model.Message
	for {
		select {
		case <-ctx.Done():
			log.Println("stopped printing messages")
			return
		case message := <-ch:
			err := json.Unmarshal(message, &msg)
			if err != nil {
				log.Println(err)
				continue
			}
			fmt.Println(msg)
			postgre.SetMsgAsProcessed(pool, msg.ID)
		}
	}
}

func main() {
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		exit := make(chan os.Signal, 1)
		signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
		<-exit
		cancel()
	}()

	cfg, err := config.LoadConfig[config.DatabaseConfig]()
	if err != nil {
		log.Fatal(err)
	}

	pool := postgre.CreateConnection(cfg.Host, cfg.Port, cfg.Username, cfg.Password, cfg.Database, cfg.Sslmode)

	defer func() {
		err = postgre.CloseConnection(pool)
		if err != nil {
			log.Println(err)
		}
	}()
	
	if err != nil {
		log.Println(err)
	}

	msgChan := make(chan []byte, 10)
	defer close(msgChan)
	wg.Add(1)
	go kafka.ReadMsg(ctx, &wg, msgChan)
	wg.Add(1)
	go printMsg(ctx, &wg, msgChan, pool)
	wg.Wait()
}
