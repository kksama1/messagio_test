package main

import (
	"log"
	"messaggio/config"
	"messaggio/internal/db/postgre"
	"messaggio/internal/handlers"
	"net/http"
)

func main() {
	log.Println("server started!")

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

	service := handlers.NewService(pool)
	service.DB.DropTable()
	err = service.DB.InitDB()
	if err != nil {
		log.Fatal(err)
	}
	go service.SendMsgToKafka()

	router := http.NewServeMux()
	server := http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	router.HandleFunc("/send", service.SendMessage)
	router.HandleFunc("/info", service.MessagesInfo)

	if err := server.ListenAndServe(); err != nil {
		log.Fatal(err)
	}

}
