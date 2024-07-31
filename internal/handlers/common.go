package handlers

import (
	"database/sql"
	"log"
	"messaggio/internal/db/postgre"
	"messaggio/internal/kafka"
	"messaggio/internal/model"
	"net/http"
)

type ServiceHandler interface {
	SendMessage(w http.ResponseWriter, r *http.Request)
	MessagesInfo(w http.ResponseWriter, r *http.Request)
	SendMsgToKafka()
}

var _ ServiceHandler = (*Service)(nil)

type Service struct {
	DB       *postgre.PostgresDriver
	Messages []model.Message
}

func NewService(pool *sql.DB) *Service {
	return &Service{
		DB: postgre.NewPostgresDriver(pool),
	}
}

func (s *Service) SendMsgToKafka() {
	var helpSlice []model.Message
	for {
		for len(s.Messages) != 0 {
			err := kafka.SendMsg(s.Messages[0].ID, s.Messages[0].Content)
			if err != nil {
				log.Printf("error during sendimg msg via kafka : %v\n", err)
				helpSlice = append(helpSlice, s.Messages[0])
			}
			log.Println("SendMsgToKafka: msg sent")
			s.Messages = s.Messages[1:]
		}

		s.Messages = append(s.Messages, helpSlice...)
	}
}
