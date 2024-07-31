package handlers

import (
	"encoding/json"
	"fmt"
	"log"
	"messaggio/internal/model"
	"net/http"
)

func (s *Service) SendMessage(w http.ResponseWriter, r *http.Request) {
	var message model.Message

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		log.Printf("error during encoding body: %v\n", err)
		fmt.Fprintf(w, "cant read your messge")
		return
	}

	message.ID, err = s.DB.AddMessage(message.Content)
	if err != nil {
		log.Printf("error during adding msg to db : %v\n", err)
		fmt.Fprintf(w, "error during storing your message into database")
		return
	}

	s.Messages = append(s.Messages, message)

	//log.Println("Msg send to kafka topic")

	log.Println("Message added to db")
	fmt.Fprintf(w, "You message added to database with id:%d\n", message.ID)
}
