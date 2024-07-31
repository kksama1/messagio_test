package handlers

import (
	"fmt"
	"log"
	"net/http"
)

func (s *Service) MessagesInfo(w http.ResponseWriter, r *http.Request) {

	overallRows, err := s.DB.GetOverallRows()
	if err != nil {
		log.Printf("error during getting overall rows count into db : %v\n", err)
		fmt.Fprintf(w, "error during getting rows count into db")
		return
	}

	processedRows, err := s.DB.GetProcessedRows()
	if err != nil {
		log.Printf("error during getting processed rows count into db : %v\n", err)
		fmt.Fprintf(w, "error during getting rows count into db")
		return
	}

	fmt.Fprintf(w, "Overall in DB %d messages\nAnd %d of them processed", overallRows, processedRows)
}
