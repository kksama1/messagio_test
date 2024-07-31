package model

type Message struct {
	ID        int64  `json:"id"`
	Content   string `json:"content"`
	Processed bool   `json:"processed"`
}
