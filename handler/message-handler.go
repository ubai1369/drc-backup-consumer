package handler

import (
	"context"
	"encoding/json"
	"github.com/nsqio/go-nsq"
	"log"
	"time"
)

type MessageHandler struct{}

func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	// Context per message (timeout 10 detik)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := processMessage(ctx, m.Body)
	if err != nil {
		log.Printf("Error processing message: %v", err)
		return err // requeue
	}

	return nil
}

func processMessage(ctx context.Context, body []byte) error {
	var payload NsqBackupDataReq

	// Decode JSON dari NSQ body ke struct
	if err := json.Unmarshal(body, &payload); err != nil {
		log.Printf("Invalid JSON format: %v", err)
		return err
	}

	return payload.SendReq()
}
