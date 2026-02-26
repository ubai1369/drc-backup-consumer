package main

import (
	_ "github.com/joho/godotenv/autoload"
	"github.com/nsqio/go-nsq"
	"github.com/ubai1369/drc-backup-consumer/handler"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	config := nsq.NewConfig()

	config.MaxAttempts = 5
	config.DefaultRequeueDelay = 2 * time.Second
	config.MaxBackoffDuration = 1 * time.Minute

	consumer, err := nsq.NewConsumer(
		os.Getenv("DRC_BACKUP_TOPIC"),
		os.Getenv("DRC_BACKUP_CHANNEL"),
		config,
	)
	if err != nil {
		log.Fatal(err)
	}

	consumer.ChangeMaxInFlight(10)

	// Add handler
	consumer.AddHandler(&handler.MessageHandler{})

	err = consumer.ConnectToNSQLookupd(os.Getenv("DRC_NSQ_HOST"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("🚀 NSQ Consumer started...")

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Shutdown signal received...")

	consumer.Stop()

	<-consumer.StopChan

	log.Println("Consumer stopped gracefully")
}
