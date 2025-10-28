package main

import (
	"go-event-api/internal/api"
	"go-event-api/internal/config"
	"go-event-api/internal/kafka"
	"log"
	"sync/atomic"
)

func main() {
	// 1. Load config
	cfg := config.Load()

	// 2. Init dependencies
	producer := kafka.NewProducer(cfg)
	acceptingRequests := &atomic.Bool{}
	handler := api.NewHandler(producer, cfg.MaxBodyBytes, acceptingRequests)
	server := api.NewServer(cfg, handler)

	// 3. Start producer workers
	producer.Start()
	log.Printf("Kafka producer started with %d workers", cfg.Workers)

	// 4. Run server (blocks until shutdown)
	if err := server.Run(); err != nil {
		log.Fatalf("server run error: %v", err)
	}

	// 5. Stop producer
	producer.Stop()
	log.Println("kafka producer stopped")

	log.Println("graceful shutdown complete")
}
