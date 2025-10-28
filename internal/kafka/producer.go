package kafka

import (
	"context"
	"go-event-api/internal/config"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer manages a pool of workers that write messages to Kafka.
type Producer struct {
	writer    *kafka.Writer
	queue     chan kafka.Message
	wgWorkers sync.WaitGroup
	cfg       *config.Config

	// some stats, maybe move them later
	Sent    atomic.Int64
	Failed  atomic.Int64
	Retries atomic.Int64
}

// NewProducer creates a new Kafka producer.
func NewProducer(cfg *config.Config) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(cfg.KafkaHost),
		Topic:        cfg.KafkaTopic,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchMaxWait,
		Async:        false,
		MaxAttempts:  3,
		RequiredAcks: kafka.RequireAll,
	}
	return &Producer{
		writer: writer,
		queue:  make(chan kafka.Message, cfg.QueueCap),
		cfg:    cfg,
	}
}

// Start launches the worker pool.
func (p *Producer) Start() {
	p.wgWorkers.Add(p.cfg.Workers)
	for i := 0; i < p.cfg.Workers; i++ {
		go p.worker(i)
	}
}

// Stop gracefully shuts down the producer and its workers.
func (p *Producer) Stop() {
	close(p.queue)
	p.wgWorkers.Wait()
	if err := p.writer.Close(); err != nil {
		log.Printf("kafka writer close error: %v", err)
	}
}

// Enqueue adds a message to the producer's queue. Returns false if the queue is full.
func (p *Producer) Enqueue(msg kafka.Message) bool {
	select {
	case p.queue <- msg:
		return true
	default:
		return false
	}
}

func (p *Producer) worker(workerID int) {
	defer p.wgWorkers.Done()
	buf := make([]kafka.Message, 0, p.cfg.BatchSize)
	timer := time.NewTimer(p.cfg.BatchMaxWait)
	defer timer.Stop()

	flush := func(ctx context.Context) {
		if len(buf) == 0 {
			return
		}
		err := p.writer.WriteMessages(ctx, buf...)
		if err != nil {
			p.Failed.Add(int64(len(buf)))
			p.Retries.Add(1)
			log.Printf("[worker %d] write batch failed: %v (size=%d)", workerID, err, len(buf))
		} else {
			p.Sent.Add(int64(len(buf)))
		}
		buf = buf[:0]
	}

	for {
		timer.Reset(p.cfg.BatchMaxWait)
		select {
		case msg, ok := <-p.queue:
			if !ok {
				ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProducerTimeout)
				flush(ctx)
				cancel()
				return
			}
			buf = append(buf, msg)
			if len(buf) >= p.cfg.BatchSize {
				ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProducerTimeout)
				flush(ctx)
				cancel()
			}
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProducerTimeout)
			flush(ctx)
			cancel()
		}
	}
}
