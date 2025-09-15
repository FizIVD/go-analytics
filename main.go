package main

import (
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bytedance/sonic"
	"github.com/oklog/ulid/v2"
	"github.com/segmentio/kafka-go"
)

// --------------------- STRUCTS ---------------------

// то, что приходит от генератора
type Event struct {
	DeviceID  string                 `json:"device_id"`
	ProfileID int64                  `json:"profile_id"`
	Action    string                 `json:"action"`
	Extras    map[string]interface{} `json:"extras"`
}

// то, что пишем в Kafka
type EnrichedEvent struct {
	EventID   string                 `json:"event_id"`
	EventTime int64                  `json:"event_time"`
	DeviceID  string                 `json:"device_id"`
	ProfileID int64                  `json:"profile_id"`
	Action    string                 `json:"action"`
	Extras    map[string]interface{} `json:"extras"`
}

// --------------------- GLOBALS ---------------------
var (
	received int64
	enqueued int64
	sent     int64
	failed   int64
	dropped  int64
	retries  int64

	kafkaWriter *kafka.Writer
	queue       chan kafka.Message
	entropy     *ulid.MonotonicEntropy
	partitions  int
	workers     int
	batchSize   int
	queueCap    int

	acceptingRequests atomic.Bool
)

// --------------------- ENV HELPERS ---------------------
func getEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

func getEnvAsInt(name string, def int) int {
	if valStr, ok := os.LookupEnv(name); ok {
		if val, err := strconv.Atoi(valStr); err == nil {
			return val
		}
	}
	return def
}

func getEnvAsDuration(name string, def time.Duration) time.Duration {
	if valStr, ok := os.LookupEnv(name); ok {
		if d, err := time.ParseDuration(valStr); err == nil {
			return d
		}
	}
	return def
}

// --------------------- MAIN ---------------------
func main() {
	rand.Seed(time.Now().UnixNano())
	entropy = ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)

	broker := getEnv("KAFKA_HOST", "localhost:9092")
	topic := getEnv("KAFKA_TOPIC", "user-events")
	cpuCores := runtime.NumCPU()
	workers = getEnvAsInt("WORKERS", cpuCores*2)
	batchSize = getEnvAsInt("BATCH_SIZE", 256)
	queueCap = getEnvAsInt("QUEUE_CAP", 100_000)
	port := getEnv("PORT", "8080")

	writeTimeout := getEnvAsDuration("WRITE_TIMEOUT", 5*time.Second)
	readTimeout := getEnvAsDuration("READ_TIMEOUT", 5*time.Second)
	idleTimeout := getEnvAsDuration("IDLE_TIMEOUT", 60*time.Second)
	producerTimeout := getEnvAsDuration("PRODUCER_TIMEOUT", 3*time.Second)
	batchMaxWait := getEnvAsDuration("BATCH_MAX_WAIT", 50*time.Millisecond)

	kafkaWriter = &kafka.Writer{
		Addr:         kafka.TCP(broker),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		BatchSize:    batchSize,
		BatchTimeout: batchMaxWait,
		Async:        false,
		MaxAttempts:  3,
		RequiredAcks: kafka.RequireAll,
	}
	defer kafkaWriter.Close()

	queue = make(chan kafka.Message, queueCap)

	// запуск воркеров
	var wgWorkers sync.WaitGroup
	wgWorkers.Add(workers)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wgWorkers.Done()
			buf := make([]kafka.Message, 0, batchSize)
			timer := time.NewTimer(batchMaxWait)
			defer timer.Stop()

			flush := func(ctx context.Context) {
				if len(buf) == 0 {
					return
				}
				err := kafkaWriter.WriteMessages(ctx, buf...)
				if err != nil {
					atomic.AddInt64(&failed, int64(len(buf)))
					atomic.AddInt64(&retries, 1)
					log.Printf("[worker %d] write batch failed: %v (size=%d)", workerID, err, len(buf))
				} else {
					atomic.AddInt64(&sent, int64(len(buf)))
				}
				buf = buf[:0]
			}

			for {
				timer.Reset(batchMaxWait)
				select {
				case msg, ok := <-queue:
					if !ok {
						ctx, cancel := context.WithTimeout(context.Background(), producerTimeout)
						flush(ctx)
						cancel()
						return
					}
					buf = append(buf, msg)
					if len(buf) >= batchSize {
						ctx, cancel := context.WithTimeout(context.Background(), producerTimeout)
						flush(ctx)
						cancel()
					}
				case <-timer.C:
					ctx, cancel := context.WithTimeout(context.Background(), producerTimeout)
					flush(ctx)
					cancel()
				}
			}
		}(i)
	}

	acceptingRequests.Store(true)

	mux := http.NewServeMux()
	mux.HandleFunc("/event", func(w http.ResponseWriter, r *http.Request) {
		if !acceptingRequests.Load() {
			http.Error(w, "shutting down", http.StatusServiceUnavailable)
			return
		}
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		maxBody := int64(getEnvAsInt("MAX_BODY", 1<<20))
		r.Body = http.MaxBytesReader(w, r.Body, maxBody)
		defer r.Body.Close()

		// gzip поддержка
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				http.Error(w, "bad gzip", http.StatusBadRequest)
				return
			}
			defer gz.Close()
			reader = gz
		}

		// декодим Event
		var payload Event
		dec := sonic.ConfigFastest.NewDecoder(reader)
		if err := dec.Decode(&payload); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}

		// enrich event
		enriched := EnrichedEvent{
			EventID:   ulid.Make().String(),
			EventTime: time.Now().UnixMilli(),
			DeviceID:  payload.DeviceID,
			ProfileID: payload.ProfileID,
			Action:    payload.Action,
			Extras:    payload.Extras,
		}

		b, err := sonic.ConfigFastest.Marshal(&enriched)
		if err != nil {
			http.Error(w, "encode error", http.StatusInternalServerError)
			return
		}

		msg := kafka.Message{
			Key:   []byte(enriched.EventID),
			Value: b,
		}

		select {
		case queue <- msg:
			atomic.AddInt64(&received, 1)
			atomic.AddInt64(&enqueued, 1)
			w.WriteHeader(http.StatusOK)
		default:
			atomic.AddInt64(&dropped, 1)
			http.Error(w, "queue full", http.StatusServiceUnavailable)
		}
	})

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 2 * time.Second,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// сервер
	errCh := make(chan error, 1)
	go func() {
		log.Printf("API listening on :%s (workers=%d, batch=%d, queueCap=%d)", port, workers, batchSize, queueCap)
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	// graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")
	case err := <-errCh:
		log.Printf("http server error: %v", err)
	}

	acceptingRequests.Store(false)

	shutdownTimeout := getEnvAsDuration("SHUTDOWN_TIMEOUT", 10*time.Second)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %v", err)
	}
	cancel()

	close(queue)
	wgWorkers.Wait()

	if err := kafkaWriter.Close(); err != nil {
		log.Printf("kafka writer close error: %v", err)
	}

	log.Println("graceful shutdown complete")
}
