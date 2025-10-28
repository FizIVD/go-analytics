package config

import (
	"os"
	"runtime"
	"strconv"
	"time"
)

// Config holds all configuration for the application.
type Config struct {
	KafkaHost       string
	KafkaTopic      string
	Port            string
	Workers         int
	BatchSize       int
	QueueCap        int
	WriteTimeout    time.Duration
	ReadTimeout     time.Duration
	IdleTimeout     time.Duration
	ProducerTimeout time.Duration
	BatchMaxWait    time.Duration
	MaxBodyBytes    int64
	ShutdownTimeout time.Duration
}

// Load populates the config from environment variables.
func Load() *Config {
	cpuCores := runtime.NumCPU()
	return &Config{
		KafkaHost:       getEnv("KAFKA_HOST", "localhost:9092"),
		KafkaTopic:      getEnv("KAFKA_TOPIC", "user-events"),
		Port:            getEnv("PORT", "8080"),
		Workers:         getEnvAsInt("WORKERS", cpuCores*2),
		BatchSize:       getEnvAsInt("BATCH_SIZE", 256),
		QueueCap:        getEnvAsInt("QUEUE_CAP", 100_000),
		WriteTimeout:    getEnvAsDuration("WRITE_TIMEOUT", 5*time.Second),
		ReadTimeout:     getEnvAsDuration("READ_TIMEOUT", 5*time.Second),
		IdleTimeout:     getEnvAsDuration("IDLE_TIMEOUT", 60*time.Second),
		ProducerTimeout: getEnvAsDuration("PRODUCER_TIMEOUT", 3*time.Second),
		BatchMaxWait:    getEnvAsDuration("BATCH_MAX_WAIT", 50*time.Millisecond),
		MaxBodyBytes:    int64(getEnvAsInt("MAX_BODY", 1<<20)), // 1MB
		ShutdownTimeout: getEnvAsDuration("SHUTDOWN_TIMEOUT", 10*time.Second),
	}
}

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
