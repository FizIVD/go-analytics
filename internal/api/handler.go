package api

import (
	"compress/gzip"
	"go-event-api/internal/domain"
	"go-event-api/internal/kafka"
	"io"
	"math/rand"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/bytedance/sonic"
	"github.com/oklog/ulid/v2"
	kafkago "github.com/segmentio/kafka-go"
)

// Handler holds the dependencies for the HTTP handlers.
type Handler struct {
	Producer          *kafka.Producer
	MaxBodyBytes      int64
	AcceptingRequests *atomic.Bool
	entropy           *ulid.MonotonicEntropy

	// stats
	Received atomic.Int64
	Enqueued atomic.Int64
	Dropped  atomic.Int64
}

// NewHandler creates a new Handler.
func NewHandler(p *kafka.Producer, maxBodyBytes int64, accepting *atomic.Bool) *Handler {
	return &Handler{
		Producer:          p,
		MaxBodyBytes:      maxBodyBytes,
		AcceptingRequests: accepting,
		entropy:           ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0),
	}
}

// EventHandler handles the /event endpoint.
func (h *Handler) EventHandler(w http.ResponseWriter, r *http.Request) {
	if !h.AcceptingRequests.Load() {
		http.Error(w, "shutting down", http.StatusServiceUnavailable)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.MaxBodyBytes)
	defer r.Body.Close()

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

	var payload domain.Event
	dec := sonic.ConfigFastest.NewDecoder(reader)
	if err := dec.Decode(&payload); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	enriched := domain.EnrichedEvent{
		EventID:   ulid.MustNew(ulid.Timestamp(time.Now()), h.entropy).String(),
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

	msg := kafkago.Message{
		Key:   []byte(enriched.EventID),
		Value: b,
	}

	if h.Producer.Enqueue(msg) {
		h.Received.Add(1)
		h.Enqueued.Add(1)
		w.WriteHeader(http.StatusOK)
	} else {
		h.Dropped.Add(1)
		http.Error(w, "queue full", http.StatusServiceUnavailable)
	}
}

// HealthHandler handles the /health endpoint.
func (h *Handler) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
