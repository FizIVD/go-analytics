package api

import (
	"context"
	"errors"
	"fmt"
	"go-event-api/internal/config"
	"log"
	"net/http"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

// Server manages the HTTP server lifecycle.
type Server struct {
	httpServer        *http.Server
	handler           *Handler
	acceptingRequests *atomic.Bool
	shutdownTimeout   time.Duration
}

// NewServer creates a new Server.
func NewServer(cfg *config.Config, h *Handler) *Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/event", h.EventHandler)
	mux.HandleFunc("/health", h.HealthHandler)

	return &Server{
		httpServer: &http.Server{
			Addr:              ":" + cfg.Port,
			Handler:           mux,
			ReadHeaderTimeout: 2 * time.Second,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
		handler:           h,
		acceptingRequests: h.AcceptingRequests,
		shutdownTimeout:   cfg.ShutdownTimeout,
	}
}

// Run starts the server and blocks until a shutdown signal is received.
func (s *Server) Run() error {
	s.acceptingRequests.Store(true)

	errCh := make(chan error, 1)
	go func() {
		log.Printf("API listening on %s", s.httpServer.Addr)
		if err := s.httpServer.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		log.Println("shutdown signal received")
	case err := <-errCh:
		log.Printf("http server error: %v", err)
	}

	s.acceptingRequests.Store(false)
	log.Println("server is shutting down, not accepting new requests")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server shutdown error: %w", err)
	}

	log.Println("http server shutdown complete")
	return nil
}
