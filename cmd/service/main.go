package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/omsu-informational-systems/alexey-shishin/internal/config"
	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
	"github.com/omsu-informational-systems/alexey-shishin/internal/httpserver"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := config.Load()
	registry := health.NewRegistry()
	srv := httpserver.New(":"+cfg.Port, cfg.ServiceName, cfg.ServiceVersion, registry)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("%s listening on :%s", cfg.ServiceName, cfg.Port)
		serveErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining connections")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
			os.Exit(1)
		}
		log.Printf("shutdown complete")
	}

	os.Exit(0)
}
