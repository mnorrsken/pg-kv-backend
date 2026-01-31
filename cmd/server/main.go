package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mnorrsken/postkeys/internal/cache"
	"github.com/mnorrsken/postkeys/internal/config"
	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
)

func main() {
	cfg := config.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect to PostgreSQL
	log.Printf("Connecting to PostgreSQL at %s:%d...", cfg.PGHost, cfg.PGPort)
	store, err := storage.New(ctx, storage.Config{
		Host:     cfg.PGHost,
		Port:     cfg.PGPort,
		User:     cfg.PGUser,
		Password: cfg.PGPassword,
		Database: cfg.PGDatabase,
		SSLMode:  cfg.PGSSLMode,
	})
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	log.Println("Connected to PostgreSQL")

	// Wrap with cache if enabled
	var backend storage.Backend = store
	if cfg.CacheEnabled {
		backend = cache.NewCachedStore(store, cache.Config{
			TTL:     cfg.CacheTTL,
			MaxSize: cfg.CacheMaxSize,
		})
		log.Printf("In-memory cache enabled (TTL: %v, MaxSize: %d)", cfg.CacheTTL, cfg.CacheMaxSize)
	}
	defer backend.Close()

	// Start metrics server
	metricsSrv := metrics.NewServer(cfg.MetricsAddr)
	if err := metricsSrv.Start(); err != nil {
		log.Fatalf("Failed to start metrics server: %v", err)
	}
	log.Printf("Metrics server listening on %s", cfg.MetricsAddr)

	// Create handler
	h := handler.New(backend, cfg.RedisPassword)

	// Create and start server
	srv := server.New(cfg.RedisAddr, h)
	if err := srv.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	if cfg.RedisPassword != "" {
		log.Println("Authentication is enabled")
	}
	log.Printf("postkeys is ready to accept connections on %s", cfg.RedisAddr)

	// Wait for shutdown signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()
	srv.Stop()
	metricsSrv.Stop()
	log.Println("Server stopped")
}
