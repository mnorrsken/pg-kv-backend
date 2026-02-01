package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/mnorrsken/postkeys/internal/cache"
	"github.com/mnorrsken/postkeys/internal/config"
	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/pubsub"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// shutdownTimeout is the maximum time to wait for graceful shutdown
const shutdownTimeout = 30 * time.Second

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
		SQLTrace: cfg.SQLTrace,
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

	// Start metrics server
	metricsSrv := metrics.NewServer(cfg.MetricsAddr)
	if err := metricsSrv.Start(); err != nil {
		log.Fatalf("Failed to start metrics server: %v", err)
	}
	log.Printf("Metrics server listening on %s", cfg.MetricsAddr)

	// Create handler
	h := handler.New(backend, cfg.RedisPassword)

	// Create and start server
	srv := server.NewWithOptions(cfg.RedisAddr, h, cfg.Debug, cfg.Trace)

	// Initialize pub/sub hub
	hub := pubsub.NewHub(store.Pool(), store.ConnString())
	if err := hub.Start(ctx); err != nil {
		log.Fatalf("Failed to start pub/sub hub: %v", err)
	}
	srv.SetPubSubHub(hub)
	log.Println("Pub/sub support enabled")

	if err := srv.Start(ctx); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	if cfg.Debug {
		log.Println("Debug logging is enabled (DEBUG=1)")
	}
	if cfg.SQLTrace {
		log.Println("SQL query tracing is enabled (SQLTRACE=true)")
	}
	if cfg.Trace {
		log.Println("RESP command tracing is enabled (TRACE=true)")
	}
	if cfg.RedisPassword != "" {
		log.Println("Authentication is enabled")
	}
	log.Printf("postkeys is ready to accept connections on %s", cfg.RedisAddr)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)

	// Wait for first shutdown signal
	sig := <-sigChan
	log.Printf("Received signal %v, initiating graceful shutdown...", sig)

	// Create shutdown context with timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer shutdownCancel()

	// Start a goroutine to handle forced shutdown on second signal
	go func() {
		sig := <-sigChan
		log.Printf("Received second signal %v, forcing immediate shutdown", sig)
		os.Exit(1)
	}()

	// Graceful shutdown sequence
	log.Println("Stopping accepting new connections...")
	cancel() // Cancel the main context to signal all goroutines

	// Stop components in order (reverse of startup)
	done := make(chan struct{})
	go func() {
		log.Println("Stopping Redis server...")
		srv.Stop()

		log.Println("Stopping metrics server...")
		metricsSrv.Stop()

		log.Println("Closing database connections...")
		backend.Close()

		close(done)
	}()

	// Wait for shutdown to complete or timeout
	select {
	case <-done:
		log.Println("Graceful shutdown completed successfully")
	case <-shutdownCtx.Done():
		log.Println("Shutdown timed out, forcing exit")
		os.Exit(1)
	}
}
