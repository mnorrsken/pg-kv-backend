// Package config provides configuration for the server.
package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds the server configuration
type Config struct {
	// Redis server address
	RedisAddr string

	// Redis authentication password (optional)
	RedisPassword string

	// Metrics server address
	MetricsAddr string

	// PostgreSQL configuration
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string
	PGSSLMode  string

	// Cache configuration
	CacheEnabled bool
	CacheTTL     time.Duration
	CacheMaxSize int
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		RedisAddr:     getEnv("REDIS_ADDR", ":6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		MetricsAddr:   getEnv("METRICS_ADDR", ":9090"),
		PGHost:        getEnv("PG_HOST", "localhost"),
		PGPort:        getEnvInt("PG_PORT", 5432),
		PGUser:        getEnv("PG_USER", "postgres"),
		PGPassword:    getEnv("PG_PASSWORD", "postgres"),
		PGDatabase:    getEnv("PG_DATABASE", "pgkv"),
		PGSSLMode:     getEnv("PG_SSLMODE", "disable"),
		CacheEnabled:  getEnvBool("CACHE_ENABLED", false),
		CacheTTL:      getEnvDuration("CACHE_TTL", 250*time.Millisecond),
		CacheMaxSize:  getEnvInt("CACHE_MAX_SIZE", 10000),
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intVal, err := strconv.Atoi(value); err == nil {
			return intVal
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolVal, err := strconv.ParseBool(value); err == nil {
			return boolVal
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}
