// Package config provides configuration for the server.
package config

import (
	"os"
	"strconv"
)

// Config holds the server configuration
type Config struct {
	// Redis server address
	RedisAddr string

	// Redis authentication password (optional)
	RedisPassword string

	// PostgreSQL configuration
	PGHost     string
	PGPort     int
	PGUser     string
	PGPassword string
	PGDatabase string
	PGSSLMode  string
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		RedisAddr:     getEnv("REDIS_ADDR", ":6379"),
		RedisPassword: getEnv("REDIS_PASSWORD", ""),
		PGHost:        getEnv("PG_HOST", "localhost"),
		PGPort:     getEnvInt("PG_PORT", 5432),
		PGUser:     getEnv("PG_USER", "postgres"),
		PGPassword: getEnv("PG_PASSWORD", "postgres"),
		PGDatabase: getEnv("PG_DATABASE", "pgkv"),
		PGSSLMode:  getEnv("PG_SSLMODE", "disable"),
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
