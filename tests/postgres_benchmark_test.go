//go:build postgres
// +build postgres

package integration

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/handler"
	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
	"github.com/redis/go-redis/v9"
)

// PostgreSQL test server setup
type pgTestServer struct {
	server *server.Server
	client *redis.Client
	store  *storage.Store
	addr   string
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func newPgTestServer(t testing.TB) *pgTestServer {
	ctx := context.Background()

	// PostgreSQL connection config from environment
	cfg := storage.Config{
		Host:     getEnvOrDefault("PG_HOST", "localhost"),
		Port:     5789, // Use test port
		User:     getEnvOrDefault("PG_USER", "postgres"),
		Password: getEnvOrDefault("PG_PASSWORD", "testingpassword"),
		Database: getEnvOrDefault("PG_DATABASE", "postgres"),
		SSLMode:  getEnvOrDefault("PG_SSLMODE", "disable"),
	}

	store, err := storage.New(ctx, cfg)
	if err != nil {
		t.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}

	// Clean up any existing data
	cleanupStore(ctx, store)

	h := handler.New(store, "")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		store.Close()
		t.Fatalf("Failed to create listener: %v", err)
	}

	addr := listener.Addr().String()
	srv := server.New(addr, h)

	go func() {
		if err := srv.ServeWithListener(listener); err != nil {
			log.Printf("Server error: %v", err)
		}
	}()

	// Give server time to start
	time.Sleep(50 * time.Millisecond)

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		store.Close()
		t.Fatalf("Failed to connect to test server: %v", err)
	}

	return &pgTestServer{
		server: srv,
		client: client,
		store:  store,
		addr:   addr,
	}
}

func cleanupStore(ctx context.Context, store *storage.Store) {
	// Use FLUSHDB equivalent - delete all keys
	keys, _ := store.Keys(ctx, "*")
	for _, key := range keys {
		store.Del(ctx, []string{key})
	}
}

func (ts *pgTestServer) Close() {
	ts.client.Close()
	ts.server.Close()
	ts.store.Close()
}

// Benchmarks against PostgreSQL
func BenchmarkPgSetGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		if err := ts.client.Set(ctx, key, value, 0).Err(); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
		if _, err := ts.client.Get(ctx, key).Result(); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkPgSet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_set_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		if err := ts.client.Set(ctx, key, value, 0).Err(); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
	}
}

func BenchmarkPgGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	// Pre-populate keys
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		ts.client.Set(ctx, key, "value", 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		if _, err := ts.client.Get(ctx, key).Result(); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkPgMSet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pairs := make([]interface{}, 20)
		for j := 0; j < 10; j++ {
			pairs[j*2] = fmt.Sprintf("mset_key_%d_%d", i, j)
			pairs[j*2+1] = fmt.Sprintf("value_%d", j)
		}
		if err := ts.client.MSet(ctx, pairs...).Err(); err != nil {
			b.Fatalf("MSET failed: %v", err)
		}
	}
}

func BenchmarkPgMGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	// Pre-populate
	keys := make([]string, 10)
	pairs := make([]interface{}, 20)
	for j := 0; j < 10; j++ {
		keys[j] = fmt.Sprintf("mget_key_%d", j)
		pairs[j*2] = keys[j]
		pairs[j*2+1] = fmt.Sprintf("value_%d", j)
	}
	ts.client.MSet(ctx, pairs...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ts.client.MGet(ctx, keys...).Result(); err != nil {
			b.Fatalf("MGET failed: %v", err)
		}
	}
}

func BenchmarkPgIncr(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()
	ts.client.Set(ctx, "incr_key", "0", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ts.client.Incr(ctx, "incr_key").Result(); err != nil {
			b.Fatalf("INCR failed: %v", err)
		}
	}
}

func BenchmarkPgHSetHGet(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("field_%d", i)
		value := fmt.Sprintf("value_%d", i)

		if err := ts.client.HSet(ctx, "bench_hash", field, value).Err(); err != nil {
			b.Fatalf("HSET failed: %v", err)
		}
		if _, err := ts.client.HGet(ctx, "bench_hash", field).Result(); err != nil {
			b.Fatalf("HGET failed: %v", err)
		}
	}
}

func BenchmarkPgLPushLPop(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("value_%d", i)
		if err := ts.client.LPush(ctx, "bench_list", value).Err(); err != nil {
			b.Fatalf("LPUSH failed: %v", err)
		}
		if _, err := ts.client.LPop(ctx, "bench_list").Result(); err != nil {
			b.Fatalf("LPOP failed: %v", err)
		}
	}
}

func BenchmarkPgSAddSMembers(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("member_%d", i)
		if err := ts.client.SAdd(ctx, "bench_set", member).Err(); err != nil {
			b.Fatalf("SADD failed: %v", err)
		}
	}
	// One SMEMBERS at the end
	if _, err := ts.client.SMembers(ctx, "bench_set").Result(); err != nil {
		b.Fatalf("SMEMBERS failed: %v", err)
	}
}

func BenchmarkPgPipeline(b *testing.B) {
	ts := newPgTestServer(b)
	defer ts.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := ts.client.Pipeline()
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("pipe_%d_%d", i, j)
			pipe.Set(ctx, key, "value", 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("Pipeline failed: %v", err)
		}
	}
}
