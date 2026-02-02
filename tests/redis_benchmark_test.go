//go:build redis
// +build redis

package integration

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Redis test client setup
type redisTestClient struct {
	client *redis.Client
	addr   string
}

func newRedisTestClient(t testing.TB) *redisTestClient {
	ctx := context.Background()

	addr := getRedisAddr()

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	// Wait for Redis to be ready with retries
	var err error
	for i := 0; i < 10; i++ {
		err = client.Ping(ctx).Err()
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("Failed to connect to Redis at %s: %v", addr, err)
	}

	// Clean up any existing data
	client.FlushDB(ctx)

	return &redisTestClient{
		client: client,
		addr:   addr,
	}
}

func getRedisAddr() string {
	host := os.Getenv("REDIS_HOST")
	if host == "" {
		host = "localhost"
	}
	port := os.Getenv("REDIS_PORT")
	if port == "" {
		port = "6399"
	}
	return fmt.Sprintf("%s:%s", host, port)
}

func (tc *redisTestClient) Close() {
	tc.client.Close()
}

// Benchmarks against Redis 7
func BenchmarkRedisSetGet(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_key_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		if err := tc.client.Set(ctx, key, value, 0).Err(); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
		if _, err := tc.client.Get(ctx, key).Result(); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkRedisSet(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_set_%d", i)
		value := fmt.Sprintf("bench_value_%d", i)

		if err := tc.client.Set(ctx, key, value, 0).Err(); err != nil {
			b.Fatalf("SET failed: %v", err)
		}
	}
}

func BenchmarkRedisGet(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	// Pre-populate keys
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		tc.client.Set(ctx, key, "value", 0)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench_get_%d", i)
		if _, err := tc.client.Get(ctx, key).Result(); err != nil {
			b.Fatalf("GET failed: %v", err)
		}
	}
}

func BenchmarkRedisMSet(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pairs := make([]interface{}, 20)
		for j := 0; j < 10; j++ {
			pairs[j*2] = fmt.Sprintf("mset_key_%d_%d", i, j)
			pairs[j*2+1] = fmt.Sprintf("value_%d", j)
		}
		if err := tc.client.MSet(ctx, pairs...).Err(); err != nil {
			b.Fatalf("MSET failed: %v", err)
		}
	}
}

func BenchmarkRedisMGet(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	// Pre-populate
	keys := make([]string, 10)
	pairs := make([]interface{}, 20)
	for j := 0; j < 10; j++ {
		keys[j] = fmt.Sprintf("mget_key_%d", j)
		pairs[j*2] = keys[j]
		pairs[j*2+1] = fmt.Sprintf("value_%d", j)
	}
	tc.client.MSet(ctx, pairs...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tc.client.MGet(ctx, keys...).Result(); err != nil {
			b.Fatalf("MGET failed: %v", err)
		}
	}
}

func BenchmarkRedisIncr(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()
	tc.client.Set(ctx, "incr_key", "0", 0)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tc.client.Incr(ctx, "incr_key").Result(); err != nil {
			b.Fatalf("INCR failed: %v", err)
		}
	}
}

func BenchmarkRedisHSetHGet(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		field := fmt.Sprintf("field_%d", i)
		value := fmt.Sprintf("value_%d", i)

		if err := tc.client.HSet(ctx, "bench_hash", field, value).Err(); err != nil {
			b.Fatalf("HSET failed: %v", err)
		}
		if _, err := tc.client.HGet(ctx, "bench_hash", field).Result(); err != nil {
			b.Fatalf("HGET failed: %v", err)
		}
	}
}

func BenchmarkRedisLPushLPop(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := fmt.Sprintf("value_%d", i)
		if err := tc.client.LPush(ctx, "bench_list", value).Err(); err != nil {
			b.Fatalf("LPUSH failed: %v", err)
		}
		if _, err := tc.client.LPop(ctx, "bench_list").Result(); err != nil {
			b.Fatalf("LPOP failed: %v", err)
		}
	}
}

func BenchmarkRedisSAddSMembers(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		member := fmt.Sprintf("member_%d", i)
		if err := tc.client.SAdd(ctx, "bench_set", member).Err(); err != nil {
			b.Fatalf("SADD failed: %v", err)
		}
	}
	// One SMEMBERS at the end
	if _, err := tc.client.SMembers(ctx, "bench_set").Result(); err != nil {
		b.Fatalf("SMEMBERS failed: %v", err)
	}
}

func BenchmarkRedisPipeline(b *testing.B) {
	tc := newRedisTestClient(b)
	defer tc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := tc.client.Pipeline()
		for j := 0; j < 10; j++ {
			key := fmt.Sprintf("pipe_%d_%d", i, j)
			pipe.Set(ctx, key, "value", 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("Pipeline failed: %v", err)
		}
	}
}
