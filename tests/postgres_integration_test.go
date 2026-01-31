//go:build postgres
// +build postgres

package integration_test

import (
	"context"
	"log"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mnorrsken/postkeys/internal/server"
	"github.com/mnorrsken/postkeys/internal/storage"
	"github.com/mnorrsken/postkeysrnal/handler"
	"github.com/redis/go-redis/v9"
)

// pgTestServer holds the test server and client using PostgreSQL storage
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

// newPgTestServer creates a new test server with PostgreSQL storage
func newPgTestServer(t *testing.T, password string) *pgTestServer {
	t.Helper()

	ctx := context.Background()

	// PostgreSQL connection config from environment
	cfg := storage.Config{
		Host:     getEnvOrDefault("PG_HOST", "localhost"),
		Port:     5789, // Use test port from docker-compose.test.yml
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
	if err := store.FlushDB(ctx); err != nil {
		store.Close()
		t.Fatalf("Failed to flush database: %v", err)
	}

	h := handler.New(store, password)

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

	opts := &redis.Options{
		Addr: addr,
	}
	if password != "" {
		opts.Password = password
	}
	client := redis.NewClient(opts)

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

func (ts *pgTestServer) Close() {
	ts.client.Close()
	ts.server.Stop()
	ts.store.Close()
}

// ============== PostgreSQL Integration Tests ==============
// These mirror the mock tests but run against real PostgreSQL

func TestPgPing(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	result, err := ts.client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if result != "PONG" {
		t.Errorf("Expected PONG, got %s", result)
	}
}

func TestPgSetGet(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Test SET
	err := ts.client.Set(ctx, "mykey", "myvalue", 0).Err()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	// Test GET
	val, err := ts.client.Get(ctx, "mykey").Result()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "myvalue" {
		t.Errorf("Expected 'myvalue', got %s", val)
	}

	// Test GET non-existent
	_, err = ts.client.Get(ctx, "nonexistent").Result()
	if err != redis.Nil {
		t.Errorf("Expected redis.Nil for non-existent key, got %v", err)
	}
}

func TestPgSetWithTTL(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// SET with TTL
	err := ts.client.Set(ctx, "ttlkey", "value", 2*time.Second).Err()
	if err != nil {
		t.Fatalf("SET with TTL failed: %v", err)
	}

	// Key should exist
	val, err := ts.client.Get(ctx, "ttlkey").Result()
	if err != nil {
		t.Fatalf("GET after SET with TTL failed: %v", err)
	}
	if val != "value" {
		t.Errorf("Expected 'value', got %s", val)
	}

	// Wait for expiration
	time.Sleep(3 * time.Second)

	// Key should be gone
	_, err = ts.client.Get(ctx, "ttlkey").Result()
	if err != redis.Nil {
		t.Errorf("Expected key to be expired, got %v", err)
	}
}

func TestPgMGetMSet(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// MSET
	err := ts.client.MSet(ctx, "key1", "val1", "key2", "val2", "key3", "val3").Err()
	if err != nil {
		t.Fatalf("MSET failed: %v", err)
	}

	// MGET
	vals, err := ts.client.MGet(ctx, "key1", "key2", "key3", "nonexistent").Result()
	if err != nil {
		t.Fatalf("MGET failed: %v", err)
	}

	if len(vals) != 4 {
		t.Fatalf("Expected 4 values, got %d", len(vals))
	}

	if vals[0] != "val1" || vals[1] != "val2" || vals[2] != "val3" {
		t.Errorf("MGET values mismatch")
	}
	if vals[3] != nil {
		t.Errorf("Expected nil for nonexistent key")
	}
}

func TestPgHashOperations(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// HSET
	count, err := ts.client.HSet(ctx, "myhash", "field1", "value1", "field2", "value2").Result()
	if err != nil {
		t.Fatalf("HSET failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 fields created, got %d", count)
	}

	// HGET
	val, err := ts.client.HGet(ctx, "myhash", "field1").Result()
	if err != nil {
		t.Fatalf("HGET failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got %s", val)
	}

	// HGETALL
	all, err := ts.client.HGetAll(ctx, "myhash").Result()
	if err != nil {
		t.Fatalf("HGETALL failed: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(all))
	}

	// HDEL
	deleted, err := ts.client.HDel(ctx, "myhash", "field1").Result()
	if err != nil {
		t.Fatalf("HDEL failed: %v", err)
	}
	if deleted != 1 {
		t.Errorf("Expected 1 deleted, got %d", deleted)
	}

	// HLEN
	length, err := ts.client.HLen(ctx, "myhash").Result()
	if err != nil {
		t.Fatalf("HLEN failed: %v", err)
	}
	if length != 1 {
		t.Errorf("Expected length 1, got %d", length)
	}
}

func TestPgListOperations(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// LPUSH
	count, err := ts.client.LPush(ctx, "mylist", "a", "b", "c").Result()
	if err != nil {
		t.Fatalf("LPUSH failed: %v", err)
	}
	if count != 3 {
		t.Errorf("Expected 3, got %d", count)
	}

	// LRANGE
	vals, err := ts.client.LRange(ctx, "mylist", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE failed: %v", err)
	}
	// LPUSH adds to head, so order is c, b, a
	if len(vals) != 3 || vals[0] != "c" || vals[1] != "b" || vals[2] != "a" {
		t.Errorf("LRANGE returned unexpected values: %v", vals)
	}

	// RPUSH
	count, err = ts.client.RPush(ctx, "mylist", "d").Result()
	if err != nil {
		t.Fatalf("RPUSH failed: %v", err)
	}
	if count != 4 {
		t.Errorf("Expected 4, got %d", count)
	}

	// LPOP
	val, err := ts.client.LPop(ctx, "mylist").Result()
	if err != nil {
		t.Fatalf("LPOP failed: %v", err)
	}
	if val != "c" {
		t.Errorf("Expected 'c', got %s", val)
	}

	// RPOP
	val, err = ts.client.RPop(ctx, "mylist").Result()
	if err != nil {
		t.Fatalf("RPOP failed: %v", err)
	}
	if val != "d" {
		t.Errorf("Expected 'd', got %s", val)
	}

	// LLEN
	length, err := ts.client.LLen(ctx, "mylist").Result()
	if err != nil {
		t.Fatalf("LLEN failed: %v", err)
	}
	if length != 2 {
		t.Errorf("Expected 2, got %d", length)
	}
}

func TestPgSetOperations(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// SADD
	count, err := ts.client.SAdd(ctx, "myset", "a", "b", "c", "a").Result()
	if err != nil {
		t.Fatalf("SADD failed: %v", err)
	}
	if count != 3 { // 'a' is duplicate
		t.Errorf("Expected 3 added, got %d", count)
	}

	// SCARD
	card, err := ts.client.SCard(ctx, "myset").Result()
	if err != nil {
		t.Fatalf("SCARD failed: %v", err)
	}
	if card != 3 {
		t.Errorf("Expected 3, got %d", card)
	}

	// SISMEMBER
	exists, err := ts.client.SIsMember(ctx, "myset", "a").Result()
	if err != nil {
		t.Fatalf("SISMEMBER failed: %v", err)
	}
	if !exists {
		t.Error("Expected 'a' to be member")
	}

	// SREM
	removed, err := ts.client.SRem(ctx, "myset", "a", "nonexistent").Result()
	if err != nil {
		t.Fatalf("SREM failed: %v", err)
	}
	if removed != 1 {
		t.Errorf("Expected 1 removed, got %d", removed)
	}

	// SMEMBERS
	members, err := ts.client.SMembers(ctx, "myset").Result()
	if err != nil {
		t.Fatalf("SMEMBERS failed: %v", err)
	}
	if len(members) != 2 {
		t.Errorf("Expected 2 members, got %d", len(members))
	}
}

func TestPgIncrDecr(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// INCR on non-existent key
	val, err := ts.client.Incr(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("INCR failed: %v", err)
	}
	if val != 1 {
		t.Errorf("Expected 1, got %d", val)
	}

	// INCRBY
	val, err = ts.client.IncrBy(ctx, "counter", 5).Result()
	if err != nil {
		t.Fatalf("INCRBY failed: %v", err)
	}
	if val != 6 {
		t.Errorf("Expected 6, got %d", val)
	}

	// DECR
	val, err = ts.client.Decr(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("DECR failed: %v", err)
	}
	if val != 5 {
		t.Errorf("Expected 5, got %d", val)
	}

	// DECRBY
	val, err = ts.client.DecrBy(ctx, "counter", 3).Result()
	if err != nil {
		t.Fatalf("DECRBY failed: %v", err)
	}
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}
}

func TestPgDelExists(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Set some keys
	ts.client.Set(ctx, "key1", "val1", 0)
	ts.client.Set(ctx, "key2", "val2", 0)

	// EXISTS
	count, err := ts.client.Exists(ctx, "key1", "key2", "nonexistent").Result()
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 existing, got %d", count)
	}

	// DEL
	deleted, err := ts.client.Del(ctx, "key1", "key2", "nonexistent").Result()
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected 2 deleted, got %d", deleted)
	}

	// Verify keys are gone
	count, _ = ts.client.Exists(ctx, "key1", "key2").Result()
	if count != 0 {
		t.Errorf("Expected 0, keys should be deleted")
	}
}

func TestPgWrongTypeErrors(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Create a string key
	ts.client.Set(ctx, "stringkey", "value", 0)

	// Try hash operation on string - should fail with WRONGTYPE
	_, err := ts.client.HGet(ctx, "stringkey", "field").Result()
	if err == nil {
		t.Error("Expected WRONGTYPE error for HGET on string key")
	}

	// Create a hash key
	ts.client.HSet(ctx, "hashkey", "field", "value")

	// Try list operation on hash - should fail
	_, err = ts.client.LPush(ctx, "hashkey", "item").Result()
	if err == nil {
		t.Error("Expected WRONGTYPE error for LPUSH on hash key")
	}
}

func TestPgUTF8MultiByteCharacters(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	testCases := []struct {
		name  string
		key   string
		value string
	}{
		{"accented_latin", "café_key", "résumé with naïve piñata"},
		{"german_umlauts", "größe", "Größenänderung über Äpfel"},
		{"cjk_chinese", "中文键", "这是中文值"},
		{"emoji_basic", "emoji_key", "Hello 👋 World 🌍 Test 🎉"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := ts.client.Set(ctx, tc.key, tc.value, 0).Err()
			if err != nil {
				t.Fatalf("SET failed for %s: %v", tc.name, err)
			}

			val, err := ts.client.Get(ctx, tc.key).Result()
			if err != nil {
				t.Fatalf("GET failed for %s: %v", tc.name, err)
			}
			if val != tc.value {
				t.Errorf("Value mismatch for %s: expected %q, got %q", tc.name, tc.value, val)
			}
		})
	}
}

func TestPgBinaryData(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Create a value containing all 256 byte values (0x00-0xFF)
	allBytes := make([]byte, 256)
	for i := 0; i < 256; i++ {
		allBytes[i] = byte(i)
	}
	binaryValue := string(allBytes)

	err := ts.client.Set(ctx, "all_bytes_key", binaryValue, 0).Err()
	if err != nil {
		t.Fatalf("SET all bytes failed: %v", err)
	}

	val, err := ts.client.Get(ctx, "all_bytes_key").Result()
	if err != nil {
		t.Fatalf("GET all bytes failed: %v", err)
	}
	if val != binaryValue {
		t.Errorf("All bytes value mismatch: got %d bytes, expected %d bytes", len(val), len(binaryValue))
	}
}

func TestPgBinaryInHash(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Note: PostgreSQL TEXT columns don't support null bytes or invalid UTF-8.
	// Hash field names use TEXT for indexing, so we test with valid UTF-8 field names.
	// Binary data (including null bytes and arbitrary bytes) works in values (BYTEA).
	field := "field_with_special_chars_αβγ"    // Valid UTF-8 field name
	binaryValue := "value\x00\x01\x02\xff\xfe" // Binary value with null bytes (BYTEA)

	err := ts.client.HSet(ctx, "binary_hash", field, binaryValue).Err()
	if err != nil {
		t.Fatalf("HSET binary failed: %v", err)
	}

	val, err := ts.client.HGet(ctx, "binary_hash", field).Result()
	if err != nil {
		t.Fatalf("HGET binary failed: %v", err)
	}
	if val != binaryValue {
		t.Errorf("Binary hash value mismatch")
	}
}

func TestPgBinaryInList(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Binary data in list elements
	binaryElements := []string{
		"elem\x00one",
		"elem\x01\x02\x03",
		"elem\xff\xfe\xfd",
	}

	for _, elem := range binaryElements {
		err := ts.client.RPush(ctx, "binary_list", elem).Err()
		if err != nil {
			t.Fatalf("RPUSH binary failed: %v", err)
		}
	}

	// Verify all elements
	vals, err := ts.client.LRange(ctx, "binary_list", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE binary failed: %v", err)
	}

	if len(vals) != len(binaryElements) {
		t.Fatalf("Expected %d elements, got %d", len(binaryElements), len(vals))
	}

	for i, expected := range binaryElements {
		if vals[i] != expected {
			t.Errorf("Element %d mismatch", i)
		}
	}
}

func TestPgBinaryInSet(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Binary data in set members
	binaryMembers := []string{
		"member\x00one",
		"member\x01\x02\x03",
		"member\xff\xfe\xfd",
	}

	for _, member := range binaryMembers {
		err := ts.client.SAdd(ctx, "binary_set", member).Err()
		if err != nil {
			t.Fatalf("SADD binary failed: %v", err)
		}
	}

	// Verify all members exist
	for _, member := range binaryMembers {
		exists, err := ts.client.SIsMember(ctx, "binary_set", member).Result()
		if err != nil {
			t.Fatalf("SISMEMBER binary failed: %v", err)
		}
		if !exists {
			t.Errorf("Binary member not found in set")
		}
	}
}

func TestPgLargeBlob(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Create a large binary blob (1MB)
	size := 1024 * 1024
	largeBlob := make([]byte, size)
	for i := 0; i < size; i++ {
		largeBlob[i] = byte(i % 256)
	}
	binaryValue := string(largeBlob)

	err := ts.client.Set(ctx, "large_blob_key", binaryValue, 0).Err()
	if err != nil {
		t.Fatalf("SET large blob failed: %v", err)
	}

	val, err := ts.client.Get(ctx, "large_blob_key").Result()
	if err != nil {
		t.Fatalf("GET large blob failed: %v", err)
	}
	if len(val) != size {
		t.Errorf("Large blob size mismatch: expected %d, got %d", size, len(val))
	}
	if val != binaryValue {
		t.Errorf("Large blob content mismatch")
	}
}

func TestPgRename(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	ts.client.Set(ctx, "oldkey", "value", 0)

	err := ts.client.Rename(ctx, "oldkey", "newkey").Err()
	if err != nil {
		t.Fatalf("RENAME failed: %v", err)
	}

	// Old key should not exist
	_, err = ts.client.Get(ctx, "oldkey").Result()
	if err != redis.Nil {
		t.Errorf("Old key should not exist after RENAME")
	}

	// New key should have the value
	val, err := ts.client.Get(ctx, "newkey").Result()
	if err != nil {
		t.Fatalf("GET newkey failed: %v", err)
	}
	if val != "value" {
		t.Errorf("Expected 'value', got %s", val)
	}
}

func TestPgFlushDB(t *testing.T) {
	ts := newPgTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Add some data
	ts.client.Set(ctx, "key1", "val1", 0)
	ts.client.Set(ctx, "key2", "val2", 0)
	ts.client.HSet(ctx, "hash1", "field", "value")

	// FLUSHDB
	err := ts.client.FlushDB(ctx).Err()
	if err != nil {
		t.Fatalf("FLUSHDB failed: %v", err)
	}

	// Verify all keys are gone
	size, err := ts.client.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("DBSIZE failed: %v", err)
	}
	if size != 0 {
		t.Errorf("Expected 0 keys after FLUSHDB, got %d", size)
	}
}

func TestPgAuthentication(t *testing.T) {
	ts := newPgTestServer(t, "secretpassword")
	defer ts.Close()

	ctx := context.Background()

	// Create client with wrong password
	wrongClient := redis.NewClient(&redis.Options{
		Addr:     ts.addr,
		Password: "wrongpassword",
	})
	defer wrongClient.Close()

	// Should fail
	_, err := wrongClient.Set(ctx, "key", "value", 0).Result()
	if err == nil {
		t.Error("Expected authentication error with wrong password")
	}

	// Original client with correct password should work
	err = ts.client.Set(ctx, "key", "value", 0).Err()
	if err != nil {
		t.Fatalf("SET with correct password failed: %v", err)
	}
}
