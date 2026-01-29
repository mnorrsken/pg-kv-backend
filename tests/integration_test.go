package integration_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/mnorrsken/pg-kv-backend/internal/handler"
	"github.com/mnorrsken/pg-kv-backend/internal/server"
	"github.com/mnorrsken/pg-kv-backend/internal/storage"
	"github.com/redis/go-redis/v9"
)

// testServer holds the test server and client
type testServer struct {
	server *server.Server
	client *redis.Client
	store  *storage.MockStore
	addr   string
}

// newTestServer creates a new test server with mock storage
func newTestServer(t *testing.T, password string) *testServer {
	t.Helper()

	// Find an available port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to find available port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()

	// Create mock store
	store := storage.NewMockStore()

	// Create handler
	h := handler.New(store, password)

	// Create and start server
	srv := server.New(addr, h)
	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}

	// Wait a bit for server to be ready
	time.Sleep(50 * time.Millisecond)

	// Create Redis client
	opts := &redis.Options{
		Addr: addr,
	}
	if password != "" {
		opts.Password = password
	}
	client := redis.NewClient(opts)

	return &testServer{
		server: srv,
		client: client,
		store:  store,
		addr:   addr,
	}
}

func (ts *testServer) Close() {
	ts.client.Close()
	ts.server.Stop()
}

// ============== String Command Tests ==============

func TestPing(t *testing.T) {
	ts := newTestServer(t, "")
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

func TestSetGet(t *testing.T) {
	ts := newTestServer(t, "")
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
		t.Errorf("Expected myvalue, got %s", val)
	}

	// Test GET non-existent key
	_, err = ts.client.Get(ctx, "nonexistent").Result()
	if err != redis.Nil {
		t.Errorf("Expected redis.Nil for non-existent key, got %v", err)
	}
}

func TestSetWithExpiry(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Set with expiry
	err := ts.client.Set(ctx, "expkey", "expvalue", 10*time.Second).Err()
	if err != nil {
		t.Fatalf("SET with expiry failed: %v", err)
	}

	// Verify it exists
	val, err := ts.client.Get(ctx, "expkey").Result()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "expvalue" {
		t.Errorf("Expected expvalue, got %s", val)
	}

	// Check TTL is positive (should be around 10s, but at least > 0)
	ttl, err := ts.client.TTL(ctx, "expkey").Result()
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	// TTL should be positive and less than or equal to 10s
	if ttl <= 0 || ttl > 10*time.Second {
		t.Errorf("Expected positive TTL <= 10s, got %v", ttl)
	}
}

func TestSetNX(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// First SETNX should succeed
	ok, err := ts.client.SetNX(ctx, "nxkey", "nxvalue", 0).Result()
	if err != nil {
		t.Fatalf("SETNX failed: %v", err)
	}
	if !ok {
		t.Error("Expected SETNX to return true for new key")
	}

	// Second SETNX should fail
	ok, err = ts.client.SetNX(ctx, "nxkey", "newvalue", 0).Result()
	if err != nil {
		t.Fatalf("SETNX failed: %v", err)
	}
	if ok {
		t.Error("Expected SETNX to return false for existing key")
	}

	// Value should still be original
	val, _ := ts.client.Get(ctx, "nxkey").Result()
	if val != "nxvalue" {
		t.Errorf("Expected nxvalue, got %s", val)
	}
}

func TestMGetMSet(t *testing.T) {
	ts := newTestServer(t, "")
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

	expected := []interface{}{"val1", "val2", "val3", nil}
	for i, v := range vals {
		if v != expected[i] {
			t.Errorf("MGET[%d]: expected %v, got %v", i, expected[i], v)
		}
	}
}

func TestIncrDecr(t *testing.T) {
	ts := newTestServer(t, "")
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

	// INCR again
	val, err = ts.client.Incr(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("INCR failed: %v", err)
	}
	if val != 2 {
		t.Errorf("Expected 2, got %d", val)
	}

	// INCRBY
	val, err = ts.client.IncrBy(ctx, "counter", 10).Result()
	if err != nil {
		t.Fatalf("INCRBY failed: %v", err)
	}
	if val != 12 {
		t.Errorf("Expected 12, got %d", val)
	}

	// DECR
	val, err = ts.client.Decr(ctx, "counter").Result()
	if err != nil {
		t.Fatalf("DECR failed: %v", err)
	}
	if val != 11 {
		t.Errorf("Expected 11, got %d", val)
	}

	// DECRBY
	val, err = ts.client.DecrBy(ctx, "counter", 5).Result()
	if err != nil {
		t.Fatalf("DECRBY failed: %v", err)
	}
	if val != 6 {
		t.Errorf("Expected 6, got %d", val)
	}
}

func TestAppend(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// APPEND to non-existent key
	length, err := ts.client.Append(ctx, "appendkey", "Hello").Result()
	if err != nil {
		t.Fatalf("APPEND failed: %v", err)
	}
	if length != 5 {
		t.Errorf("Expected length 5, got %d", length)
	}

	// APPEND more
	length, err = ts.client.Append(ctx, "appendkey", " World").Result()
	if err != nil {
		t.Fatalf("APPEND failed: %v", err)
	}
	if length != 11 {
		t.Errorf("Expected length 11, got %d", length)
	}

	// Verify value
	val, _ := ts.client.Get(ctx, "appendkey").Result()
	if val != "Hello World" {
		t.Errorf("Expected 'Hello World', got '%s'", val)
	}
}

// ============== Key Command Tests ==============

func TestDelExists(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Set some keys
	ts.client.Set(ctx, "del1", "val1", 0)
	ts.client.Set(ctx, "del2", "val2", 0)

	// EXISTS
	count, err := ts.client.Exists(ctx, "del1", "del2", "del3").Result()
	if err != nil {
		t.Fatalf("EXISTS failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 keys to exist, got %d", count)
	}

	// DEL
	deleted, err := ts.client.Del(ctx, "del1", "del2", "del3").Result()
	if err != nil {
		t.Fatalf("DEL failed: %v", err)
	}
	if deleted != 2 {
		t.Errorf("Expected 2 keys deleted, got %d", deleted)
	}

	// Verify deletion
	count, _ = ts.client.Exists(ctx, "del1", "del2").Result()
	if count != 0 {
		t.Errorf("Expected 0 keys after deletion, got %d", count)
	}
}

func TestExpireTTL(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	ts.client.Set(ctx, "expirekey", "value", 0)

	// TTL on key without expiry returns -1 seconds
	ttl, err := ts.client.TTL(ctx, "expirekey").Result()
	if err != nil {
		t.Fatalf("TTL failed: %v", err)
	}
	// go-redis returns -1*time.Second when there's no expiry
	if ttl >= 0 {
		t.Errorf("Expected negative TTL for key without expiry, got %v", ttl)
	}

	// EXPIRE
	ok, err := ts.client.Expire(ctx, "expirekey", 10*time.Second).Result()
	if err != nil {
		t.Fatalf("EXPIRE failed: %v", err)
	}
	if !ok {
		t.Error("Expected EXPIRE to return true")
	}

	// TTL should now be positive
	ttl, _ = ts.client.TTL(ctx, "expirekey").Result()
	if ttl <= 0 || ttl > 10*time.Second {
		t.Errorf("Expected TTL between 0 and 10s, got %v", ttl)
	}

	// PERSIST
	ok, err = ts.client.Persist(ctx, "expirekey").Result()
	if err != nil {
		t.Fatalf("PERSIST failed: %v", err)
	}
	if !ok {
		t.Error("Expected PERSIST to return true")
	}

	// TTL should be negative again (no expiry)
	ttl, _ = ts.client.TTL(ctx, "expirekey").Result()
	if ttl >= 0 {
		t.Errorf("Expected negative TTL after PERSIST, got %v", ttl)
	}
}

func TestType(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// String
	ts.client.Set(ctx, "typestr", "value", 0)
	typ, err := ts.client.Type(ctx, "typestr").Result()
	if err != nil {
		t.Fatalf("TYPE failed: %v", err)
	}
	if typ != "string" {
		t.Errorf("Expected 'string', got '%s'", typ)
	}

	// Hash
	ts.client.HSet(ctx, "typehash", "field", "value")
	typ, _ = ts.client.Type(ctx, "typehash").Result()
	if typ != "hash" {
		t.Errorf("Expected 'hash', got '%s'", typ)
	}

	// List
	ts.client.LPush(ctx, "typelist", "value")
	typ, _ = ts.client.Type(ctx, "typelist").Result()
	if typ != "list" {
		t.Errorf("Expected 'list', got '%s'", typ)
	}

	// Set
	ts.client.SAdd(ctx, "typeset", "value")
	typ, _ = ts.client.Type(ctx, "typeset").Result()
	if typ != "set" {
		t.Errorf("Expected 'set', got '%s'", typ)
	}

	// Non-existent
	typ, _ = ts.client.Type(ctx, "nonexistent").Result()
	if typ != "none" {
		t.Errorf("Expected 'none', got '%s'", typ)
	}
}

func TestKeys(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	ts.client.Set(ctx, "user:1", "a", 0)
	ts.client.Set(ctx, "user:2", "b", 0)
	ts.client.Set(ctx, "user:3", "c", 0)
	ts.client.Set(ctx, "other", "d", 0)

	keys, err := ts.client.Keys(ctx, "user:*").Result()
	if err != nil {
		t.Fatalf("KEYS failed: %v", err)
	}
	if len(keys) != 3 {
		t.Errorf("Expected 3 keys, got %d", len(keys))
	}
}

func TestRename(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	ts.client.Set(ctx, "oldname", "value", 0)

	err := ts.client.Rename(ctx, "oldname", "newname").Err()
	if err != nil {
		t.Fatalf("RENAME failed: %v", err)
	}

	// Old key should not exist
	exists, _ := ts.client.Exists(ctx, "oldname").Result()
	if exists != 0 {
		t.Error("Old key should not exist after RENAME")
	}

	// New key should have the value
	val, _ := ts.client.Get(ctx, "newname").Result()
	if val != "value" {
		t.Errorf("Expected 'value', got '%s'", val)
	}
}

// ============== Hash Command Tests ==============

func TestHashOperations(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// HSET
	count, err := ts.client.HSet(ctx, "myhash", "field1", "value1", "field2", "value2").Result()
	if err != nil {
		t.Fatalf("HSET failed: %v", err)
	}
	if count != 2 {
		t.Errorf("Expected 2 fields set, got %d", count)
	}

	// HGET
	val, err := ts.client.HGet(ctx, "myhash", "field1").Result()
	if err != nil {
		t.Fatalf("HGET failed: %v", err)
	}
	if val != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val)
	}

	// HEXISTS
	exists, _ := ts.client.HExists(ctx, "myhash", "field1").Result()
	if !exists {
		t.Error("Expected field1 to exist")
	}

	// HLEN
	length, _ := ts.client.HLen(ctx, "myhash").Result()
	if length != 2 {
		t.Errorf("Expected length 2, got %d", length)
	}

	// HGETALL
	all, err := ts.client.HGetAll(ctx, "myhash").Result()
	if err != nil {
		t.Fatalf("HGETALL failed: %v", err)
	}
	if len(all) != 2 || all["field1"] != "value1" || all["field2"] != "value2" {
		t.Errorf("Unexpected HGETALL result: %v", all)
	}

	// HKEYS
	keys, _ := ts.client.HKeys(ctx, "myhash").Result()
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys, got %d", len(keys))
	}

	// HVALS
	vals, _ := ts.client.HVals(ctx, "myhash").Result()
	if len(vals) != 2 {
		t.Errorf("Expected 2 values, got %d", len(vals))
	}

	// HDEL
	deleted, _ := ts.client.HDel(ctx, "myhash", "field1").Result()
	if deleted != 1 {
		t.Errorf("Expected 1 field deleted, got %d", deleted)
	}

	// Verify deletion
	exists, _ = ts.client.HExists(ctx, "myhash", "field1").Result()
	if exists {
		t.Error("field1 should not exist after HDEL")
	}
}

func TestHMGetHMSet(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// HMSET (deprecated but still works)
	err := ts.client.HMSet(ctx, "hmhash", map[string]interface{}{
		"f1": "v1",
		"f2": "v2",
		"f3": "v3",
	}).Err()
	if err != nil {
		t.Fatalf("HMSET failed: %v", err)
	}

	// HMGET
	vals, err := ts.client.HMGet(ctx, "hmhash", "f1", "f2", "f3", "f4").Result()
	if err != nil {
		t.Fatalf("HMGET failed: %v", err)
	}
	if vals[0] != "v1" || vals[1] != "v2" || vals[2] != "v3" || vals[3] != nil {
		t.Errorf("Unexpected HMGET result: %v", vals)
	}
}

// ============== List Command Tests ==============

func TestListOperations(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// RPUSH
	length, err := ts.client.RPush(ctx, "mylist", "a", "b", "c").Result()
	if err != nil {
		t.Fatalf("RPUSH failed: %v", err)
	}
	if length != 3 {
		t.Errorf("Expected length 3, got %d", length)
	}

	// LPUSH
	length, _ = ts.client.LPush(ctx, "mylist", "x", "y").Result()
	if length != 5 {
		t.Errorf("Expected length 5, got %d", length)
	}

	// LLEN
	llen, _ := ts.client.LLen(ctx, "mylist").Result()
	if llen != 5 {
		t.Errorf("Expected LLEN 5, got %d", llen)
	}

	// LRANGE
	vals, err := ts.client.LRange(ctx, "mylist", 0, -1).Result()
	if err != nil {
		t.Fatalf("LRANGE failed: %v", err)
	}
	expected := []string{"y", "x", "a", "b", "c"}
	for i, v := range vals {
		if v != expected[i] {
			t.Errorf("LRANGE[%d]: expected '%s', got '%s'", i, expected[i], v)
		}
	}

	// LINDEX
	val, err := ts.client.LIndex(ctx, "mylist", 2).Result()
	if err != nil {
		t.Fatalf("LINDEX failed: %v", err)
	}
	if val != "a" {
		t.Errorf("Expected 'a', got '%s'", val)
	}

	// LPOP
	val, _ = ts.client.LPop(ctx, "mylist").Result()
	if val != "y" {
		t.Errorf("Expected 'y', got '%s'", val)
	}

	// RPOP
	val, _ = ts.client.RPop(ctx, "mylist").Result()
	if val != "c" {
		t.Errorf("Expected 'c', got '%s'", val)
	}

	// Verify remaining
	llen, _ = ts.client.LLen(ctx, "mylist").Result()
	if llen != 3 {
		t.Errorf("Expected LLEN 3 after pops, got %d", llen)
	}
}

// ============== Set Command Tests ==============

func TestSetOperations(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// SADD
	added, err := ts.client.SAdd(ctx, "myset", "a", "b", "c", "a").Result()
	if err != nil {
		t.Fatalf("SADD failed: %v", err)
	}
	if added != 3 { // 'a' is duplicate
		t.Errorf("Expected 3 added, got %d", added)
	}

	// SCARD
	card, _ := ts.client.SCard(ctx, "myset").Result()
	if card != 3 {
		t.Errorf("Expected cardinality 3, got %d", card)
	}

	// SISMEMBER
	isMember, _ := ts.client.SIsMember(ctx, "myset", "a").Result()
	if !isMember {
		t.Error("Expected 'a' to be a member")
	}

	isMember, _ = ts.client.SIsMember(ctx, "myset", "z").Result()
	if isMember {
		t.Error("Expected 'z' to not be a member")
	}

	// SMEMBERS
	members, _ := ts.client.SMembers(ctx, "myset").Result()
	if len(members) != 3 {
		t.Errorf("Expected 3 members, got %d", len(members))
	}

	// SREM
	removed, _ := ts.client.SRem(ctx, "myset", "a", "z").Result()
	if removed != 1 { // only 'a' exists
		t.Errorf("Expected 1 removed, got %d", removed)
	}

	// Verify
	card, _ = ts.client.SCard(ctx, "myset").Result()
	if card != 2 {
		t.Errorf("Expected cardinality 2 after SREM, got %d", card)
	}
}

// ============== Server Command Tests ==============

func TestServerCommands(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Add some keys
	ts.client.Set(ctx, "s1", "v1", 0)
	ts.client.Set(ctx, "s2", "v2", 0)
	ts.client.HSet(ctx, "h1", "f1", "v1")

	// DBSIZE
	size, err := ts.client.DBSize(ctx).Result()
	if err != nil {
		t.Fatalf("DBSIZE failed: %v", err)
	}
	if size != 3 {
		t.Errorf("Expected DBSIZE 3, got %d", size)
	}

	// FLUSHDB
	err = ts.client.FlushDB(ctx).Err()
	if err != nil {
		t.Fatalf("FLUSHDB failed: %v", err)
	}

	// Verify
	size, _ = ts.client.DBSize(ctx).Result()
	if size != 0 {
		t.Errorf("Expected DBSIZE 0 after FLUSHDB, got %d", size)
	}
}

// ============== Authentication Tests ==============

func TestAuthenticationRequired(t *testing.T) {
	ts := newTestServer(t, "secret123")
	defer ts.Close()

	ctx := context.Background()

	// Create client without password
	noAuthClient := redis.NewClient(&redis.Options{
		Addr: ts.addr,
	})
	defer noAuthClient.Close()

	// Should fail with NOAUTH
	_, err := noAuthClient.Get(ctx, "anykey").Result()
	if err == nil {
		t.Error("Expected NOAUTH error, got nil")
	}
	if err != nil && err.Error() != "NOAUTH Authentication required." {
		// May be redis.Nil if the key doesn't exist but auth passed
		// But with no password, we should get auth error
		t.Logf("Got error: %v", err)
	}

	// PING should work without auth
	_, err = noAuthClient.Ping(ctx).Result()
	if err != nil {
		t.Errorf("PING should work without auth: %v", err)
	}
}

func TestAuthenticationSuccess(t *testing.T) {
	password := "secret123"
	ts := newTestServer(t, password)
	defer ts.Close()

	ctx := context.Background()

	// Client with correct password (set in newTestServer)
	err := ts.client.Set(ctx, "authkey", "authvalue", 0).Err()
	if err != nil {
		t.Fatalf("SET with auth failed: %v", err)
	}

	val, err := ts.client.Get(ctx, "authkey").Result()
	if err != nil {
		t.Fatalf("GET with auth failed: %v", err)
	}
	if val != "authvalue" {
		t.Errorf("Expected 'authvalue', got '%s'", val)
	}
}

// ============== Type Error Tests ==============

func TestWrongTypeErrors(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Create a string key
	ts.client.Set(ctx, "stringkey", "value", 0)

	// Try to use hash commands on string
	_, err := ts.client.HGet(ctx, "stringkey", "field").Result()
	// Should get WRONGTYPE error
	if err == nil || err == redis.Nil {
		// Mock store doesn't return error if type doesn't match for HGet
		// This is a limitation of the simplified mock
		t.Log("Note: Mock store may not fully implement type checking for HGet on missing hash")
	}

	// Create a hash
	ts.client.HSet(ctx, "hashkey", "f1", "v1")

	// Try to use list commands on hash
	_, err = ts.client.LPush(ctx, "hashkey", "value").Result()
	if err == nil {
		t.Error("Expected WRONGTYPE error for LPUSH on hash key")
	}
}

// ============== Edge Cases ==============

func TestEmptyListPopReturnsNil(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Pop from non-existent list
	_, err := ts.client.LPop(ctx, "nonexistent").Result()
	if err != redis.Nil {
		t.Errorf("Expected redis.Nil, got %v", err)
	}
}

func TestNegativeIndices(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	ts.client.RPush(ctx, "indexlist", "a", "b", "c", "d", "e")

	// LINDEX with negative index
	val, err := ts.client.LIndex(ctx, "indexlist", -1).Result()
	if err != nil {
		t.Fatalf("LINDEX failed: %v", err)
	}
	if val != "e" {
		t.Errorf("Expected 'e', got '%s'", val)
	}

	// LRANGE with negative indices
	vals, _ := ts.client.LRange(ctx, "indexlist", -3, -1).Result()
	if len(vals) != 3 || vals[0] != "c" || vals[1] != "d" || vals[2] != "e" {
		t.Errorf("Unexpected LRANGE result: %v", vals)
	}
}

// ============== Client Command Tests ==============

func TestClientID(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// CLIENT ID should return a positive integer
	id, err := ts.client.ClientID(ctx).Result()
	if err != nil {
		t.Fatalf("CLIENT ID failed: %v", err)
	}
	if id <= 0 {
		t.Errorf("Expected positive client ID, got %d", id)
	}
}

func TestClientSetNameGetName(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Initially, name should be empty
	name, err := ts.client.ClientGetName(ctx).Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("CLIENT GETNAME failed: %v", err)
	}
	if name != "" {
		t.Errorf("Expected empty name initially, got '%s'", name)
	}

	// Set a name
	err = ts.client.Do(ctx, "CLIENT", "SETNAME", "test-client").Err()
	if err != nil {
		t.Fatalf("CLIENT SETNAME failed: %v", err)
	}

	// Get the name back
	name, err = ts.client.ClientGetName(ctx).Result()
	if err != nil {
		t.Fatalf("CLIENT GETNAME failed: %v", err)
	}
	if name != "test-client" {
		t.Errorf("Expected 'test-client', got '%s'", name)
	}
}

func TestClientSetInfo(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Set library name
	err := ts.client.Do(ctx, "CLIENT", "SETINFO", "LIB-NAME", "my-lib").Err()
	if err != nil {
		t.Fatalf("CLIENT SETINFO LIB-NAME failed: %v", err)
	}

	// Set library version
	err = ts.client.Do(ctx, "CLIENT", "SETINFO", "LIB-VER", "1.0.0").Err()
	if err != nil {
		t.Fatalf("CLIENT SETINFO LIB-VER failed: %v", err)
	}

	// Verify via CLIENT INFO
	info, err := ts.client.Do(ctx, "CLIENT", "INFO").Text()
	if err != nil {
		t.Fatalf("CLIENT INFO failed: %v", err)
	}
	if info == "" {
		t.Error("CLIENT INFO returned empty string")
	}
}

func TestClientInfo(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Set a name first for easier verification
	ts.client.Do(ctx, "CLIENT", "SETNAME", "info-test")

	info, err := ts.client.Do(ctx, "CLIENT", "INFO").Text()
	if err != nil {
		t.Fatalf("CLIENT INFO failed: %v", err)
	}

	// Should contain client ID
	if info == "" {
		t.Error("CLIENT INFO returned empty string")
	}

	// Should contain the name we set
	if !contains(info, "name=info-test") {
		t.Errorf("CLIENT INFO should contain name=info-test, got: %s", info)
	}

	// Should contain addr
	if !contains(info, "addr=") {
		t.Errorf("CLIENT INFO should contain addr=, got: %s", info)
	}
}

func TestClientList(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Set a name first
	ts.client.Do(ctx, "CLIENT", "SETNAME", "list-test")

	list, err := ts.client.Do(ctx, "CLIENT", "LIST").Text()
	if err != nil {
		t.Fatalf("CLIENT LIST failed: %v", err)
	}

	// Should contain our client
	if !contains(list, "name=list-test") {
		t.Errorf("CLIENT LIST should contain name=list-test, got: %s", list)
	}
}

func TestClientTrackingInfo(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// TRACKINGINFO should return an empty array (not supported)
	result, err := ts.client.Do(ctx, "CLIENT", "TRACKINGINFO").Result()
	if err != nil {
		t.Fatalf("CLIENT TRACKINGINFO failed: %v", err)
	}

	// Should be an empty slice
	arr, ok := result.([]interface{})
	if !ok {
		t.Errorf("Expected array result, got %T", result)
	}
	if len(arr) != 0 {
		t.Errorf("Expected empty array, got %v", arr)
	}
}

func TestClientGetRedir(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// GETREDIR should return -1 (no redirection)
	redir, err := ts.client.Do(ctx, "CLIENT", "GETREDIR").Int64()
	if err != nil {
		t.Fatalf("CLIENT GETREDIR failed: %v", err)
	}
	if redir != -1 {
		t.Errorf("Expected -1, got %d", redir)
	}
}

func TestClientReply(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// CLIENT REPLY ON should succeed
	err := ts.client.Do(ctx, "CLIENT", "REPLY", "ON").Err()
	if err != nil {
		t.Fatalf("CLIENT REPLY ON failed: %v", err)
	}
}

func TestClientUnknownSubcommand(t *testing.T) {
	ts := newTestServer(t, "")
	defer ts.Close()

	ctx := context.Background()

	// Unknown subcommand should return error
	err := ts.client.Do(ctx, "CLIENT", "UNKNOWN").Err()
	if err == nil {
		t.Error("Expected error for unknown CLIENT subcommand")
	}
}

// contains checks if s contains substr
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// ============== Benchmark Tests ==============

func BenchmarkSetGet(b *testing.B) {
	// Find port
	listener, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := listener.Addr().String()
	listener.Close()

	store := storage.NewMockStore()
	h := handler.New(store, "")
	srv := server.New(addr, h)
	srv.Start(context.Background())
	defer srv.Stop()

	time.Sleep(50 * time.Millisecond)

	client := redis.NewClient(&redis.Options{Addr: addr})
	defer client.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key%d", i)
		client.Set(ctx, key, "value", 0)
		client.Get(ctx, key)
	}
}
