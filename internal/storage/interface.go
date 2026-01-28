package storage

import (
	"context"
	"time"
)

// Backend defines the interface for Redis-compatible storage operations
type Backend interface {
	// String commands
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key, value string) (bool, error)
	MGet(ctx context.Context, keys []string) ([]interface{}, error)
	MSet(ctx context.Context, pairs map[string]string) error
	Incr(ctx context.Context, key string, delta int64) (int64, error)
	Append(ctx context.Context, key, value string) (int64, error)

	// Key commands
	Del(ctx context.Context, keys []string) (int64, error)
	Exists(ctx context.Context, keys []string) (int64, error)
	Expire(ctx context.Context, key string, ttl time.Duration) (bool, error)
	TTL(ctx context.Context, key string) (int64, error)
	PTTL(ctx context.Context, key string) (int64, error)
	Persist(ctx context.Context, key string) (bool, error)
	Keys(ctx context.Context, pattern string) ([]string, error)
	Type(ctx context.Context, key string) (KeyType, error)
	Rename(ctx context.Context, oldKey, newKey string) error

	// Hash commands
	HGet(ctx context.Context, key, field string) (string, bool, error)
	HSet(ctx context.Context, key string, fields map[string]string) (int64, error)
	HDel(ctx context.Context, key string, fields []string) (int64, error)
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error)
	HExists(ctx context.Context, key, field string) (bool, error)
	HKeys(ctx context.Context, key string) ([]string, error)
	HVals(ctx context.Context, key string) ([]string, error)
	HLen(ctx context.Context, key string) (int64, error)

	// List commands
	LPush(ctx context.Context, key string, values []string) (int64, error)
	RPush(ctx context.Context, key string, values []string) (int64, error)
	LPop(ctx context.Context, key string) (string, bool, error)
	RPop(ctx context.Context, key string) (string, bool, error)
	LLen(ctx context.Context, key string) (int64, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	LIndex(ctx context.Context, key string, index int64) (string, bool, error)

	// Set commands
	SAdd(ctx context.Context, key string, members []string) (int64, error)
	SRem(ctx context.Context, key string, members []string) (int64, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	SIsMember(ctx context.Context, key, member string) (bool, error)
	SCard(ctx context.Context, key string) (int64, error)

	// Server commands
	DBSize(ctx context.Context) (int64, error)
	FlushDB(ctx context.Context) error

	// Lifecycle
	Close()
}

// Ensure Store implements Backend
var _ Backend = (*Store)(nil)
