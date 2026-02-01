package cache

import (
	"context"
	"time"

	"github.com/mnorrsken/postkeys/internal/metrics"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// CachedStore wraps a storage.Backend with an in-memory cache
type CachedStore struct {
	backend storage.Backend
	cache   *Cache
}

// NewCachedStore creates a new cached storage wrapper
func NewCachedStore(backend storage.Backend, cfg Config) *CachedStore {
	return &CachedStore{
		backend: backend,
		cache:   New(cfg),
	}
}

// Close closes the cached store and underlying backend
func (s *CachedStore) Close() {
	s.cache.Stop()
	s.backend.Close()
}

// ============== String Commands (with caching) ==============

func (s *CachedStore) Get(ctx context.Context, key string) (string, bool, error) {
	// Try cache first
	if value, found := s.cache.Get(key); found {
		metrics.CacheHits.Inc()
		return value, true, nil
	}
	metrics.CacheMisses.Inc()

	// Cache miss - fetch from backend
	value, found, err := s.backend.Get(ctx, key)
	if err != nil {
		return "", false, err
	}

	// Cache the result if found
	if found {
		s.cache.Set(key, value)
	}

	return value, found, nil
}

func (s *CachedStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	err := s.backend.Set(ctx, key, value, ttl)
	if err != nil {
		return err
	}
	// Invalidate cache on write (write-through invalidation)
	s.cache.Invalidate(key)
	return nil
}

func (s *CachedStore) SetNX(ctx context.Context, key, value string) (bool, error) {
	ok, err := s.backend.SetNX(ctx, key, value)
	if err != nil {
		return false, err
	}
	if ok {
		s.cache.Invalidate(key)
	}
	return ok, nil
}

func (s *CachedStore) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	// For simplicity, MGet doesn't use cache (could be optimized later)
	return s.backend.MGet(ctx, keys)
}

func (s *CachedStore) MSet(ctx context.Context, pairs map[string]string) error {
	err := s.backend.MSet(ctx, pairs)
	if err != nil {
		return err
	}
	// Invalidate all keys
	for key := range pairs {
		s.cache.Invalidate(key)
	}
	return nil
}

func (s *CachedStore) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	result, err := s.backend.Incr(ctx, key, delta)
	if err != nil {
		return 0, err
	}
	s.cache.Invalidate(key)
	return result, nil
}

func (s *CachedStore) Append(ctx context.Context, key, value string) (int64, error) {
	result, err := s.backend.Append(ctx, key, value)
	if err != nil {
		return 0, err
	}
	s.cache.Invalidate(key)
	return result, nil
}

// ============== Key Commands ==============

func (s *CachedStore) Del(ctx context.Context, keys []string) (int64, error) {
	count, err := s.backend.Del(ctx, keys)
	if err != nil {
		return 0, err
	}
	s.cache.DeleteMulti(keys)
	return count, nil
}

func (s *CachedStore) Exists(ctx context.Context, keys []string) (int64, error) {
	return s.backend.Exists(ctx, keys)
}

func (s *CachedStore) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	ok, err := s.backend.Expire(ctx, key, ttl)
	if err != nil {
		return false, err
	}
	// Invalidate on TTL change
	s.cache.Invalidate(key)
	return ok, nil
}

func (s *CachedStore) TTL(ctx context.Context, key string) (int64, error) {
	return s.backend.TTL(ctx, key)
}

func (s *CachedStore) PTTL(ctx context.Context, key string) (int64, error) {
	return s.backend.PTTL(ctx, key)
}

func (s *CachedStore) Persist(ctx context.Context, key string) (bool, error) {
	ok, err := s.backend.Persist(ctx, key)
	if err != nil {
		return false, err
	}
	s.cache.Invalidate(key)
	return ok, nil
}

func (s *CachedStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	return s.backend.Keys(ctx, pattern)
}

func (s *CachedStore) Type(ctx context.Context, key string) (storage.KeyType, error) {
	return s.backend.Type(ctx, key)
}

func (s *CachedStore) Rename(ctx context.Context, oldKey, newKey string) error {
	err := s.backend.Rename(ctx, oldKey, newKey)
	if err != nil {
		return err
	}
	s.cache.Invalidate(oldKey)
	s.cache.Invalidate(newKey)
	return nil
}

// ============== Hash Commands (pass-through, no caching) ==============

func (s *CachedStore) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return s.backend.HGet(ctx, key, field)
}

func (s *CachedStore) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	return s.backend.HSet(ctx, key, fields)
}

func (s *CachedStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return s.backend.HDel(ctx, key, fields)
}

func (s *CachedStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.backend.HGetAll(ctx, key)
}

func (s *CachedStore) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return s.backend.HMGet(ctx, key, fields)
}

func (s *CachedStore) HExists(ctx context.Context, key, field string) (bool, error) {
	return s.backend.HExists(ctx, key, field)
}

func (s *CachedStore) HKeys(ctx context.Context, key string) ([]string, error) {
	return s.backend.HKeys(ctx, key)
}

func (s *CachedStore) HVals(ctx context.Context, key string) ([]string, error) {
	return s.backend.HVals(ctx, key)
}

func (s *CachedStore) HLen(ctx context.Context, key string) (int64, error) {
	return s.backend.HLen(ctx, key)
}

// ============== List Commands (pass-through, no caching) ==============

func (s *CachedStore) LPush(ctx context.Context, key string, values []string) (int64, error) {
	return s.backend.LPush(ctx, key, values)
}

func (s *CachedStore) RPush(ctx context.Context, key string, values []string) (int64, error) {
	return s.backend.RPush(ctx, key, values)
}

func (s *CachedStore) LPop(ctx context.Context, key string) (string, bool, error) {
	return s.backend.LPop(ctx, key)
}

func (s *CachedStore) RPop(ctx context.Context, key string) (string, bool, error) {
	return s.backend.RPop(ctx, key)
}

func (s *CachedStore) LLen(ctx context.Context, key string) (int64, error) {
	return s.backend.LLen(ctx, key)
}

func (s *CachedStore) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return s.backend.LRange(ctx, key, start, stop)
}

func (s *CachedStore) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return s.backend.LIndex(ctx, key, index)
}

// ============== Set Commands (pass-through, no caching) ==============

func (s *CachedStore) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	return s.backend.SAdd(ctx, key, members)
}

func (s *CachedStore) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.backend.SRem(ctx, key, members)
}

func (s *CachedStore) SMembers(ctx context.Context, key string) ([]string, error) {
	return s.backend.SMembers(ctx, key)
}

func (s *CachedStore) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return s.backend.SIsMember(ctx, key, member)
}

func (s *CachedStore) SCard(ctx context.Context, key string) (int64, error) {
	return s.backend.SCard(ctx, key)
}

// ============== Sorted Set Commands ==============

func (s *CachedStore) ZAdd(ctx context.Context, key string, members []storage.ZMember) (int64, error) {
	return s.backend.ZAdd(ctx, key, members)
}

func (s *CachedStore) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]storage.ZMember, error) {
	return s.backend.ZRange(ctx, key, start, stop, withScores)
}

func (s *CachedStore) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return s.backend.ZScore(ctx, key, member)
}

func (s *CachedStore) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.backend.ZRem(ctx, key, members)
}

func (s *CachedStore) ZCard(ctx context.Context, key string) (int64, error) {
	return s.backend.ZCard(ctx, key)
}

// ============== Server Commands ==============

func (s *CachedStore) DBSize(ctx context.Context) (int64, error) {
	return s.backend.DBSize(ctx)
}

func (s *CachedStore) FlushDB(ctx context.Context) error {
	err := s.backend.FlushDB(ctx)
	if err != nil {
		return err
	}
	s.cache.Flush()
	return nil
}

// ============== Transaction Support ==============

// BeginTx starts a transaction on the underlying backend
// Note: Transactions bypass the cache and operate directly on the backend
func (s *CachedStore) BeginTx(ctx context.Context) (storage.Transaction, error) {
	return s.backend.BeginTx(ctx)
}

// Ensure CachedStore implements Backend
var _ storage.Backend = (*CachedStore)(nil)
