// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store provides PostgreSQL-backed storage for Redis operations
type Store struct {
	pool    *pgxpool.Pool
	connStr string
	ops     queryOps
}

// Config holds PostgreSQL connection configuration
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

// New creates a new Store with the given configuration
func New(ctx context.Context, cfg Config) (*Store, error) {
	connStr := fmt.Sprintf(
		"user=%s password=%s host=%s port=%d dbname=%s sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode,
	)

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create pool: %w", err)
	}

	store := &Store{pool: pool, connStr: connStr}
	if err := store.initSchema(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	// Start background goroutine to clean expired keys
	go store.cleanupExpiredKeys(ctx)

	return store, nil
}

// Close closes the database connection pool
func (s *Store) Close() {
	s.pool.Close()
}

// Pool returns the underlying connection pool
func (s *Store) Pool() *pgxpool.Pool {
	return s.pool
}

// ConnString returns the connection string
func (s *Store) ConnString() string {
	return s.connStr
}

func (s *Store) initSchema(ctx context.Context) error {
	schema := `
		-- Main key-value store for string types (BYTEA for binary-safe storage)
		CREATE TABLE IF NOT EXISTS kv_strings (
			key TEXT PRIMARY KEY,
			value BYTEA NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_strings_expires ON kv_strings(expires_at) WHERE expires_at IS NOT NULL;

		-- Hash type storage
		CREATE TABLE IF NOT EXISTS kv_hashes (
			key TEXT NOT NULL,
			field TEXT NOT NULL,
			value BYTEA NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, field)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_hashes_expires ON kv_hashes(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_hashes_key ON kv_hashes(key);

		-- List type storage
		CREATE TABLE IF NOT EXISTS kv_lists (
			key TEXT NOT NULL,
			idx BIGINT NOT NULL,
			value BYTEA NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, idx)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_lists_expires ON kv_lists(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_lists_key ON kv_lists(key);

		-- Set type storage
		CREATE TABLE IF NOT EXISTS kv_sets (
			key TEXT NOT NULL,
			member BYTEA NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, member)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_sets_expires ON kv_sets(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_sets_key ON kv_sets(key);

		-- Sorted set type storage
		CREATE TABLE IF NOT EXISTS kv_zsets (
			key TEXT NOT NULL,
			member BYTEA NOT NULL,
			score DOUBLE PRECISION NOT NULL,
			expires_at TIMESTAMPTZ,
			PRIMARY KEY (key, member)
		);
		CREATE INDEX IF NOT EXISTS idx_kv_zsets_expires ON kv_zsets(expires_at) WHERE expires_at IS NOT NULL;
		CREATE INDEX IF NOT EXISTS idx_kv_zsets_key ON kv_zsets(key);
		CREATE INDEX IF NOT EXISTS idx_kv_zsets_score ON kv_zsets(key, score);

		-- Key metadata for tracking types and TTL
		CREATE TABLE IF NOT EXISTS kv_meta (
			key TEXT PRIMARY KEY,
			key_type TEXT NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_meta_expires ON kv_meta(expires_at) WHERE expires_at IS NOT NULL;

		-- HyperLogLog storage (stores serialized HLL registers)
		CREATE TABLE IF NOT EXISTS kv_hyperloglog (
			key TEXT PRIMARY KEY,
			registers BYTEA NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_hyperloglog_expires ON kv_hyperloglog(expires_at) WHERE expires_at IS NOT NULL;
	`
	_, err := s.pool.Exec(ctx, schema)
	return err
}

func (s *Store) cleanupExpiredKeys(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.deleteExpiredKeys(context.Background())
		}
	}
}

func (s *Store) deleteExpiredKeys(ctx context.Context) {
	now := time.Now()
	queries := []string{
		"DELETE FROM kv_strings WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_hashes WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_lists WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_sets WHERE expires_at IS NOT NULL AND expires_at <= $1",
		"DELETE FROM kv_meta WHERE expires_at IS NOT NULL AND expires_at <= $1",
	}
	for _, q := range queries {
		s.pool.Exec(ctx, q, now)
	}
}

// withTx wraps an operation in a transaction
func (s *Store) withTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// BeginTx starts a new transaction
func (s *Store) BeginTx(ctx context.Context) (Transaction, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TxStore{tx: tx}, nil
}

// ============== String Commands ==============

func (s *Store) Get(ctx context.Context, key string) (string, bool, error) {
	return s.ops.get(ctx, s.pool, key)
}

func (s *Store) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.set(ctx, tx, key, value, ttl)
	})
}

func (s *Store) SetNX(ctx context.Context, key, value string) (bool, error) {
	return s.ops.setNX(ctx, s.pool, key, value)
}

func (s *Store) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	return s.ops.mGet(ctx, s.pool, keys)
}

func (s *Store) MSet(ctx context.Context, pairs map[string]string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.mSet(ctx, tx, pairs)
	})
}

func (s *Store) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.incr(ctx, tx, key, delta)
		return err
	})
	return result, err
}

func (s *Store) Append(ctx context.Context, key, value string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.appendStr(ctx, tx, key, value)
		return err
	})
	return result, err
}

// ============== Key Commands ==============

func (s *Store) Del(ctx context.Context, keys []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.del(ctx, tx, keys)
		return err
	})
	return result, err
}

func (s *Store) Exists(ctx context.Context, keys []string) (int64, error) {
	return s.ops.exists(ctx, s.pool, keys)
}

func (s *Store) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return s.ops.expire(ctx, s.pool, key, ttl)
}

func (s *Store) TTL(ctx context.Context, key string) (int64, error) {
	return s.ops.ttl(ctx, s.pool, key)
}

func (s *Store) PTTL(ctx context.Context, key string) (int64, error) {
	return s.ops.pttl(ctx, s.pool, key)
}

func (s *Store) Persist(ctx context.Context, key string) (bool, error) {
	return s.ops.persist(ctx, s.pool, key)
}

func (s *Store) Keys(ctx context.Context, pattern string) ([]string, error) {
	return s.ops.keys(ctx, s.pool, pattern)
}

func (s *Store) Type(ctx context.Context, key string) (KeyType, error) {
	return s.ops.keyType(ctx, s.pool, key)
}

func (s *Store) Rename(ctx context.Context, oldKey, newKey string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		return s.ops.rename(ctx, tx, oldKey, newKey)
	})
}

// ============== Hash Commands ==============

func (s *Store) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return s.ops.hGet(ctx, s.pool, key, field)
}

func (s *Store) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.hSet(ctx, tx, key, fields)
		return err
	})
	return result, err
}

func (s *Store) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return s.ops.hDel(ctx, s.pool, key, fields)
}

func (s *Store) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return s.ops.hGetAll(ctx, s.pool, key)
}

func (s *Store) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return s.ops.hMGet(ctx, s.pool, key, fields)
}

func (s *Store) HExists(ctx context.Context, key, field string) (bool, error) {
	return s.ops.hExists(ctx, s.pool, key, field)
}

func (s *Store) HKeys(ctx context.Context, key string) ([]string, error) {
	return s.ops.hKeys(ctx, s.pool, key)
}

func (s *Store) HVals(ctx context.Context, key string) ([]string, error) {
	return s.ops.hVals(ctx, s.pool, key)
}

func (s *Store) HLen(ctx context.Context, key string) (int64, error) {
	return s.ops.hLen(ctx, s.pool, key)
}

// ============== List Commands ==============

func (s *Store) LPush(ctx context.Context, key string, values []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.lPush(ctx, tx, key, values)
		return err
	})
	return result, err
}

func (s *Store) RPush(ctx context.Context, key string, values []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.rPush(ctx, tx, key, values)
		return err
	})
	return result, err
}

func (s *Store) LPop(ctx context.Context, key string) (string, bool, error) {
	var value string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		value, found, err = s.ops.lPop(ctx, tx, key)
		return err
	})
	return value, found, err
}

func (s *Store) RPop(ctx context.Context, key string) (string, bool, error) {
	var value string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		value, found, err = s.ops.rPop(ctx, tx, key)
		return err
	})
	return value, found, err
}

func (s *Store) LLen(ctx context.Context, key string) (int64, error) {
	return s.ops.lLen(ctx, s.pool, key)
}

func (s *Store) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return s.ops.lRange(ctx, s.pool, key, start, stop)
}

func (s *Store) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return s.ops.lIndex(ctx, s.pool, key, index)
}

// ============== Set Commands ==============

func (s *Store) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.sAdd(ctx, tx, key, members)
		return err
	})
	return result, err
}

func (s *Store) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.ops.sRem(ctx, s.pool, key, members)
}

func (s *Store) SMembers(ctx context.Context, key string) ([]string, error) {
	return s.ops.sMembers(ctx, s.pool, key)
}

func (s *Store) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return s.ops.sIsMember(ctx, s.pool, key, member)
}

func (s *Store) SCard(ctx context.Context, key string) (int64, error) {
	return s.ops.sCard(ctx, s.pool, key)
}

// ============== Sorted Set Commands ==============

func (s *Store) ZAdd(ctx context.Context, key string, members []ZMember) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zAdd(ctx, tx, key, members)
		return err
	})
	return result, err
}

func (s *Store) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	return s.ops.zRange(ctx, s.pool, key, start, stop, withScores)
}

func (s *Store) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return s.ops.zScore(ctx, s.pool, key, member)
}

func (s *Store) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return s.ops.zRem(ctx, s.pool, key, members)
}

func (s *Store) ZCard(ctx context.Context, key string) (int64, error) {
	return s.ops.zCard(ctx, s.pool, key)
}

func (s *Store) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	return s.ops.zRangeByScore(ctx, s.pool, key, min, max, withScores, offset, count)
}

func (s *Store) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	return s.ops.zRemRangeByScore(ctx, s.pool, key, min, max)
}

func (s *Store) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return s.ops.zRemRangeByRank(ctx, s.pool, key, start, stop)
}

func (s *Store) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	var result float64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zIncrBy(ctx, tx, key, increment, member)
		return err
	})
	return result, err
}

func (s *Store) ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error) {
	var result []ZMember
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, err = s.ops.zPopMin(ctx, tx, key, count)
		return err
	})
	return result, err
}

func (s *Store) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	return s.ops.lRem(ctx, s.pool, key, count, element)
}

func (s *Store) LTrim(ctx context.Context, key string, start, stop int64) error {
	return s.ops.lTrim(ctx, s.pool, key, start, stop)
}

func (s *Store) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	var result string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var err error
		result, found, err = s.ops.rPopLPush(ctx, tx, source, destination)
		return err
	})
	return result, found, err
}

// ============== HyperLogLog Commands ==============

func (s *Store) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	return s.ops.pfAdd(ctx, s.pool, key, elements)
}

func (s *Store) PFCount(ctx context.Context, keys []string) (int64, error) {
	return s.ops.pfCount(ctx, s.pool, keys)
}

func (s *Store) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	return s.ops.pfMerge(ctx, s.pool, destKey, sourceKeys)
}

// ============== Server Commands ==============

func (s *Store) DBSize(ctx context.Context) (int64, error) {
	return s.ops.dbSize(ctx, s.pool)
}

func (s *Store) FlushDB(ctx context.Context) error {
	queries := []string{
		"TRUNCATE kv_strings",
		"TRUNCATE kv_hashes",
		"TRUNCATE kv_lists",
		"TRUNCATE kv_sets",
		"TRUNCATE kv_zsets",
		"TRUNCATE kv_hyperloglog",
		"TRUNCATE kv_meta",
	}
	for _, q := range queries {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}
