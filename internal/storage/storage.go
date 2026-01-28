// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// KeyType represents the type of a Redis key
type KeyType string

const (
	TypeString KeyType = "string"
	TypeHash   KeyType = "hash"
	TypeList   KeyType = "list"
	TypeSet    KeyType = "set"
	TypeNone   KeyType = "none"
)

// Store provides PostgreSQL-backed storage for Redis operations
type Store struct {
	pool *pgxpool.Pool
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

	store := &Store{pool: pool}
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

		-- Key metadata for tracking types and TTL
		CREATE TABLE IF NOT EXISTS kv_meta (
			key TEXT PRIMARY KEY,
			key_type TEXT NOT NULL,
			expires_at TIMESTAMPTZ
		);
		CREATE INDEX IF NOT EXISTS idx_kv_meta_expires ON kv_meta(expires_at) WHERE expires_at IS NOT NULL;
	`
	_, err := s.pool.Exec(ctx, schema)
	if err != nil {
		return err
	}

	// Run migrations for existing installations (alter TEXT columns to BYTEA)
	return s.runMigrations(ctx)
}

func (s *Store) runMigrations(ctx context.Context) error {
	// Migration: Convert TEXT columns to BYTEA for binary-safe storage
	migrations := []string{
		// kv_strings.value: TEXT -> BYTEA
		`DO $$ 
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name = 'kv_strings' AND column_name = 'value' AND data_type = 'text'
			) THEN
				ALTER TABLE kv_strings ALTER COLUMN value TYPE BYTEA USING value::bytea;
			END IF;
		END $$`,
		// kv_hashes.value: TEXT -> BYTEA
		`DO $$ 
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name = 'kv_hashes' AND column_name = 'value' AND data_type = 'text'
			) THEN
				ALTER TABLE kv_hashes ALTER COLUMN value TYPE BYTEA USING value::bytea;
			END IF;
		END $$`,
		// kv_lists.value: TEXT -> BYTEA
		`DO $$ 
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name = 'kv_lists' AND column_name = 'value' AND data_type = 'text'
			) THEN
				ALTER TABLE kv_lists ALTER COLUMN value TYPE BYTEA USING value::bytea;
			END IF;
		END $$`,
		// kv_sets.member: TEXT -> BYTEA
		`DO $$ 
		BEGIN
			IF EXISTS (
				SELECT 1 FROM information_schema.columns 
				WHERE table_name = 'kv_sets' AND column_name = 'member' AND data_type = 'text'
			) THEN
				ALTER TABLE kv_sets ALTER COLUMN member TYPE BYTEA USING member::bytea;
			END IF;
		END $$`,
	}

	for _, m := range migrations {
		if _, err := s.pool.Exec(ctx, m); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}
	return nil
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

// ============== String Commands ==============

// Get retrieves a string value by key
func (s *Store) Get(ctx context.Context, key string) (string, bool, error) {
	var value []byte
	err := s.pool.QueryRow(ctx,
		"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(value), true, nil
}

// Set stores a string value
func (s *Store) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		// Delete from other types first
		if err := s.deleteKeyFromAllTables(ctx, tx, key); err != nil {
			return err
		}

		var expiresAt *time.Time
		if ttl > 0 {
			t := time.Now().Add(ttl)
			expiresAt = &t
		}

		_, err := tx.Exec(ctx,
			`INSERT INTO kv_strings (key, value, expires_at) VALUES ($1, $2, $3)
			 ON CONFLICT (key) DO UPDATE SET value = $2, expires_at = $3`,
			key, value, expiresAt,
		)
		if err != nil {
			return err
		}

		return s.setMeta(ctx, tx, key, TypeString, expiresAt)
	})
}

// SetNX sets a key only if it doesn't exist
func (s *Store) SetNX(ctx context.Context, key, value string) (bool, error) {
	result, err := s.pool.Exec(ctx,
		`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
		 ON CONFLICT (key) DO NOTHING`,
		key, value,
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() > 0 {
		s.pool.Exec(ctx,
			`INSERT INTO kv_meta (key, key_type) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET key_type = $2`,
			key, TypeString,
		)
		return true, nil
	}
	return false, nil
}

// MGet retrieves multiple string values
func (s *Store) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	results := make([]interface{}, len(keys))

	rows, err := s.pool.Query(ctx,
		`SELECT key, value FROM kv_strings 
		 WHERE key = ANY($1) AND (expires_at IS NULL OR expires_at > NOW())`,
		keys,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keyValues := make(map[string]string)
	for rows.Next() {
		var key string
		var value []byte
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		keyValues[key] = string(value)
	}

	for i, key := range keys {
		if val, ok := keyValues[key]; ok {
			results[i] = val
		} else {
			results[i] = nil
		}
	}

	return results, nil
}

// MSet sets multiple string values
func (s *Store) MSet(ctx context.Context, pairs map[string]string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		for key, value := range pairs {
			if err := s.deleteKeyFromAllTables(ctx, tx, key); err != nil {
				return err
			}

			_, err := tx.Exec(ctx,
				`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
				 ON CONFLICT (key) DO UPDATE SET value = $2`,
				key, value,
			)
			if err != nil {
				return err
			}

			if err := s.setMeta(ctx, tx, key, TypeString, nil); err != nil {
				return err
			}
		}
		return nil
	})
}

// Incr increments a string value as integer
func (s *Store) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	var result int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var value []byte
		err := tx.QueryRow(ctx,
			"SELECT value FROM kv_strings WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
			key,
		).Scan(&value)

		var current int64
		if err == pgx.ErrNoRows {
			current = 0
		} else if err != nil {
			return err
		} else {
			current, err = strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				return fmt.Errorf("value is not an integer")
			}
		}

		result = current + delta
		_, err = tx.Exec(ctx,
			`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET value = $2`,
			key, strconv.FormatInt(result, 10),
		)
		if err != nil {
			return err
		}

		return s.setMeta(ctx, tx, key, TypeString, nil)
	})
	return result, err
}

// Append appends a value to an existing string
func (s *Store) Append(ctx context.Context, key, value string) (int64, error) {
	var length int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx,
			`INSERT INTO kv_strings (key, value) VALUES ($1, $2)
			 ON CONFLICT (key) DO UPDATE SET value = kv_strings.value || $2`,
			key, []byte(value),
		)
		if err != nil {
			return err
		}

		var newValue []byte
		err = tx.QueryRow(ctx, "SELECT value FROM kv_strings WHERE key = $1", key).Scan(&newValue)
		if err != nil {
			return err
		}
		length = int64(len(newValue))

		return s.setMeta(ctx, tx, key, TypeString, nil)
	})
	return length, err
}

// ============== Key Commands ==============

// Del deletes one or more keys
func (s *Store) Del(ctx context.Context, keys []string) (int64, error) {
	var deleted int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		for _, key := range keys {
			keyType, err := s.getKeyType(ctx, tx, key)
			if err != nil {
				return err
			}
			if keyType == TypeNone {
				continue
			}

			if err := s.deleteKeyFromAllTables(ctx, tx, key); err != nil {
				return err
			}
			deleted++
		}
		return nil
	})
	return deleted, err
}

// Exists checks if keys exist
func (s *Store) Exists(ctx context.Context, keys []string) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_meta 
		 WHERE key = ANY($1) AND (expires_at IS NULL OR expires_at > NOW())`,
		keys,
	).Scan(&count)
	return count, err
}

// Expire sets a TTL on a key
func (s *Store) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	expiresAt := time.Now().Add(ttl)

	result, err := s.pool.Exec(ctx,
		`UPDATE kv_meta SET expires_at = $2 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, expiresAt,
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() == 0 {
		return false, nil
	}

	// Update expires_at in the data table
	keyType, err := s.Type(ctx, key)
	if err != nil {
		return false, err
	}

	var table string
	switch keyType {
	case TypeString:
		table = "kv_strings"
	case TypeHash:
		table = "kv_hashes"
	case TypeList:
		table = "kv_lists"
	case TypeSet:
		table = "kv_sets"
	default:
		return false, nil
	}

	_, err = s.pool.Exec(ctx,
		fmt.Sprintf("UPDATE %s SET expires_at = $2 WHERE key = $1", table),
		key, expiresAt,
	)
	return err == nil, err
}

// TTL returns the remaining TTL of a key in seconds
func (s *Store) TTL(ctx context.Context, key string) (int64, error) {
	var expiresAt *time.Time
	err := s.pool.QueryRow(ctx,
		"SELECT expires_at FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&expiresAt)

	if err == pgx.ErrNoRows {
		return -2, nil // Key doesn't exist
	}
	if err != nil {
		return 0, err
	}
	if expiresAt == nil {
		return -1, nil // No TTL
	}

	ttl := time.Until(*expiresAt).Seconds()
	if ttl < 0 {
		return -2, nil
	}
	return int64(ttl), nil
}

// PTTL returns the remaining TTL of a key in milliseconds
func (s *Store) PTTL(ctx context.Context, key string) (int64, error) {
	var expiresAt *time.Time
	err := s.pool.QueryRow(ctx,
		"SELECT expires_at FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&expiresAt)

	if err == pgx.ErrNoRows {
		return -2, nil
	}
	if err != nil {
		return 0, err
	}
	if expiresAt == nil {
		return -1, nil
	}

	ttl := time.Until(*expiresAt).Milliseconds()
	if ttl < 0 {
		return -2, nil
	}
	return ttl, nil
}

// Persist removes the TTL from a key
func (s *Store) Persist(ctx context.Context, key string) (bool, error) {
	result, err := s.pool.Exec(ctx,
		"UPDATE kv_meta SET expires_at = NULL WHERE key = $1 AND expires_at IS NOT NULL",
		key,
	)
	if err != nil {
		return false, err
	}

	if result.RowsAffected() == 0 {
		return false, nil
	}

	// Also clear expires_at in data tables
	queries := []string{
		"UPDATE kv_strings SET expires_at = NULL WHERE key = $1",
		"UPDATE kv_hashes SET expires_at = NULL WHERE key = $1",
		"UPDATE kv_lists SET expires_at = NULL WHERE key = $1",
		"UPDATE kv_sets SET expires_at = NULL WHERE key = $1",
	}
	for _, q := range queries {
		s.pool.Exec(ctx, q, key)
	}

	return true, nil
}

// Keys returns all keys matching a pattern
func (s *Store) Keys(ctx context.Context, pattern string) ([]string, error) {
	// Convert Redis glob pattern to SQL LIKE pattern
	sqlPattern := strings.ReplaceAll(pattern, "*", "%")
	sqlPattern = strings.ReplaceAll(sqlPattern, "?", "_")

	rows, err := s.pool.Query(ctx,
		`SELECT key FROM kv_meta 
		 WHERE key LIKE $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		sqlPattern,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	return keys, nil
}

// Type returns the type of a key
func (s *Store) Type(ctx context.Context, key string) (KeyType, error) {
	var keyType string
	err := s.pool.QueryRow(ctx,
		"SELECT key_type FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&keyType)

	if err == pgx.ErrNoRows {
		return TypeNone, nil
	}
	if err != nil {
		return TypeNone, err
	}
	return KeyType(keyType), nil
}

// Rename renames a key
func (s *Store) Rename(ctx context.Context, oldKey, newKey string) error {
	return s.withTx(ctx, func(tx pgx.Tx) error {
		keyType, err := s.getKeyType(ctx, tx, oldKey)
		if err != nil {
			return err
		}
		if keyType == TypeNone {
			return fmt.Errorf("no such key")
		}

		// Delete newKey if exists
		if err := s.deleteKeyFromAllTables(ctx, tx, newKey); err != nil {
			return err
		}

		// Rename in appropriate table
		var table string
		switch keyType {
		case TypeString:
			table = "kv_strings"
		case TypeHash:
			table = "kv_hashes"
		case TypeList:
			table = "kv_lists"
		case TypeSet:
			table = "kv_sets"
		}

		_, err = tx.Exec(ctx,
			fmt.Sprintf("UPDATE %s SET key = $2 WHERE key = $1", table),
			oldKey, newKey,
		)
		if err != nil {
			return err
		}

		_, err = tx.Exec(ctx,
			"UPDATE kv_meta SET key = $2 WHERE key = $1",
			oldKey, newKey,
		)
		return err
	})
}

// ============== Hash Commands ==============

// HGet gets a hash field value
func (s *Store) HGet(ctx context.Context, key, field string) (string, bool, error) {
	var value []byte
	err := s.pool.QueryRow(ctx,
		`SELECT value FROM kv_hashes 
		 WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW())`,
		key, field,
	).Scan(&value)

	if err == pgx.ErrNoRows {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(value), true, nil
}

// HSet sets hash fields
func (s *Store) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	var created int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		// Check type
		keyType, err := s.getKeyType(ctx, tx, key)
		if err != nil {
			return err
		}
		if keyType != TypeNone && keyType != TypeHash {
			return fmt.Errorf("WRONGTYPE")
		}

		for field, value := range fields {
			result, err := tx.Exec(ctx,
				`INSERT INTO kv_hashes (key, field, value) VALUES ($1, $2, $3)
				 ON CONFLICT (key, field) DO UPDATE SET value = $3`,
				key, field, value,
			)
			if err != nil {
				return err
			}
			// Count only truly new fields
			if result.RowsAffected() > 0 {
				var exists bool
				tx.QueryRow(ctx,
					"SELECT EXISTS(SELECT 1 FROM kv_hashes WHERE key = $1 AND field = $2)",
					key, field,
				).Scan(&exists)
				if !exists {
					created++
				}
			}
		}

		return s.setMeta(ctx, tx, key, TypeHash, nil)
	})
	// Simplified: return number of fields set
	return int64(len(fields)), err
}

// HDel deletes hash fields
func (s *Store) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	result, err := s.pool.Exec(ctx,
		"DELETE FROM kv_hashes WHERE key = $1 AND field = ANY($2)",
		key, fields,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// HGetAll gets all fields and values from a hash
func (s *Store) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT field, value FROM kv_hashes 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]string)
	for rows.Next() {
		var field string
		var value []byte
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		result[field] = string(value)
	}
	return result, nil
}

// HMGet gets multiple hash field values
func (s *Store) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT field, value FROM kv_hashes 
		 WHERE key = $1 AND field = ANY($2) AND (expires_at IS NULL OR expires_at > NOW())`,
		key, fields,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fieldValues := make(map[string]string)
	for rows.Next() {
		var field string
		var value []byte
		if err := rows.Scan(&field, &value); err != nil {
			return nil, err
		}
		fieldValues[field] = string(value)
	}

	results := make([]interface{}, len(fields))
	for i, field := range fields {
		if val, ok := fieldValues[field]; ok {
			results[i] = val
		} else {
			results[i] = nil
		}
	}
	return results, nil
}

// HExists checks if a hash field exists
func (s *Store) HExists(ctx context.Context, key, field string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM kv_hashes 
		 WHERE key = $1 AND field = $2 AND (expires_at IS NULL OR expires_at > NOW()))`,
		key, field,
	).Scan(&exists)
	return exists, err
}

// HKeys gets all field names in a hash
func (s *Store) HKeys(ctx context.Context, key string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT field FROM kv_hashes 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var fields []string
	for rows.Next() {
		var field string
		if err := rows.Scan(&field); err != nil {
			return nil, err
		}
		fields = append(fields, field)
	}
	return fields, nil
}

// HVals gets all values in a hash
func (s *Store) HVals(ctx context.Context, key string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT value FROM kv_hashes 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		values = append(values, string(value))
	}
	return values, nil
}

// HLen returns the number of fields in a hash
func (s *Store) HLen(ctx context.Context, key string) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_hashes 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	).Scan(&count)
	return count, err
}

// ============== List Commands ==============

// LPush prepends values to a list
func (s *Store) LPush(ctx context.Context, key string, values []string) (int64, error) {
	var length int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		keyType, err := s.getKeyType(ctx, tx, key)
		if err != nil {
			return err
		}
		if keyType != TypeNone && keyType != TypeList {
			return fmt.Errorf("WRONGTYPE")
		}

		// Get current minimum index
		var minIdx int64
		err = tx.QueryRow(ctx,
			"SELECT COALESCE(MIN(idx), 0) FROM kv_lists WHERE key = $1",
			key,
		).Scan(&minIdx)
		if err != nil {
			return err
		}

		// Insert values at the beginning
		for i, value := range values {
			_, err := tx.Exec(ctx,
				"INSERT INTO kv_lists (key, idx, value) VALUES ($1, $2, $3)",
				key, minIdx-int64(i+1), value,
			)
			if err != nil {
				return err
			}
		}

		// Get new length
		err = tx.QueryRow(ctx,
			"SELECT COUNT(*) FROM kv_lists WHERE key = $1",
			key,
		).Scan(&length)
		if err != nil {
			return err
		}

		return s.setMeta(ctx, tx, key, TypeList, nil)
	})
	return length, err
}

// RPush appends values to a list
func (s *Store) RPush(ctx context.Context, key string, values []string) (int64, error) {
	var length int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		keyType, err := s.getKeyType(ctx, tx, key)
		if err != nil {
			return err
		}
		if keyType != TypeNone && keyType != TypeList {
			return fmt.Errorf("WRONGTYPE")
		}

		// Get current maximum index
		var maxIdx int64
		err = tx.QueryRow(ctx,
			"SELECT COALESCE(MAX(idx), -1) FROM kv_lists WHERE key = $1",
			key,
		).Scan(&maxIdx)
		if err != nil {
			return err
		}

		// Insert values at the end
		for i, value := range values {
			_, err := tx.Exec(ctx,
				"INSERT INTO kv_lists (key, idx, value) VALUES ($1, $2, $3)",
				key, maxIdx+int64(i+1), value,
			)
			if err != nil {
				return err
			}
		}

		// Get new length
		err = tx.QueryRow(ctx,
			"SELECT COUNT(*) FROM kv_lists WHERE key = $1",
			key,
		).Scan(&length)
		if err != nil {
			return err
		}

		return s.setMeta(ctx, tx, key, TypeList, nil)
	})
	return length, err
}

// LPop removes and returns the first element of a list
func (s *Store) LPop(ctx context.Context, key string) (string, bool, error) {
	var value string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var idx int64
		err := tx.QueryRow(ctx,
			`SELECT idx, value FROM kv_lists 
			 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY idx ASC LIMIT 1`,
			key,
		).Scan(&idx, &value)

		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}

		found = true
		_, err = tx.Exec(ctx, "DELETE FROM kv_lists WHERE key = $1 AND idx = $2", key, idx)
		return err
	})
	return value, found, err
}

// RPop removes and returns the last element of a list
func (s *Store) RPop(ctx context.Context, key string) (string, bool, error) {
	var value string
	var found bool
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var idx int64
		err := tx.QueryRow(ctx,
			`SELECT idx, value FROM kv_lists 
			 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
			 ORDER BY idx DESC LIMIT 1`,
			key,
		).Scan(&idx, &value)

		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return err
		}

		found = true
		_, err = tx.Exec(ctx, "DELETE FROM kv_lists WHERE key = $1 AND idx = $2", key, idx)
		return err
	})
	return value, found, err
}

// LLen returns the length of a list
func (s *Store) LLen(ctx context.Context, key string) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_lists 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	).Scan(&count)
	return count, err
}

// LRange returns a range of elements from a list
func (s *Store) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	// Get all elements ordered by index
	rows, err := s.pool.Query(ctx,
		`SELECT value FROM kv_lists 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())
		 ORDER BY idx ASC`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var allValues []string
	for rows.Next() {
		var value []byte
		if err := rows.Scan(&value); err != nil {
			return nil, err
		}
		allValues = append(allValues, string(value))
	}

	length := int64(len(allValues))
	if length == 0 {
		return []string{}, nil
	}

	// Handle negative indices
	if start < 0 {
		start = length + start
	}
	if stop < 0 {
		stop = length + stop
	}

	// Clamp values
	if start < 0 {
		start = 0
	}
	if stop >= length {
		stop = length - 1
	}
	if start > stop || start >= length {
		return []string{}, nil
	}

	return allValues[start : stop+1], nil
}

// LIndex returns an element by index
func (s *Store) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	values, err := s.LRange(ctx, key, index, index)
	if err != nil {
		return "", false, err
	}
	if len(values) == 0 {
		return "", false, nil
	}
	return values[0], true, nil
}

// ============== Set Commands ==============

// SAdd adds members to a set
func (s *Store) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	var added int64
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		keyType, err := s.getKeyType(ctx, tx, key)
		if err != nil {
			return err
		}
		if keyType != TypeNone && keyType != TypeSet {
			return fmt.Errorf("WRONGTYPE")
		}

		for _, member := range members {
			result, err := tx.Exec(ctx,
				`INSERT INTO kv_sets (key, member) VALUES ($1, $2)
				 ON CONFLICT (key, member) DO NOTHING`,
				key, member,
			)
			if err != nil {
				return err
			}
			added += result.RowsAffected()
		}

		return s.setMeta(ctx, tx, key, TypeSet, nil)
	})
	return added, err
}

// SRem removes members from a set
func (s *Store) SRem(ctx context.Context, key string, members []string) (int64, error) {
	result, err := s.pool.Exec(ctx,
		"DELETE FROM kv_sets WHERE key = $1 AND member = ANY($2)",
		key, members,
	)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected(), nil
}

// SMembers returns all members of a set
func (s *Store) SMembers(ctx context.Context, key string) ([]string, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT member FROM kv_sets 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var members []string
	for rows.Next() {
		var member []byte
		if err := rows.Scan(&member); err != nil {
			return nil, err
		}
		members = append(members, string(member))
	}
	return members, nil
}

// SIsMember checks if a member exists in a set
func (s *Store) SIsMember(ctx context.Context, key, member string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM kv_sets 
		 WHERE key = $1 AND member = $2 AND (expires_at IS NULL OR expires_at > NOW()))`,
		key, member,
	).Scan(&exists)
	return exists, err
}

// SCard returns the number of members in a set
func (s *Store) SCard(ctx context.Context, key string) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM kv_sets 
		 WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())`,
		key,
	).Scan(&count)
	return count, err
}

// ============== Server Commands ==============

// DBSize returns the number of keys
func (s *Store) DBSize(ctx context.Context) (int64, error) {
	var count int64
	err := s.pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM kv_meta WHERE expires_at IS NULL OR expires_at > NOW()",
	).Scan(&count)
	return count, err
}

// FlushDB deletes all keys
func (s *Store) FlushDB(ctx context.Context) error {
	queries := []string{
		"TRUNCATE kv_strings",
		"TRUNCATE kv_hashes",
		"TRUNCATE kv_lists",
		"TRUNCATE kv_sets",
		"TRUNCATE kv_meta",
	}
	for _, q := range queries {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// ============== Helper Methods ==============

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

func (s *Store) getKeyType(ctx context.Context, tx pgx.Tx, key string) (KeyType, error) {
	var keyType string
	err := tx.QueryRow(ctx,
		"SELECT key_type FROM kv_meta WHERE key = $1 AND (expires_at IS NULL OR expires_at > NOW())",
		key,
	).Scan(&keyType)

	if err == pgx.ErrNoRows {
		return TypeNone, nil
	}
	if err != nil {
		return TypeNone, err
	}
	return KeyType(keyType), nil
}

func (s *Store) setMeta(ctx context.Context, tx pgx.Tx, key string, keyType KeyType, expiresAt *time.Time) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO kv_meta (key, key_type, expires_at) VALUES ($1, $2, $3)
		 ON CONFLICT (key) DO UPDATE SET key_type = $2, expires_at = $3`,
		key, string(keyType), expiresAt,
	)
	return err
}

func (s *Store) deleteKeyFromAllTables(ctx context.Context, tx pgx.Tx, key string) error {
	queries := []string{
		"DELETE FROM kv_strings WHERE key = $1",
		"DELETE FROM kv_hashes WHERE key = $1",
		"DELETE FROM kv_lists WHERE key = $1",
		"DELETE FROM kv_sets WHERE key = $1",
		"DELETE FROM kv_meta WHERE key = $1",
	}
	for _, q := range queries {
		if _, err := tx.Exec(ctx, q, key); err != nil {
			return err
		}
	}
	return nil
}
