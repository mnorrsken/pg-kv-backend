package storage

import (
	"context"
	"fmt"
	"time"
)

// MockTransaction implements Transaction for in-memory storage by delegating to MockStore.
// For simplicity, this implementation operates directly on the parent store.
// A more complete implementation would snapshot data and apply changes on commit.
type MockTransaction struct {
	parent     *MockStore
	committed  bool
	rolledBack bool
}

// BeginTx starts a new mock transaction
func (m *MockStore) BeginTx(ctx context.Context) (Transaction, error) {
	return &MockTransaction{
		parent: m,
	}, nil
}

// Commit commits the transaction (no-op for mock as changes are applied immediately)
func (t *MockTransaction) Commit(ctx context.Context) error {
	if t.committed || t.rolledBack {
		return fmt.Errorf("transaction already completed")
	}
	t.committed = true
	return nil
}

// Rollback rolls back the transaction
// Note: For simplicity, mock transactions don't support true rollback
func (t *MockTransaction) Rollback(ctx context.Context) error {
	if t.committed || t.rolledBack {
		return nil
	}
	t.rolledBack = true
	return nil
}

// ============== Delegate all Operations to parent MockStore ==============

func (t *MockTransaction) Get(ctx context.Context, key string) (string, bool, error) {
	return t.parent.Get(ctx, key)
}

func (t *MockTransaction) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return t.parent.Set(ctx, key, value, ttl)
}

func (t *MockTransaction) SetNX(ctx context.Context, key, value string) (bool, error) {
	return t.parent.SetNX(ctx, key, value)
}

func (t *MockTransaction) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	return t.parent.MGet(ctx, keys)
}

func (t *MockTransaction) MSet(ctx context.Context, pairs map[string]string) error {
	return t.parent.MSet(ctx, pairs)
}

func (t *MockTransaction) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return t.parent.Incr(ctx, key, delta)
}

func (t *MockTransaction) Append(ctx context.Context, key, value string) (int64, error) {
	return t.parent.Append(ctx, key, value)
}

func (t *MockTransaction) Del(ctx context.Context, keys []string) (int64, error) {
	return t.parent.Del(ctx, keys)
}

func (t *MockTransaction) Exists(ctx context.Context, keys []string) (int64, error) {
	return t.parent.Exists(ctx, keys)
}

func (t *MockTransaction) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return t.parent.Expire(ctx, key, ttl)
}

func (t *MockTransaction) TTL(ctx context.Context, key string) (int64, error) {
	return t.parent.TTL(ctx, key)
}

func (t *MockTransaction) PTTL(ctx context.Context, key string) (int64, error) {
	return t.parent.PTTL(ctx, key)
}

func (t *MockTransaction) Persist(ctx context.Context, key string) (bool, error) {
	return t.parent.Persist(ctx, key)
}

func (t *MockTransaction) Keys(ctx context.Context, pattern string) ([]string, error) {
	return t.parent.Keys(ctx, pattern)
}

func (t *MockTransaction) Type(ctx context.Context, key string) (KeyType, error) {
	return t.parent.Type(ctx, key)
}

func (t *MockTransaction) Rename(ctx context.Context, oldKey, newKey string) error {
	return t.parent.Rename(ctx, oldKey, newKey)
}

func (t *MockTransaction) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return t.parent.HGet(ctx, key, field)
}

func (t *MockTransaction) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	return t.parent.HSet(ctx, key, fields)
}

func (t *MockTransaction) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return t.parent.HDel(ctx, key, fields)
}

func (t *MockTransaction) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return t.parent.HGetAll(ctx, key)
}

func (t *MockTransaction) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return t.parent.HMGet(ctx, key, fields)
}

func (t *MockTransaction) HExists(ctx context.Context, key, field string) (bool, error) {
	return t.parent.HExists(ctx, key, field)
}

func (t *MockTransaction) HKeys(ctx context.Context, key string) ([]string, error) {
	return t.parent.HKeys(ctx, key)
}

func (t *MockTransaction) HVals(ctx context.Context, key string) ([]string, error) {
	return t.parent.HVals(ctx, key)
}

func (t *MockTransaction) HLen(ctx context.Context, key string) (int64, error) {
	return t.parent.HLen(ctx, key)
}

func (t *MockTransaction) LPush(ctx context.Context, key string, values []string) (int64, error) {
	return t.parent.LPush(ctx, key, values)
}

func (t *MockTransaction) RPush(ctx context.Context, key string, values []string) (int64, error) {
	return t.parent.RPush(ctx, key, values)
}

func (t *MockTransaction) LPop(ctx context.Context, key string) (string, bool, error) {
	return t.parent.LPop(ctx, key)
}

func (t *MockTransaction) RPop(ctx context.Context, key string) (string, bool, error) {
	return t.parent.RPop(ctx, key)
}

func (t *MockTransaction) LLen(ctx context.Context, key string) (int64, error) {
	return t.parent.LLen(ctx, key)
}

func (t *MockTransaction) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return t.parent.LRange(ctx, key, start, stop)
}

func (t *MockTransaction) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return t.parent.LIndex(ctx, key, index)
}

func (t *MockTransaction) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	return t.parent.SAdd(ctx, key, members)
}

func (t *MockTransaction) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return t.parent.SRem(ctx, key, members)
}

func (t *MockTransaction) SMembers(ctx context.Context, key string) ([]string, error) {
	return t.parent.SMembers(ctx, key)
}

func (t *MockTransaction) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return t.parent.SIsMember(ctx, key, member)
}

func (t *MockTransaction) SCard(ctx context.Context, key string) (int64, error) {
	return t.parent.SCard(ctx, key)
}

func (t *MockTransaction) DBSize(ctx context.Context) (int64, error) {
	return t.parent.DBSize(ctx)
}

// Ensure MockTransaction implements Transaction
var _ Transaction = (*MockTransaction)(nil)
