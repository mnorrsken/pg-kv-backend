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

func (t *MockTransaction) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return t.parent.GetRange(ctx, key, start, end)
}

func (t *MockTransaction) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return t.parent.SetRange(ctx, key, offset, value)
}

func (t *MockTransaction) BitField(ctx context.Context, key string, ops []BitFieldOp) ([]int64, error) {
	return t.parent.BitField(ctx, key, ops)
}

func (t *MockTransaction) StrLen(ctx context.Context, key string) (int64, error) {
	return t.parent.StrLen(ctx, key)
}

func (t *MockTransaction) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	return t.parent.GetEx(ctx, key, ttl, persist)
}

func (t *MockTransaction) GetDel(ctx context.Context, key string) (string, bool, error) {
	return t.parent.GetDel(ctx, key)
}

func (t *MockTransaction) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	return t.parent.GetSet(ctx, key, value)
}

func (t *MockTransaction) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	return t.parent.IncrByFloat(ctx, key, delta)
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

func (t *MockTransaction) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	return t.parent.HIncrBy(ctx, key, field, increment)
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

func (t *MockTransaction) ZAdd(ctx context.Context, key string, members []ZMember) (int64, error) {
	return t.parent.ZAdd(ctx, key, members)
}

func (t *MockTransaction) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	return t.parent.ZRange(ctx, key, start, stop, withScores)
}

func (t *MockTransaction) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return t.parent.ZScore(ctx, key, member)
}

func (t *MockTransaction) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return t.parent.ZRem(ctx, key, members)
}

func (t *MockTransaction) ZCard(ctx context.Context, key string) (int64, error) {
	return t.parent.ZCard(ctx, key)
}

func (t *MockTransaction) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	return t.parent.ZRangeByScore(ctx, key, min, max, withScores, offset, count)
}

func (t *MockTransaction) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	return t.parent.ZRemRangeByScore(ctx, key, min, max)
}

func (t *MockTransaction) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return t.parent.ZRemRangeByRank(ctx, key, start, stop)
}

func (t *MockTransaction) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	return t.parent.ZIncrBy(ctx, key, increment, member)
}

func (t *MockTransaction) ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error) {
	return t.parent.ZPopMin(ctx, key, count)
}

func (t *MockTransaction) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	return t.parent.LRem(ctx, key, count, element)
}

func (t *MockTransaction) LTrim(ctx context.Context, key string, start, stop int64) error {
	return t.parent.LTrim(ctx, key, start, stop)
}

func (t *MockTransaction) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	return t.parent.RPopLPush(ctx, source, destination)
}

func (t *MockTransaction) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	return t.parent.PFAdd(ctx, key, elements)
}

func (t *MockTransaction) PFCount(ctx context.Context, keys []string) (int64, error) {
	return t.parent.PFCount(ctx, keys)
}

func (t *MockTransaction) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	return t.parent.PFMerge(ctx, destKey, sourceKeys)
}

func (t *MockTransaction) DBSize(ctx context.Context) (int64, error) {
	return t.parent.DBSize(ctx)
}

// Ensure MockTransaction implements Transaction
var _ Transaction = (*MockTransaction)(nil)
