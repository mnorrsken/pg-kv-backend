// Package storage provides PostgreSQL-backed storage for Redis data types.
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// TxStore wraps a PostgreSQL transaction and implements the Transaction interface
type TxStore struct {
	tx       pgx.Tx
	ops      queryOps
	done     bool
	sqlTrace bool
}

// querier returns a Querier for the transaction, optionally wrapped with tracing
func (t *TxStore) querier() Querier {
	if t.sqlTrace {
		return NewTracingQuerier(t.tx)
	}
	return t.tx
}

// Commit commits the transaction
func (t *TxStore) Commit(ctx context.Context) error {
	if t.done {
		return fmt.Errorf("transaction already completed")
	}
	t.done = true
	return t.tx.Commit(ctx)
}

// Rollback aborts the transaction
func (t *TxStore) Rollback(ctx context.Context) error {
	if t.done {
		return nil // Already done, no-op
	}
	t.done = true
	return t.tx.Rollback(ctx)
}

// ============== String Commands ==============

func (t *TxStore) Get(ctx context.Context, key string) (string, bool, error) {
	return t.ops.get(ctx, t.querier(), key)
}

func (t *TxStore) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return t.ops.set(ctx, t.querier(), key, value, ttl)
}

func (t *TxStore) SetNX(ctx context.Context, key, value string) (bool, error) {
	return t.ops.setNX(ctx, t.querier(), key, value)
}

func (t *TxStore) MGet(ctx context.Context, keys []string) ([]interface{}, error) {
	return t.ops.mGet(ctx, t.querier(), keys)
}

func (t *TxStore) MSet(ctx context.Context, pairs map[string]string) error {
	return t.ops.mSet(ctx, t.querier(), pairs)
}

func (t *TxStore) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	return t.ops.incr(ctx, t.querier(), key, delta)
}

func (t *TxStore) Append(ctx context.Context, key, value string) (int64, error) {
	return t.ops.appendStr(ctx, t.querier(), key, value)
}

func (t *TxStore) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return t.ops.getRange(ctx, t.querier(), key, start, end)
}

func (t *TxStore) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	return t.ops.setRange(ctx, t.querier(), key, offset, value)
}

func (t *TxStore) BitField(ctx context.Context, key string, ops []BitFieldOp) ([]int64, error) {
	return t.ops.bitField(ctx, t.querier(), key, ops)
}

func (t *TxStore) StrLen(ctx context.Context, key string) (int64, error) {
	return t.ops.strLen(ctx, t.querier(), key)
}

func (t *TxStore) GetEx(ctx context.Context, key string, ttl time.Duration, persist bool) (string, bool, error) {
	return t.ops.getEx(ctx, t.querier(), key, ttl, persist)
}

func (t *TxStore) GetDel(ctx context.Context, key string) (string, bool, error) {
	return t.ops.getDel(ctx, t.querier(), key)
}

func (t *TxStore) GetSet(ctx context.Context, key, value string) (string, bool, error) {
	return t.ops.getSet(ctx, t.querier(), key, value)
}

func (t *TxStore) IncrByFloat(ctx context.Context, key string, delta float64) (float64, error) {
	return t.ops.incrByFloat(ctx, t.querier(), key, delta)
}

// ============== Key Commands ==============

func (t *TxStore) Del(ctx context.Context, keys []string) (int64, error) {
	return t.ops.del(ctx, t.querier(), keys)
}

func (t *TxStore) Exists(ctx context.Context, keys []string) (int64, error) {
	return t.ops.exists(ctx, t.querier(), keys)
}

func (t *TxStore) Expire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return t.ops.expire(ctx, t.querier(), key, ttl)
}

func (t *TxStore) TTL(ctx context.Context, key string) (int64, error) {
	return t.ops.ttl(ctx, t.querier(), key)
}

func (t *TxStore) PTTL(ctx context.Context, key string) (int64, error) {
	return t.ops.pttl(ctx, t.querier(), key)
}

func (t *TxStore) Persist(ctx context.Context, key string) (bool, error) {
	return t.ops.persist(ctx, t.querier(), key)
}

func (t *TxStore) Keys(ctx context.Context, pattern string) ([]string, error) {
	return t.ops.keys(ctx, t.querier(), pattern)
}

func (t *TxStore) Type(ctx context.Context, key string) (KeyType, error) {
	return t.ops.keyType(ctx, t.querier(), key)
}

func (t *TxStore) Rename(ctx context.Context, oldKey, newKey string) error {
	return t.ops.rename(ctx, t.querier(), oldKey, newKey)
}

// ============== Hash Commands ==============

func (t *TxStore) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return t.ops.hGet(ctx, t.querier(), key, field)
}

func (t *TxStore) HSet(ctx context.Context, key string, fields map[string]string) (int64, error) {
	return t.ops.hSet(ctx, t.querier(), key, fields)
}

func (t *TxStore) HDel(ctx context.Context, key string, fields []string) (int64, error) {
	return t.ops.hDel(ctx, t.querier(), key, fields)
}

func (t *TxStore) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return t.ops.hGetAll(ctx, t.querier(), key)
}

func (t *TxStore) HMGet(ctx context.Context, key string, fields []string) ([]interface{}, error) {
	return t.ops.hMGet(ctx, t.querier(), key, fields)
}

func (t *TxStore) HExists(ctx context.Context, key, field string) (bool, error) {
	return t.ops.hExists(ctx, t.querier(), key, field)
}

func (t *TxStore) HKeys(ctx context.Context, key string) ([]string, error) {
	return t.ops.hKeys(ctx, t.querier(), key)
}

func (t *TxStore) HVals(ctx context.Context, key string) ([]string, error) {
	return t.ops.hVals(ctx, t.querier(), key)
}

func (t *TxStore) HLen(ctx context.Context, key string) (int64, error) {
	return t.ops.hLen(ctx, t.querier(), key)
}

func (t *TxStore) HIncrBy(ctx context.Context, key, field string, increment int64) (int64, error) {
	return t.ops.hIncrBy(ctx, t.querier(), key, field, increment)
}

// ============== List Commands ==============

func (t *TxStore) LPush(ctx context.Context, key string, values []string) (int64, error) {
	return t.ops.lPush(ctx, t.querier(), key, values)
}

func (t *TxStore) RPush(ctx context.Context, key string, values []string) (int64, error) {
	return t.ops.rPush(ctx, t.querier(), key, values)
}

func (t *TxStore) LPop(ctx context.Context, key string) (string, bool, error) {
	return t.ops.lPop(ctx, t.querier(), key)
}

func (t *TxStore) RPop(ctx context.Context, key string) (string, bool, error) {
	return t.ops.rPop(ctx, t.querier(), key)
}

func (t *TxStore) LLen(ctx context.Context, key string) (int64, error) {
	return t.ops.lLen(ctx, t.querier(), key)
}

func (t *TxStore) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return t.ops.lRange(ctx, t.querier(), key, start, stop)
}

func (t *TxStore) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return t.ops.lIndex(ctx, t.querier(), key, index)
}

// ============== Set Commands ==============

func (t *TxStore) SAdd(ctx context.Context, key string, members []string) (int64, error) {
	return t.ops.sAdd(ctx, t.querier(), key, members)
}

func (t *TxStore) SRem(ctx context.Context, key string, members []string) (int64, error) {
	return t.ops.sRem(ctx, t.querier(), key, members)
}

func (t *TxStore) SMembers(ctx context.Context, key string) ([]string, error) {
	return t.ops.sMembers(ctx, t.querier(), key)
}

func (t *TxStore) SIsMember(ctx context.Context, key, member string) (bool, error) {
	return t.ops.sIsMember(ctx, t.querier(), key, member)
}

func (t *TxStore) SCard(ctx context.Context, key string) (int64, error) {
	return t.ops.sCard(ctx, t.querier(), key)
}

// ============== Sorted Set Commands ==============

func (t *TxStore) ZAdd(ctx context.Context, key string, members []ZMember) (int64, error) {
	return t.ops.zAdd(ctx, t.querier(), key, members)
}

func (t *TxStore) ZRange(ctx context.Context, key string, start, stop int64, withScores bool) ([]ZMember, error) {
	return t.ops.zRange(ctx, t.querier(), key, start, stop, withScores)
}

func (t *TxStore) ZScore(ctx context.Context, key, member string) (float64, bool, error) {
	return t.ops.zScore(ctx, t.querier(), key, member)
}

func (t *TxStore) ZRem(ctx context.Context, key string, members []string) (int64, error) {
	return t.ops.zRem(ctx, t.querier(), key, members)
}

func (t *TxStore) ZCard(ctx context.Context, key string) (int64, error) {
	return t.ops.zCard(ctx, t.querier(), key)
}

func (t *TxStore) ZRangeByScore(ctx context.Context, key string, min, max float64, withScores bool, offset, count int64) ([]ZMember, error) {
	return t.ops.zRangeByScore(ctx, t.querier(), key, min, max, withScores, offset, count)
}

func (t *TxStore) ZRemRangeByScore(ctx context.Context, key string, min, max float64) (int64, error) {
	return t.ops.zRemRangeByScore(ctx, t.querier(), key, min, max)
}

func (t *TxStore) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) (int64, error) {
	return t.ops.zRemRangeByRank(ctx, t.querier(), key, start, stop)
}

func (t *TxStore) ZIncrBy(ctx context.Context, key string, increment float64, member string) (float64, error) {
	return t.ops.zIncrBy(ctx, t.querier(), key, increment, member)
}

func (t *TxStore) ZPopMin(ctx context.Context, key string, count int64) ([]ZMember, error) {
	return t.ops.zPopMin(ctx, t.querier(), key, count)
}

func (t *TxStore) LRem(ctx context.Context, key string, count int64, element string) (int64, error) {
	return t.ops.lRem(ctx, t.querier(), key, count, element)
}

func (t *TxStore) LTrim(ctx context.Context, key string, start, stop int64) error {
	return t.ops.lTrim(ctx, t.querier(), key, start, stop)
}

func (t *TxStore) RPopLPush(ctx context.Context, source, destination string) (string, bool, error) {
	return t.ops.rPopLPush(ctx, t.querier(), source, destination)
}

// ============== HyperLogLog Commands ==============

func (t *TxStore) PFAdd(ctx context.Context, key string, elements []string) (int64, error) {
	return t.ops.pfAdd(ctx, t.querier(), key, elements)
}

func (t *TxStore) PFCount(ctx context.Context, keys []string) (int64, error) {
	return t.ops.pfCount(ctx, t.querier(), keys)
}

func (t *TxStore) PFMerge(ctx context.Context, destKey string, sourceKeys []string) error {
	return t.ops.pfMerge(ctx, t.querier(), destKey, sourceKeys)
}

// ============== Server Commands ==============

func (t *TxStore) DBSize(ctx context.Context) (int64, error) {
	return t.ops.dbSize(ctx, t.tx)
}

// Ensure TxStore implements Transaction
var _ Transaction = (*TxStore)(nil)
