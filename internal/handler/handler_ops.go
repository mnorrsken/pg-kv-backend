// Package handler implements Redis command handlers.
// This file contains the unified command handlers that work with storage.Operations.
package handler

import (
	"context"
	"fmt"
	"log"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/mnorrsken/postkeys/internal/resp"
	"github.com/mnorrsken/postkeys/internal/storage"
)

// ============== Unified Command Handlers ==============
// These handlers work with storage.Operations interface, which is implemented
// by both Backend (h.store) and Transaction (tx). This eliminates duplication
// between regular and transaction command handlers.

// ============== String Commands ==============

func (h *Handler) getOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("get")
	}

	value, found, err := ops.Get(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) setOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("set")
	}

	key := args[0].Bulk
	value := args[1].Bulk
	var ttl time.Duration

	// Parse options (EX, PX, NX, XX, etc.)
	for i := 2; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "EX":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			secs, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Duration(secs) * time.Second
		case "PX":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			ms, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Duration(ms) * time.Millisecond
		case "EXAT":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			ts, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Until(time.Unix(ts, 0))
		case "PXAT":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			ts, err := strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer")
			}
			ttl = time.Until(time.UnixMilli(ts))
		case "NX":
			// Set only if not exists
			ok, err := ops.SetNX(ctx, key, value)
			if err != nil {
				return resp.Err(err.Error())
			}
			if !ok {
				return resp.NullBulk()
			}
			return resp.OK()
		case "XX":
			// Set only if exists
			_, found, err := ops.Get(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if !found {
				return resp.NullBulk()
			}
		case "KEEPTTL":
			// Keep existing TTL
			currentTTL, err := ops.TTL(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if currentTTL > 0 {
				ttl = time.Duration(currentTTL) * time.Second
			}
		case "GET":
			// Return old value
			oldValue, found, err := ops.Get(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if err := ops.Set(ctx, key, value, ttl); err != nil {
				return resp.Err(err.Error())
			}
			if !found {
				return resp.NullBulk()
			}
			return resp.Bulk(oldValue)
		case "IFEQ", "IFGT":
			// Not implemented, ignore
			continue
		}
	}

	if err := ops.Set(ctx, key, value, ttl); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) setnxOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("setnx")
	}

	set, err := ops.SetNX(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if set {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) setexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("setex")
	}

	secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	if err := ops.Set(ctx, args[0].Bulk, args[2].Bulk, time.Duration(secs)*time.Second); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) mgetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("mget")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	values, err := ops.MGet(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		if val == nil {
			result[i] = resp.NullBulk()
		} else {
			result[i] = resp.Bulk(val.(string))
		}
	}
	return resp.Arr(result...)
}

func (h *Handler) msetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return resp.ErrWrongArgs("mset")
	}

	pairs := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Bulk] = args[i+1].Bulk
	}

	if err := ops.MSet(ctx, pairs); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) incrOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("incr")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, 1)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) decrOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("decr")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, -1)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) incrbyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("incrby")
	}

	delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) decrbyOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("decrby")
	}

	delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	val, err := ops.Incr(ctx, args[0].Bulk, -delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(val)
}

func (h *Handler) appendCmdOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("append")
	}

	length, err := ops.Append(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

// ============== Key Commands ==============

func (h *Handler) delOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("del")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	deleted, err := ops.Del(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(deleted)
}

func (h *Handler) existsOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("exists")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	count, err := ops.Exists(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) expireOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("expire")
	}

	secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	ok, err := ops.Expire(ctx, args[0].Bulk, time.Duration(secs)*time.Second)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) pexpireOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("pexpire")
	}

	ms, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	ok, err := ops.Expire(ctx, args[0].Bulk, time.Duration(ms)*time.Millisecond)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) ttlOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("ttl")
	}

	ttl, err := ops.TTL(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(ttl)
}

func (h *Handler) pttlOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("pttl")
	}

	pttl, err := ops.PTTL(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(pttl)
}

func (h *Handler) persistOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("persist")
	}

	ok, err := ops.Persist(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) keysOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("keys")
	}

	keys, err := ops.Keys(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(keys))
	for i, key := range keys {
		result[i] = resp.Bulk(key)
	}
	return resp.Arr(result...)
}

func (h *Handler) typeCmdOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("type")
	}

	keyType, err := ops.Type(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Value{Type: resp.SimpleString, Str: string(keyType)}
}

func (h *Handler) renameOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("rename")
	}

	err := ops.Rename(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

// ============== Hash Commands ==============

func (h *Handler) hgetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("hget")
	}

	value, found, err := ops.HGet(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) hsetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.ErrWrongArgs("hset")
	}

	key := args[0].Bulk
	fields := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	added, err := ops.HSet(ctx, key, fields)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) hdelOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hdel")
	}

	key := args[0].Bulk
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i].Bulk
	}

	deleted, err := ops.HDel(ctx, key, fields)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(deleted)
}

func (h *Handler) hgetallOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hgetall")
	}

	fields, err := ops.HGetAll(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	log.Printf("[DEBUG] HGETALL %s returning %d fields", args[0].Bulk, len(fields))

	// Use RESP3 Map type if client supports it, otherwise use flat array (RESP2)
	if UseRESP3(ctx) {
		return resp.MapVal(fields)
	}

	// RESP2: Return flat array [field1, value1, field2, value2, ...]
	result := make([]resp.Value, 0, len(fields)*2)
	for key, value := range fields {
		result = append(result, resp.Bulk(key), resp.Bulk(value))
	}
	return resp.Arr(result...)
}

func (h *Handler) hmgetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hmget")
	}

	key := args[0].Bulk
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i].Bulk
	}

	values, err := ops.HMGet(ctx, key, fields)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		if val == nil {
			result[i] = resp.NullBulk()
		} else {
			result[i] = resp.Bulk(val.(string))
		}
	}
	return resp.Arr(result...)
}

func (h *Handler) hmsetOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return resp.ErrWrongArgs("hmset")
	}

	key := args[0].Bulk
	fields := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	_, err := ops.HSet(ctx, key, fields)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) hexistsOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("hexists")
	}

	exists, err := ops.HExists(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) hkeysOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hkeys")
	}

	keys, err := ops.HKeys(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(keys))
	for i, key := range keys {
		result[i] = resp.Bulk(key)
	}
	return resp.Arr(result...)
}

func (h *Handler) hvalsOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hvals")
	}

	vals, err := ops.HVals(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(vals))
	for i, val := range vals {
		result[i] = resp.Bulk(val)
	}
	return resp.Arr(result...)
}

func (h *Handler) hlenOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hlen")
	}

	length, err := ops.HLen(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) hscanOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hscan")
	}

	key := args[0].Bulk
	// cursor is args[1], we ignore it since we return all results at once

	// Parse optional MATCH and COUNT arguments
	var pattern string
	for i := 2; i < len(args)-1; i += 2 {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "MATCH":
			pattern = args[i+1].Bulk
		case "COUNT":
			// Ignore COUNT, we return all matches
		}
	}

	// Get all fields from the hash
	fields, err := ops.HGetAll(ctx, key)
	if err != nil {
		return resp.Err(err.Error())
	}

	// Build result array with field-value pairs
	result := make([]resp.Value, 0, len(fields)*2)
	for field, value := range fields {
		// Apply pattern matching if specified
		if pattern != "" && pattern != "*" {
			matched, _ := matchGlob(pattern, field)
			if !matched {
				continue
			}
		}
		result = append(result, resp.Bulk(field), resp.Bulk(value))
	}

	// HSCAN returns [cursor, [field1, value1, field2, value2, ...]]
	// We always return cursor "0" to indicate scan is complete
	return resp.Arr(
		resp.Bulk("0"),
		resp.Arr(result...),
	)
}

// matchGlob performs simple glob pattern matching (supports * and ?)
func matchGlob(pattern, s string) (bool, error) {
	pi, si := 0, 0
	starIdx, matchIdx := -1, 0

	for si < len(s) {
		if pi < len(pattern) && (pattern[pi] == '?' || pattern[pi] == s[si]) {
			pi++
			si++
		} else if pi < len(pattern) && pattern[pi] == '*' {
			starIdx = pi
			matchIdx = si
			pi++
		} else if starIdx != -1 {
			pi = starIdx + 1
			matchIdx++
			si = matchIdx
		} else {
			return false, nil
		}
	}

	for pi < len(pattern) && pattern[pi] == '*' {
		pi++
	}

	return pi == len(pattern), nil
}

// ============== Watch Commands ==============
// WATCH and UNWATCH are used for optimistic locking in Redis.
// Since we use PostgreSQL transactions with proper isolation,
// we implement these as no-ops for compatibility.

func (h *Handler) watchOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	// WATCH is a no-op - PostgreSQL transactions provide proper isolation
	return resp.OK()
}

func (h *Handler) unwatchOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	// UNWATCH is a no-op - PostgreSQL transactions provide proper isolation
	return resp.OK()
}

// ============== List Commands ==============

func (h *Handler) lpushOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("lpush")
	}

	key := args[0].Bulk
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}

	length, err := ops.LPush(ctx, key, values)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) rpushOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("rpush")
	}

	key := args[0].Bulk
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}

	length, err := ops.RPush(ctx, key, values)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) lpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("lpop")
	}

	value, found, err := ops.LPop(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) rpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("rpop")
	}

	value, found, err := ops.RPop(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) llenOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("llen")
	}

	length, err := ops.LLen(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) lrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("lrange")
	}

	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	stop, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	values, err := ops.LRange(ctx, args[0].Bulk, start, stop)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		result[i] = resp.Bulk(val)
	}
	return resp.Arr(result...)
}

func (h *Handler) lindexOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("lindex")
	}

	index, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	value, found, err := ops.LIndex(ctx, args[0].Bulk, index)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

// brpopOp implements BRPOP - blocking right pop from list(s)
// BRPOP key [key ...] timeout
func (h *Handler) brpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("brpop")
	}

	// Last arg is timeout in seconds
	timeout, err := strconv.ParseFloat(args[len(args)-1].Bulk, 64)
	if err != nil {
		return resp.Err("timeout is not a float or out of range")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].Bulk
	}

	// Calculate deadline
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(time.Duration(timeout * float64(time.Second)))
	}

	// Poll interval - short for responsiveness
	pollInterval := 100 * time.Millisecond

	for {
		// Try each key in order
		for _, key := range keys {
			value, found, err := ops.RPop(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if found {
				// Return [key, value] as array
				return resp.Arr(resp.Bulk(key), resp.Bulk(value))
			}
		}

		// Check if timeout expired (0 means block forever)
		if timeout > 0 && time.Now().After(deadline) {
			return resp.NullBulk()
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return resp.NullBulk()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}
}

// blpopOp implements BLPOP - blocking left pop from list(s)
// BLPOP key [key ...] timeout
func (h *Handler) blpopOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("blpop")
	}

	// Last arg is timeout in seconds
	timeout, err := strconv.ParseFloat(args[len(args)-1].Bulk, 64)
	if err != nil {
		return resp.Err("timeout is not a float or out of range")
	}

	keys := make([]string, len(args)-1)
	for i := 0; i < len(args)-1; i++ {
		keys[i] = args[i].Bulk
	}

	// Calculate deadline
	var deadline time.Time
	if timeout > 0 {
		deadline = time.Now().Add(time.Duration(timeout * float64(time.Second)))
	}

	// Poll interval - short for responsiveness
	pollInterval := 100 * time.Millisecond

	for {
		// Try each key in order
		for _, key := range keys {
			value, found, err := ops.LPop(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if found {
				// Return [key, value] as array
				return resp.Arr(resp.Bulk(key), resp.Bulk(value))
			}
		}

		// Check if timeout expired (0 means block forever)
		if timeout > 0 && time.Now().After(deadline) {
			return resp.NullBulk()
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return resp.NullBulk()
		case <-time.After(pollInterval):
			// Continue polling
		}
	}
}

// ============== Key Scan Commands ==============

// scanOp implements SCAN - incrementally iterate over keys
// SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
func (h *Handler) scanOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 1 {
		return resp.ErrWrongArgs("scan")
	}

	cursor, err := strconv.ParseInt(args[0].Bulk, 10, 64)
	if err != nil {
		return resp.Err("invalid cursor")
	}

	// Parse optional arguments
	pattern := "*"
	count := int64(10)
	var typeFilter string

	for i := 1; i < len(args); i++ {
		opt := strings.ToUpper(args[i].Bulk)
		switch opt {
		case "MATCH":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			pattern = args[i].Bulk
		case "COUNT":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			count, err = strconv.ParseInt(args[i].Bulk, 10, 64)
			if err != nil {
				return resp.Err("value is not an integer or out of range")
			}
		case "TYPE":
			if i+1 >= len(args) {
				return resp.Err("syntax error")
			}
			i++
			typeFilter = strings.ToLower(args[i].Bulk)
		}
	}

	// Get all matching keys
	allKeys, err := ops.Keys(ctx, pattern)
	if err != nil {
		return resp.Err(err.Error())
	}

	// Filter by type if specified
	var filteredKeys []string
	if typeFilter != "" {
		for _, key := range allKeys {
			keyType, err := ops.Type(ctx, key)
			if err != nil {
				continue
			}
			if string(keyType) == typeFilter {
				filteredKeys = append(filteredKeys, key)
			}
		}
	} else {
		filteredKeys = allKeys
	}

	// Simulate cursor-based pagination
	// cursor is the start index, we return up to 'count' keys
	start := int(cursor)
	if start >= len(filteredKeys) {
		// No more keys, return cursor 0 (end of iteration)
		return resp.Arr(resp.Bulk("0"), resp.Arr())
	}

	end := start + int(count)
	if end > len(filteredKeys) {
		end = len(filteredKeys)
	}

	resultKeys := filteredKeys[start:end]

	// Calculate next cursor
	var nextCursor string
	if end >= len(filteredKeys) {
		nextCursor = "0" // End of iteration
	} else {
		nextCursor = strconv.Itoa(end)
	}

	// Build result array
	keyValues := make([]resp.Value, len(resultKeys))
	for i, key := range resultKeys {
		keyValues[i] = resp.Bulk(key)
	}

	return resp.Arr(resp.Bulk(nextCursor), resp.Arr(keyValues...))
}

// ============== Set Commands ==============

func (h *Handler) saddOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sadd")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	added, err := ops.SAdd(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) sremOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("srem")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	removed, err := ops.SRem(ctx, key, members)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) smembersOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("smembers")
	}

	members, err := ops.SMembers(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(members))
	for i, member := range members {
		result[i] = resp.Bulk(member)
	}
	return resp.Arr(result...)
}

func (h *Handler) sismemberOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("sismember")
	}

	exists, err := ops.SIsMember(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) scardOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("scard")
	}

	count, err := ops.SCard(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

// ============== Sorted Set Commands ==============

func (h *Handler) zaddOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zadd")
	}

	key := args[0].Bulk

	// Parse optional flags (NX, XX, GT, LT, CH)
	// For now, we'll implement basic ZADD without flags
	i := 1

	// Check if we have score-member pairs
	if (len(args)-i)%2 != 0 {
		return resp.ErrWrongArgs("zadd")
	}

	var members []storage.ZMember
	for i < len(args) {
		score, err := strconv.ParseFloat(args[i].Bulk, 64)
		if err != nil {
			return resp.Err("value is not a valid float")
		}
		member := args[i+1].Bulk
		members = append(members, storage.ZMember{Member: member, Score: score})
		i += 2
	}

	added, err := ops.ZAdd(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) zrangeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 3 {
		return resp.ErrWrongArgs("zrange")
	}

	key := args[0].Bulk
	start, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(args[2].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer or out of range")
	}

	withScores := false
	if len(args) > 3 && strings.ToUpper(args[3].Bulk) == "WITHSCORES" {
		withScores = true
	}

	members, err := ops.ZRange(ctx, key, start, stop, withScores)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}

	if withScores {
		result := make([]resp.Value, 0, len(members)*2)
		for _, m := range members {
			result = append(result, resp.Bulk(m.Member))
			result = append(result, resp.Bulk(strconv.FormatFloat(m.Score, 'f', -1, 64)))
		}
		return resp.Arr(result...)
	}

	result := make([]resp.Value, len(members))
	for i, m := range members {
		result[i] = resp.Bulk(m.Member)
	}
	return resp.Arr(result...)
}

func (h *Handler) zscoreOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("zscore")
	}

	score, found, err := ops.ZScore(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(strconv.FormatFloat(score, 'f', -1, 64))
}

func (h *Handler) zremOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("zrem")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	removed, err := ops.ZRem(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) zcardOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("zcard")
	}

	count, err := ops.ZCard(ctx, args[0].Bulk)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

// ============== Server Commands ==============

func (h *Handler) infoOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	uptime := time.Since(h.startTime)
	dbSize, _ := ops.DBSize(ctx)

	info := fmt.Sprintf(`# Server
redis_version:7.0.0-postkeys
os:%s
arch:%s

# Stats
uptime_in_seconds:%d
uptime_in_days:%d

# Keyspace
db0:keys=%d
`, runtime.GOOS, runtime.GOARCH, int(uptime.Seconds()), int(uptime.Hours()/24), dbSize)

	return resp.Bulk(info)
}

func (h *Handler) dbsizeOp(ctx context.Context, ops storage.Operations, args []resp.Value) resp.Value {
	size, err := ops.DBSize(ctx)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(size)
}

// ExecuteWithOps executes a command using the provided Operations interface.
// This is the unified command execution that works for both regular and transaction contexts.
func (h *Handler) ExecuteWithOps(ctx context.Context, ops storage.Operations, cmdName string, args []resp.Value) resp.Value {
	switch cmdName {
	// String commands
	case "GET":
		return h.getOp(ctx, ops, args)
	case "SET":
		return h.setOp(ctx, ops, args)
	case "SETNX":
		return h.setnxOp(ctx, ops, args)
	case "SETEX":
		return h.setexOp(ctx, ops, args)
	case "MGET":
		return h.mgetOp(ctx, ops, args)
	case "MSET":
		return h.msetOp(ctx, ops, args)
	case "INCR":
		return h.incrOp(ctx, ops, args)
	case "DECR":
		return h.decrOp(ctx, ops, args)
	case "INCRBY":
		return h.incrbyOp(ctx, ops, args)
	case "DECRBY":
		return h.decrbyOp(ctx, ops, args)
	case "APPEND":
		return h.appendCmdOp(ctx, ops, args)

	// Key commands
	case "DEL":
		return h.delOp(ctx, ops, args)
	case "EXISTS":
		return h.existsOp(ctx, ops, args)
	case "EXPIRE":
		return h.expireOp(ctx, ops, args)
	case "PEXPIRE":
		return h.pexpireOp(ctx, ops, args)
	case "TTL":
		return h.ttlOp(ctx, ops, args)
	case "PTTL":
		return h.pttlOp(ctx, ops, args)
	case "PERSIST":
		return h.persistOp(ctx, ops, args)
	case "KEYS":
		return h.keysOp(ctx, ops, args)
	case "TYPE":
		return h.typeCmdOp(ctx, ops, args)
	case "RENAME":
		return h.renameOp(ctx, ops, args)

	// Hash commands
	case "HGET":
		return h.hgetOp(ctx, ops, args)
	case "HSET":
		return h.hsetOp(ctx, ops, args)
	case "HDEL":
		return h.hdelOp(ctx, ops, args)
	case "HGETALL":
		return h.hgetallOp(ctx, ops, args)
	case "HMGET":
		return h.hmgetOp(ctx, ops, args)
	case "HMSET":
		return h.hmsetOp(ctx, ops, args)
	case "HEXISTS":
		return h.hexistsOp(ctx, ops, args)
	case "HKEYS":
		return h.hkeysOp(ctx, ops, args)
	case "HVALS":
		return h.hvalsOp(ctx, ops, args)
	case "HLEN":
		return h.hlenOp(ctx, ops, args)
	case "HSCAN":
		return h.hscanOp(ctx, ops, args)

	// Watch commands (no-ops for PostgreSQL compatibility)
	case "WATCH":
		return h.watchOp(ctx, ops, args)
	case "UNWATCH":
		return h.unwatchOp(ctx, ops, args)

	// List commands
	case "LPUSH":
		return h.lpushOp(ctx, ops, args)
	case "RPUSH":
		return h.rpushOp(ctx, ops, args)
	case "LPOP":
		return h.lpopOp(ctx, ops, args)
	case "RPOP":
		return h.rpopOp(ctx, ops, args)
	case "BLPOP":
		return h.blpopOp(ctx, ops, args)
	case "BRPOP":
		return h.brpopOp(ctx, ops, args)
	case "LLEN":
		return h.llenOp(ctx, ops, args)
	case "LRANGE":
		return h.lrangeOp(ctx, ops, args)
	case "LINDEX":
		return h.lindexOp(ctx, ops, args)

	// Key scan commands
	case "SCAN":
		return h.scanOp(ctx, ops, args)

	// Set commands
	case "SADD":
		return h.saddOp(ctx, ops, args)
	case "SREM":
		return h.sremOp(ctx, ops, args)
	case "SMEMBERS":
		return h.smembersOp(ctx, ops, args)
	case "SISMEMBER":
		return h.sismemberOp(ctx, ops, args)
	case "SCARD":
		return h.scardOp(ctx, ops, args)

	// Sorted set commands
	case "ZADD":
		return h.zaddOp(ctx, ops, args)
	case "ZRANGE":
		return h.zrangeOp(ctx, ops, args)
	case "ZSCORE":
		return h.zscoreOp(ctx, ops, args)
	case "ZREM":
		return h.zremOp(ctx, ops, args)
	case "ZCARD":
		return h.zcardOp(ctx, ops, args)

	// Server commands
	case "INFO":
		return h.infoOp(ctx, ops, args)
	case "DBSIZE":
		return h.dbsizeOp(ctx, ops, args)

	// Scripting commands
	case "EVAL":
		return h.evalOp(ctx, ops, args)
	case "EVALSHA":
		return h.evalshaOp(ctx, ops, args)
	case "SCRIPT":
		return h.scriptOp(ctx, ops, args)

	default:
		return resp.Err(fmt.Sprintf("unknown command '%s'", cmdName))
	}
}
