// Package handler implements Redis command handlers.
// This file contains the unified command handlers that work with storage.Operations.
package handler

import (
	"context"
	"fmt"
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

	// List commands
	case "LPUSH":
		return h.lpushOp(ctx, ops, args)
	case "RPUSH":
		return h.rpushOp(ctx, ops, args)
	case "LPOP":
		return h.lpopOp(ctx, ops, args)
	case "RPOP":
		return h.rpopOp(ctx, ops, args)
	case "LLEN":
		return h.llenOp(ctx, ops, args)
	case "LRANGE":
		return h.lrangeOp(ctx, ops, args)
	case "LINDEX":
		return h.lindexOp(ctx, ops, args)

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

	// Server commands
	case "INFO":
		return h.infoOp(ctx, ops, args)
	case "DBSIZE":
		return h.dbsizeOp(ctx, ops, args)

	default:
		return resp.Err(fmt.Sprintf("unknown command '%s'", cmdName))
	}
}
