// Package handler implements Redis command handlers.
package handler

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/mnorrsken/pg-kv-backend/internal/resp"
	"github.com/mnorrsken/pg-kv-backend/internal/storage"
)

// Handler processes Redis commands
type Handler struct {
	store     *storage.Store
	password  string
	startTime time.Time
}

// New creates a new command handler
func New(store *storage.Store, password string) *Handler {
	return &Handler{
		store:     store,
		password:  password,
		startTime: time.Now(),
	}
}

// RequiresAuth returns true if a password is configured
func (h *Handler) RequiresAuth() bool {
	return h.password != ""
}

// CheckAuth verifies the provided password
func (h *Handler) CheckAuth(providedPassword string) bool {
	return h.password == providedPassword
}

// Handle processes a RESP command and returns a response
func (h *Handler) Handle(ctx context.Context, cmd resp.Value) resp.Value {
	if cmd.Type != resp.Array || len(cmd.Array) == 0 {
		return resp.Err("invalid command format")
	}

	// Extract command name
	cmdName := strings.ToUpper(cmd.Array[0].Bulk)
	args := cmd.Array[1:]

	switch cmdName {
	// Connection commands
	case "PING":
		return h.ping(args)
	case "ECHO":
		return h.echo(args)
	case "QUIT":
		return resp.OK()
	case "AUTH":
		return h.auth(args)
	case "COMMAND":
		return h.command(args)

	// String commands
	case "GET":
		return h.get(ctx, args)
	case "SET":
		return h.set(ctx, args)
	case "SETNX":
		return h.setnx(ctx, args)
	case "SETEX":
		return h.setex(ctx, args)
	case "MGET":
		return h.mget(ctx, args)
	case "MSET":
		return h.mset(ctx, args)
	case "INCR":
		return h.incr(ctx, args)
	case "DECR":
		return h.decr(ctx, args)
	case "INCRBY":
		return h.incrby(ctx, args)
	case "DECRBY":
		return h.decrby(ctx, args)
	case "APPEND":
		return h.appendCmd(ctx, args)

	// Key commands
	case "DEL":
		return h.del(ctx, args)
	case "EXISTS":
		return h.exists(ctx, args)
	case "EXPIRE":
		return h.expire(ctx, args)
	case "PEXPIRE":
		return h.pexpire(ctx, args)
	case "TTL":
		return h.ttl(ctx, args)
	case "PTTL":
		return h.pttl(ctx, args)
	case "PERSIST":
		return h.persist(ctx, args)
	case "KEYS":
		return h.keys(ctx, args)
	case "TYPE":
		return h.typeCmd(ctx, args)
	case "RENAME":
		return h.rename(ctx, args)

	// Hash commands
	case "HGET":
		return h.hget(ctx, args)
	case "HSET":
		return h.hset(ctx, args)
	case "HDEL":
		return h.hdel(ctx, args)
	case "HGETALL":
		return h.hgetall(ctx, args)
	case "HMGET":
		return h.hmget(ctx, args)
	case "HMSET":
		return h.hmset(ctx, args)
	case "HEXISTS":
		return h.hexists(ctx, args)
	case "HKEYS":
		return h.hkeys(ctx, args)
	case "HVALS":
		return h.hvals(ctx, args)
	case "HLEN":
		return h.hlen(ctx, args)

	// List commands
	case "LPUSH":
		return h.lpush(ctx, args)
	case "RPUSH":
		return h.rpush(ctx, args)
	case "LPOP":
		return h.lpop(ctx, args)
	case "RPOP":
		return h.rpop(ctx, args)
	case "LLEN":
		return h.llen(ctx, args)
	case "LRANGE":
		return h.lrange(ctx, args)
	case "LINDEX":
		return h.lindex(ctx, args)

	// Set commands
	case "SADD":
		return h.sadd(ctx, args)
	case "SREM":
		return h.srem(ctx, args)
	case "SMEMBERS":
		return h.smembers(ctx, args)
	case "SISMEMBER":
		return h.sismember(ctx, args)
	case "SCARD":
		return h.scard(ctx, args)

	// Server commands
	case "INFO":
		return h.info(ctx, args)
	case "DBSIZE":
		return h.dbsize(ctx, args)
	case "FLUSHDB":
		return h.flushdb(ctx, args)
	case "FLUSHALL":
		return h.flushdb(ctx, args) // Same as FLUSHDB for single-DB

	default:
		return resp.Err(fmt.Sprintf("unknown command '%s'", cmdName))
	}
}

// ============== Connection Commands ==============

func (h *Handler) ping(args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.Value{Type: resp.SimpleString, Str: "PONG"}
	}
	return resp.Bulk(args[0].Bulk)
}

func (h *Handler) echo(args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("echo")
	}
	return resp.Bulk(args[0].Bulk)
}

func (h *Handler) command(args []resp.Value) resp.Value {
	// Simplified COMMAND response
	if len(args) > 0 && strings.ToUpper(args[0].Bulk) == "DOCS" {
		return resp.Arr()
	}
	return resp.Arr()
}

func (h *Handler) auth(args []resp.Value) resp.Value {
	// AUTH command is handled specially in the server connection loop
	// This is called when AUTH is sent but no password is configured
	if !h.RequiresAuth() {
		return resp.Err("AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	if len(args) == 0 {
		return resp.ErrWrongArgs("auth")
	}
	// Support both AUTH <password> and AUTH <username> <password> (Redis 6+ ACL style)
	var password string
	if len(args) == 1 {
		password = args[0].Bulk
	} else if len(args) == 2 {
		// args[0] is username (ignored, we only support default user)
		password = args[1].Bulk
	} else {
		return resp.ErrWrongArgs("auth")
	}
	if h.CheckAuth(password) {
		return resp.OK()
	}
	return resp.Value{Type: resp.Error, Str: "WRONGPASS invalid username-password pair"}
}

// ============== String Commands ==============

func (h *Handler) get(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("get")
	}

	value, found, err := h.store.Get(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) set(ctx context.Context, args []resp.Value) resp.Value {
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
			// Set only if not exists - handled separately
			ok, err := h.store.SetNX(ctx, key, value)
			if err != nil {
				return resp.Err(err.Error())
			}
			if !ok {
				return resp.NullBulk()
			}
			return resp.OK()
		case "XX":
			// Set only if exists
			_, found, err := h.store.Get(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if !found {
				return resp.NullBulk()
			}
		case "KEEPTTL":
			// Keep existing TTL - get current TTL
			currentTTL, err := h.store.TTL(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if currentTTL > 0 {
				ttl = time.Duration(currentTTL) * time.Second
			}
		case "GET":
			// Return old value
			oldValue, found, err := h.store.Get(ctx, key)
			if err != nil {
				return resp.Err(err.Error())
			}
			if err := h.store.Set(ctx, key, value, ttl); err != nil {
				return resp.Err(err.Error())
			}
			if !found {
				return resp.NullBulk()
			}
			return resp.Bulk(oldValue)
		}
	}

	if err := h.store.Set(ctx, key, value, ttl); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) setnx(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("setnx")
	}

	ok, err := h.store.SetNX(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) setex(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 3 {
		return resp.ErrWrongArgs("setex")
	}

	secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	if err := h.store.Set(ctx, args[0].Bulk, args[2].Bulk, time.Duration(secs)*time.Second); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) mget(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("mget")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	values, err := h.store.MGet(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = resp.NullBulk()
		} else {
			result[i] = resp.Bulk(v.(string))
		}
	}
	return resp.Arr(result...)
}

func (h *Handler) mset(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 || len(args)%2 != 0 {
		return resp.ErrWrongArgs("mset")
	}

	pairs := make(map[string]string)
	for i := 0; i < len(args); i += 2 {
		pairs[args[i].Bulk] = args[i+1].Bulk
	}

	if err := h.store.MSet(ctx, pairs); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) incr(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("incr")
	}

	result, err := h.store.Incr(ctx, args[0].Bulk, 1)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(result)
}

func (h *Handler) decr(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("decr")
	}

	result, err := h.store.Incr(ctx, args[0].Bulk, -1)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(result)
}

func (h *Handler) incrby(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("incrby")
	}

	delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	result, err := h.store.Incr(ctx, args[0].Bulk, delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(result)
}

func (h *Handler) decrby(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("decrby")
	}

	delta, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	result, err := h.store.Incr(ctx, args[0].Bulk, -delta)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(result)
}

func (h *Handler) appendCmd(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("append")
	}

	length, err := h.store.Append(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

// ============== Key Commands ==============

func (h *Handler) del(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("del")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	deleted, err := h.store.Del(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(deleted)
}

func (h *Handler) exists(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) == 0 {
		return resp.ErrWrongArgs("exists")
	}

	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = arg.Bulk
	}

	count, err := h.store.Exists(ctx, keys)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) expire(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("expire")
	}

	secs, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	ok, err := h.store.Expire(ctx, args[0].Bulk, time.Duration(secs)*time.Second)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) pexpire(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("pexpire")
	}

	ms, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	ok, err := h.store.Expire(ctx, args[0].Bulk, time.Duration(ms)*time.Millisecond)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) ttl(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("ttl")
	}

	ttl, err := h.store.TTL(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(ttl)
}

func (h *Handler) pttl(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("pttl")
	}

	pttl, err := h.store.PTTL(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(pttl)
}

func (h *Handler) persist(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("persist")
	}

	ok, err := h.store.Persist(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if ok {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) keys(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("keys")
	}

	keys, err := h.store.Keys(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(keys))
	for i, key := range keys {
		result[i] = resp.Bulk(key)
	}
	return resp.Arr(result...)
}

func (h *Handler) typeCmd(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("type")
	}

	keyType, err := h.store.Type(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Value{Type: resp.SimpleString, Str: string(keyType)}
}

func (h *Handler) rename(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("rename")
	}

	if err := h.store.Rename(ctx, args[0].Bulk, args[1].Bulk); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}

// ============== Hash Commands ==============

func (h *Handler) hget(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("hget")
	}

	value, found, err := h.store.HGet(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) hset(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.ErrWrongArgs("hset")
	}

	key := args[0].Bulk
	fields := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	count, err := h.store.HSet(ctx, key, fields)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) hdel(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hdel")
	}

	key := args[0].Bulk
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i].Bulk
	}

	count, err := h.store.HDel(ctx, key, fields)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

func (h *Handler) hgetall(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hgetall")
	}

	hash, err := h.store.HGetAll(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, 0, len(hash)*2)
	for field, value := range hash {
		result = append(result, resp.Bulk(field), resp.Bulk(value))
	}
	return resp.Arr(result...)
}

func (h *Handler) hmget(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("hmget")
	}

	key := args[0].Bulk
	fields := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		fields[i-1] = args[i].Bulk
	}

	values, err := h.store.HMGet(ctx, key, fields)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, v := range values {
		if v == nil {
			result[i] = resp.NullBulk()
		} else {
			result[i] = resp.Bulk(v.(string))
		}
	}
	return resp.Arr(result...)
}

func (h *Handler) hmset(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 3 || len(args)%2 == 0 {
		return resp.ErrWrongArgs("hmset")
	}

	key := args[0].Bulk
	fields := make(map[string]string)
	for i := 1; i < len(args); i += 2 {
		fields[args[i].Bulk] = args[i+1].Bulk
	}

	_, err := h.store.HSet(ctx, key, fields)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.OK()
}

func (h *Handler) hexists(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("hexists")
	}

	exists, err := h.store.HExists(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) hkeys(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hkeys")
	}

	keys, err := h.store.HKeys(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(keys))
	for i, key := range keys {
		result[i] = resp.Bulk(key)
	}
	return resp.Arr(result...)
}

func (h *Handler) hvals(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hvals")
	}

	vals, err := h.store.HVals(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(vals))
	for i, val := range vals {
		result[i] = resp.Bulk(val)
	}
	return resp.Arr(result...)
}

func (h *Handler) hlen(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("hlen")
	}

	length, err := h.store.HLen(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

// ============== List Commands ==============

func (h *Handler) lpush(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("lpush")
	}

	key := args[0].Bulk
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}

	length, err := h.store.LPush(ctx, key, values)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) rpush(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("rpush")
	}

	key := args[0].Bulk
	values := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		values[i-1] = args[i].Bulk
	}

	length, err := h.store.RPush(ctx, key, values)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) lpop(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("lpop")
	}

	value, found, err := h.store.LPop(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) rpop(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("rpop")
	}

	value, found, err := h.store.RPop(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

func (h *Handler) llen(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("llen")
	}

	length, err := h.store.LLen(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(length)
}

func (h *Handler) lrange(ctx context.Context, args []resp.Value) resp.Value {
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

	values, err := h.store.LRange(ctx, args[0].Bulk, start, stop)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(values))
	for i, val := range values {
		result[i] = resp.Bulk(val)
	}
	return resp.Arr(result...)
}

func (h *Handler) lindex(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("lindex")
	}

	index, err := strconv.ParseInt(args[1].Bulk, 10, 64)
	if err != nil {
		return resp.Err("value is not an integer")
	}

	value, found, err := h.store.LIndex(ctx, args[0].Bulk, index)
	if err != nil {
		return resp.Err(err.Error())
	}
	if !found {
		return resp.NullBulk()
	}
	return resp.Bulk(value)
}

// ============== Set Commands ==============

func (h *Handler) sadd(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("sadd")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	added, err := h.store.SAdd(ctx, key, members)
	if err != nil {
		if strings.Contains(err.Error(), "WRONGTYPE") {
			return resp.ErrWrongType()
		}
		return resp.Err(err.Error())
	}
	return resp.Int(added)
}

func (h *Handler) srem(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) < 2 {
		return resp.ErrWrongArgs("srem")
	}

	key := args[0].Bulk
	members := make([]string, len(args)-1)
	for i := 1; i < len(args); i++ {
		members[i-1] = args[i].Bulk
	}

	removed, err := h.store.SRem(ctx, key, members)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(removed)
}

func (h *Handler) smembers(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("smembers")
	}

	members, err := h.store.SMembers(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}

	result := make([]resp.Value, len(members))
	for i, member := range members {
		result[i] = resp.Bulk(member)
	}
	return resp.Arr(result...)
}

func (h *Handler) sismember(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 2 {
		return resp.ErrWrongArgs("sismember")
	}

	exists, err := h.store.SIsMember(ctx, args[0].Bulk, args[1].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	if exists {
		return resp.Int(1)
	}
	return resp.Int(0)
}

func (h *Handler) scard(ctx context.Context, args []resp.Value) resp.Value {
	if len(args) != 1 {
		return resp.ErrWrongArgs("scard")
	}

	count, err := h.store.SCard(ctx, args[0].Bulk)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(count)
}

// ============== Server Commands ==============

func (h *Handler) info(ctx context.Context, args []resp.Value) resp.Value {
	uptime := time.Since(h.startTime)
	dbSize, _ := h.store.DBSize(ctx)

	info := fmt.Sprintf(`# Server
redis_version:7.0.0-pg-kv-backend
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

func (h *Handler) dbsize(ctx context.Context, args []resp.Value) resp.Value {
	size, err := h.store.DBSize(ctx)
	if err != nil {
		return resp.Err(err.Error())
	}
	return resp.Int(size)
}

func (h *Handler) flushdb(ctx context.Context, args []resp.Value) resp.Value {
	if err := h.store.FlushDB(ctx); err != nil {
		return resp.Err(err.Error())
	}
	return resp.OK()
}
