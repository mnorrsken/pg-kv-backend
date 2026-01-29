# pg-kv-backend

> [!CAUTION]
> **DISCLAIMER: This is purely vibecoded and will most likely have a large amount of bugs and security issues. THIS IS ONLY A TEST CONCEPT**

A Redis 7 API-compatible server that uses PostgreSQL as the backend storage.

## Features

- Redis protocol (RESP3) compatible
- PostgreSQL persistent storage
- Supports common Redis commands:
  - **String commands**: GET, SET, SETNX, SETEX, MGET, MSET, INCR, DECR, INCRBY, DECRBY, APPEND
  - **Key commands**: DEL, EXISTS, EXPIRE, TTL, PTTL, PERSIST, KEYS, TYPE, RENAME
  - **Hash commands**: HGET, HSET, HDEL, HGETALL, HMGET, HMSET, HEXISTS, HKEYS, HVALS, HLEN
  - **List commands**: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX
  - **Set commands**: SADD, SREM, SMEMBERS, SISMEMBER, SCARD
  - **Server commands**: PING, ECHO, INFO, DBSIZE, FLUSHDB, FLUSHALL

## Requirements

- Go 1.21+
- PostgreSQL 14+

## Installation

```bash
go build -o pg-kv-backend ./cmd/server
```

## Configuration

Environment variables:

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_ADDR` | Address to listen on | `:6379` |
| `REDIS_PASSWORD` | Authentication password (optional) | `` |
| `PG_HOST` | PostgreSQL host | `localhost` |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_USER` | PostgreSQL user | `postgres` |
| `PG_PASSWORD` | PostgreSQL password | `postgres` |
| `PG_DATABASE` | PostgreSQL database | `pgkv` |
| `PG_SSLMODE` | PostgreSQL SSL mode | `disable` |

## Running

### With Docker Compose

```bash
docker-compose up -d
```

### Manually

1. Start PostgreSQL and create a database
2. Set environment variables
3. Run the server:

```bash
./pg-kv-backend
```

## Usage

Connect using any Redis client:

```bash
redis-cli -p 6379

> SET mykey "Hello"
OK
> GET mykey
"Hello"
> HSET user:1 name "John" age "30"
(integer) 2
> HGETALL user:1
1) "name"
2) "John"
3) "age"
4) "30"
```

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Redis Client  │────▶│   RESP Parser   │────▶│  Command Handler│
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                                                ┌─────────────────┐
                                                │  PostgreSQL DB  │
                                                └─────────────────┘
```

