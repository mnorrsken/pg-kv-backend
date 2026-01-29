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
| `METRICS_ADDR` | Prometheus metrics server address | `:9090` |
| `PG_HOST` | PostgreSQL host | `localhost` |
| `PG_PORT` | PostgreSQL port | `5432` |
| `PG_USER` | PostgreSQL user | `postgres` |
| `PG_PASSWORD` | PostgreSQL password | `postgres` |
| `PG_DATABASE` | PostgreSQL database | `pgkv` |
| `PG_SSLMODE` | PostgreSQL SSL mode | `disable` |
| `CACHE_ENABLED` | Enable in-memory cache (opt-in) | `false` |
| `CACHE_TTL` | Cache TTL duration | `250ms` |
| `CACHE_MAX_SIZE` | Maximum cached entries | `10000` |

### In-Memory Cache

The optional in-memory cache reduces PostgreSQL load for read-heavy workloads by caching `GET` results:

```bash
export CACHE_ENABLED=true
export CACHE_TTL=250ms
export CACHE_MAX_SIZE=10000
```

**Important considerations:**
- Cache is **opt-in** and disabled by default
- Only caches string `GET` operations (hashes, lists, sets are not cached)
- In **multi-pod deployments**, cached data may be stale for up to TTL duration
- Writes (`SET`, `DEL`, etc.) invalidate the local cache immediately
- Monitor cache effectiveness with `pgkv_cache_hits_total` and `pgkv_cache_misses_total` metrics

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

## Metrics

Prometheus metrics are exposed on a separate HTTP server (default port `:9090`).

### Available Endpoints

- `GET /metrics` - Prometheus metrics
- `GET /health` - Health check endpoint

### Metrics Exposed

| Metric | Type | Description |
|--------|------|-------------|
| `pgkv_commands_total` | Counter | Total number of Redis commands processed (labeled by command) |
| `pgkv_command_duration_seconds` | Histogram | Duration of Redis command execution in seconds (labeled by command) |
| `pgkv_command_errors_total` | Counter | Total number of Redis command errors (labeled by command) |
| `pgkv_active_connections` | Gauge | Number of active client connections |
| `pgkv_connections_total` | Counter | Total number of connections accepted |

### Example Prometheus Configuration

```yaml
scrape_configs:
  - job_name: 'pg-kv-backend'
    static_configs:
      - targets: ['localhost:9090']
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

