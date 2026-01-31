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
  - **Connection commands**: PING, ECHO, AUTH, QUIT
  - **Client commands**: CLIENT ID, CLIENT GETNAME, CLIENT SETNAME, CLIENT SETINFO, CLIENT INFO, CLIENT LIST
  - **Server commands**: INFO, DBSIZE, FLUSHDB, FLUSHALL, COMMAND

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

## Helm Chart

The Helm chart is available for deploying pg-kv-backend to Kubernetes.

### Installation

```bash
# Add the repository (if hosted) or install from local chart
helm install pg-kv-backend ./charts/pg-kv-backend

# Install with custom values
helm install pg-kv-backend ./charts/pg-kv-backend -f my-values.yaml

# Install in a specific namespace
helm install pg-kv-backend ./charts/pg-kv-backend -n my-namespace --create-namespace
```

### Configuration

The following table lists the configurable parameters of the pg-kv-backend chart and their default values.

#### General

| Parameter | Description | Default |
|-----------|-------------|---------|
| `replicaCount` | Number of replicas | `1` |
| `image.repository` | Image repository | `ghcr.io/mnorrsken/pg-kv-backend` |
| `image.pullPolicy` | Image pull policy | `IfNotPresent` |
| `image.tag` | Image tag (defaults to chart appVersion) | `""` |
| `imagePullSecrets` | Image pull secrets | `[]` |
| `nameOverride` | Override the chart name | `""` |
| `fullnameOverride` | Override the full release name | `""` |

#### Service Account

| Parameter | Description | Default |
|-----------|-------------|---------|
| `serviceAccount.create` | Create a service account | `true` |
| `serviceAccount.annotations` | Service account annotations | `{}` |
| `serviceAccount.name` | Service account name | `""` |

#### Pod Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `podAnnotations` | Pod annotations | `{}` |
| `podSecurityContext` | Pod security context | `{}` |
| `securityContext.readOnlyRootFilesystem` | Read-only root filesystem | `true` |
| `securityContext.runAsNonRoot` | Run as non-root user | `true` |
| `securityContext.runAsUser` | User ID to run as | `1000` |
| `resources` | CPU/Memory resource requests/limits | `{}` |
| `nodeSelector` | Node selector | `{}` |
| `tolerations` | Tolerations | `[]` |
| `affinity` | Affinity rules | `{}` |

#### Service

| Parameter | Description | Default |
|-----------|-------------|---------|
| `service.type` | Service type | `ClusterIP` |
| `service.port` | Service port | `6379` |

#### Ingress

| Parameter | Description | Default |
|-----------|-------------|---------|
| `ingress.enabled` | Enable ingress | `false` |
| `ingress.className` | Ingress class name | `""` |
| `ingress.annotations` | Ingress annotations | `{}` |
| `ingress.hosts` | Ingress hosts configuration | `[]` |
| `ingress.tls` | Ingress TLS configuration | `[]` |

#### Autoscaling

| Parameter | Description | Default |
|-----------|-------------|---------|
| `autoscaling.enabled` | Enable horizontal pod autoscaling | `false` |
| `autoscaling.minReplicas` | Minimum replicas | `1` |
| `autoscaling.maxReplicas` | Maximum replicas | `100` |
| `autoscaling.targetCPUUtilizationPercentage` | Target CPU utilization | `80` |

#### Redis Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `redis.addr` | Address to listen on inside the container | `:6379` |
| `redis.password.create` | Enable auto-generation of a Redis password secret via a Helm hook Job | `false` |
| `redis.password.secretName` | Name of the secret to create (if `create` is true) | `pgkv-secret` |
| `redis.password.value` | Redis password (ignored if `create` is true or existingSecret is set) | `""` |
| `redis.password.secretGenerator.image.repository` | Image repository for the secret generator Job | `rancher/kubectl` |
| `redis.password.secretGenerator.image.tag` | Image tag for the secret generator Job | `v1.35.0` |
| `redis.password.secretGenerator.image.pullPolicy` | Image pull policy for the secret generator Job | `IfNotPresent` |
| `redis.password.existingSecret.name` | Name of existing secret for Redis password | `""` |
| `redis.password.existingSecret.key` | Key in secret containing the password | `redis-password` |

> **Note:** When `redis.password.create` is `true`, a random 32-character password is automatically generated using a Kubernetes Job that runs as a Helm pre-install/pre-upgrade hook. The `password.value` field is ignored in this case. If the secret already exists, it will not be overwritten. The Job inherits `nodeSelector` and `tolerations` from the main deployment configuration.

#### PostgreSQL Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `postgresql.host` | PostgreSQL host | `postgresql` |
| `postgresql.port` | PostgreSQL port | `5432` |
| `postgresql.database` | PostgreSQL database name | `pgkv` |
| `postgresql.sslmode` | PostgreSQL SSL mode | `disable` |
| `postgresql.auth.username` | PostgreSQL username | `postgres` |
| `postgresql.auth.password` | PostgreSQL password (ignored if existingSecret is set) | `""` |
| `postgresql.existingSecret.name` | Name of existing secret for PostgreSQL credentials | `""` |
| `postgresql.existingSecret.usernameKey` | Key in secret containing the username | `""` |
| `postgresql.existingSecret.passwordKey` | Key in secret containing the password | `password` |
| `postgresql.existingSecret.hostKey` | Key in secret containing the host | `""` |
| `postgresql.existingSecret.portKey` | Key in secret containing the port | `""` |
| `postgresql.existingSecret.databaseKey` | Key in secret containing the database name | `""` |

#### Cache Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `cache.enabled` | Enable in-memory cache (opt-in) | `false` |
| `cache.ttl` | Cache TTL duration | `250ms` |
| `cache.maxSize` | Maximum number of cached entries | `10000` |

> **Note:** In multi-pod deployments, cached data may be stale for up to TTL duration.

#### Metrics Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `metrics.enabled` | Enable metrics endpoint | `true` |
| `metrics.addr` | Metrics server address inside the container | `:9090` |
| `metrics.service.port` | Metrics service port | `9090` |
| `metrics.service.annotations` | Metrics service annotations | `{}` |
| `metrics.serviceMonitor.enabled` | Enable ServiceMonitor (requires Prometheus Operator) | `false` |
| `metrics.serviceMonitor.namespace` | ServiceMonitor namespace | `""` |
| `metrics.serviceMonitor.labels` | ServiceMonitor labels | `{}` |
| `metrics.serviceMonitor.interval` | Scrape interval | `30s` |
| `metrics.serviceMonitor.scrapeTimeout` | Scrape timeout | `10s` |
| `metrics.serviceMonitor.metricRelabelings` | Metric relabel configs | `[]` |
| `metrics.serviceMonitor.relabelings` | Relabel configs | `[]` |
| `metrics.serviceMonitor.honorLabels` | Honor labels | `false` |

#### Additional Configuration

| Parameter | Description | Default |
|-----------|-------------|---------|
| `extraEnv` | Additional environment variables | `[]` |
| `extraEnvFrom` | Additional environment variables from secrets/configmaps | `[]` |

### Examples

For deployment examples, including usage with CloudNativePG, see the [examples/](examples/) folder.

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

