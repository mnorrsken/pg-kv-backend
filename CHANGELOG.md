# Changelog

All notable changes to this project will be documented in this file.

## [0.10] - 2026-02-01

### Added
- More complete RESP3 support, including Lua and queue support.
- Configurable trace levels (0-3) for SQL and RESP command logging
- Graceful shutdown with 30-second timeout and ordered component shutdown
- Force-exit on second signal during shutdown

### Fixed
- Transaction rollback errors from ignored QueryRow errors in list/hash operations
- pg_notify "channel name too long" errors for keys exceeding 63 bytes

### Changed
- SQLTRACE and TRACE now accept levels 0-3 instead of boolean values

## [0.9] - 2026-01-31

### Added
- HELLO command support for Redis protocol negotiation (RESP2/RESP3)
- HELLO can run without authentication (like PING, QUIT, COMMAND)
- HELLO AUTH inline authentication support

## [0.8] - 2026-01-31

### Added
- Debug logging support via `DEBUG=1` environment variable
- Enhanced error logging with remote address details when debug enabled
- RESP parser logs full buffer content on unknown type errors in debug mode
- Helm chart `debug` option to enable debug logging

## [0.7] - 2026-01-31

### Added
- Redis password management with secret generation job
- Example configuration with CloudNativePG for full HA setup
- Helm chart installation and configuration details to README

### Changed
- Renamed project from `pg-kv-backend` to `postkeys`
- Updated database key references in configuration files
- Refactored Grafana dashboard configuration and panel settings
- Updated test command to include internal tests

## [0.6] - 2026-01-29

### Changed
- Updated Docker publish workflow to include GitHub release creation
- Streamlined tagging process in CI/CD

### Added
- Grafana dashboard for monitoring
- Unit tests for cache and RESP protocol
- Additional PostgreSQL integration tests

## [0.5] - 2026-01-28

### Added
- CLIENT commands handling with ClientState management
- In-memory cache support with configurable TTL and max size

## [0.4] - 2026-01-27

### Added
- Prometheus metrics support with `/metrics` endpoint
- ServiceMonitor for Prometheus Operator integration
- Configurable metrics server address

### Changed
- Updated PostgreSQL secret handling and configuration options

## [0.3] - 2026-01-26

### Added
- Initial release with Redis-compatible protocol
- PostgreSQL backend for persistent storage
- Helm chart for Kubernetes deployment
- Docker image published to GHCR

