# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- Renamed project from `pg-kv-backend` to `postkeys`

## [0.7.0] - 2026-01-31

### Added
- Redis password management with secret generation job
- Example configuration with CloudNativePG for full HA setup
- Helm chart installation and configuration details to README

### Changed
- Updated database key references in configuration files
- Refactored Grafana dashboard configuration and panel settings
- Updated test command to include internal tests

## [0.6.1] - 2026-01-30

### Changed
- Updated Docker publish workflow to include GitHub release creation
- Streamlined tagging process in CI/CD

## [0.6.0] - 2026-01-29

### Added
- Grafana dashboard for monitoring
- Unit tests for cache and RESP protocol
- Additional PostgreSQL integration tests

## [0.5.0] - 2026-01-28

### Added
- CLIENT commands handling with ClientState management
- In-memory cache support with configurable TTL and max size

## [0.4.0] - 2026-01-27

### Added
- Prometheus metrics support with `/metrics` endpoint
- ServiceMonitor for Prometheus Operator integration
- Configurable metrics server address

### Changed
- Updated PostgreSQL secret handling and configuration options

## [0.3.0] - 2026-01-26

### Added
- Initial release with Redis-compatible protocol
- PostgreSQL backend for persistent storage
- Helm chart for Kubernetes deployment
- Docker image published to GHCR

[Unreleased]: https://github.com/mnorrsken/postkeys/compare/v0.7.0...HEAD
[0.7.0]: https://github.com/mnorrsken/postkeys/compare/v0.6.1...v0.7.0
[0.6.1]: https://github.com/mnorrsken/postkeys/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/mnorrsken/postkeys/compare/v0.5.0...v0.6.0
[0.5.0]: https://github.com/mnorrsken/postkeys/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/mnorrsken/postkeys/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/mnorrsken/postkeys/releases/tag/v0.3.0
