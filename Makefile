.PHONY: build test test-postgres bench bench-postgres bench-compare docker-up docker-down test-up test-down clean

# Build
build:
	go build -o bin/pg-kv-backend ./cmd/server

# Run unit tests (mock storage)
test:
	go test -v ./tests/...

# Start test PostgreSQL container
test-up:
	docker compose -f docker-compose.test.yml up -d
	@echo "Waiting for PostgreSQL to be ready..."
	@sleep 3
	@until docker compose -f docker-compose.test.yml exec -T postgres pg_isready -U postgres > /dev/null 2>&1; do \
		sleep 1; \
	done
	@echo "PostgreSQL is ready!"

# Stop test PostgreSQL container
test-down:
	docker compose -f docker-compose.test.yml down -v

# Run tests against PostgreSQL
test-postgres: test-up
	go test -v -tags=postgres ./tests/...

# Run benchmarks (mock storage only)
bench:
	go test -bench=. -benchmem ./tests/...

# Run benchmarks against PostgreSQL
bench-postgres: test-up
	go test -bench=BenchmarkPg -benchmem -tags=postgres ./tests/...

# Run comparison benchmarks (Mock vs PostgreSQL)
bench-compare: test-up
	@echo "=== Mock Storage Benchmarks ==="
	go test -bench='BenchmarkMock|BenchmarkSetGet' -benchmem -tags=postgres ./tests/... 2>/dev/null | grep -E '(Benchmark|ns/op)'
	@echo ""
	@echo "=== PostgreSQL Storage Benchmarks ==="
	go test -bench=BenchmarkPg -benchmem -tags=postgres ./tests/... 2>/dev/null | grep -E '(Benchmark|ns/op)'

# Start production containers
docker-up:
	docker compose up -d

# Stop production containers
docker-down:
	docker compose down

# Clean build artifacts
clean:
	rm -rf bin/
	docker compose -f docker-compose.test.yml down -v 2>/dev/null || true
