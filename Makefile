.PHONY: build test bench docker-up docker-down test-up test-down clean

# Build
build:
	go build -o bin/postkeys ./cmd/server

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

# Run integration tests against PostgreSQL
test: test-up
	go test -v -tags=postgres ./tests/...
	go test -v ./internal/...

# Run benchmarks against PostgreSQL
bench: test-up
	go test -bench=. -benchmem -tags=postgres ./tests/...

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
