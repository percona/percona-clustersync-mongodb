GOOS?=$(shell go env GOOS)
GITCOMMIT?=$(shell git rev-parse --short HEAD)
GITBRANCH?=$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
BUILD_TIME := $(shell TZ=UTC date "+%Y-%m-%d_%H:%M_UTC")
BUILD_INFO := \
  -X main.Platform=$(GOOS) \
  -X main.GitCommit=$(GITCOMMIT) \
  -X main.GitBranch=$(GITBRANCH) \
  -X main.BuildTime=$(BUILD_TIME)


# Flags for production build
BUILD_FLAGS := -ldflags="-s -w $(BUILD_INFO)" -trimpath -buildvcs=false -tags=performance

# Flags for test build (debugging, race detection, and more)
TEST_BUILD_FLAGS := -ldflags="$(BUILD_INFO)" -gcflags=all="-N -l" -trimpath -race -tags=debug

# Default target: build for production
all: build

# Build production binary (optimized for runtime speed and size)
build:
	go build $(BUILD_FLAGS) -o bin/pcsm .

# Build test binary with race detection and debugging enabled
test-build:
	go build $(TEST_BUILD_FLAGS) -o bin/pcsm_test .

# Run tests with race detection
test:
	go test -race ./...

pytest:
	poetry run pytest

lint:
	golangci-lint run

pcsm-run: build
	./bin/pcsm --source=$(SOURCE) --target=$(TARGET) --log-level=debug --reset-state

pcsm-start: build
	./bin/pcsm --source=$(SOURCE) --target=$(TARGET) --log-level=debug --reset-state --start

# Clean generated files
clean:
	rm -rf bin/*
	go clean -cache -testcache

# Start Prometheus + Grafana metrics stack (Grafana: http://localhost:3000, Prometheus: http://localhost:9090)
metrics-up:
	docker compose -f hack/metrics/docker-compose.yml up -d

# Stop metrics stack
metrics-down:
	docker compose -f hack/metrics/docker-compose.yml down

.PHONY: all build test-build test pcsm-start clean metrics-up metrics-down metrics-logs
