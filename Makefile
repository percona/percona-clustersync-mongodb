# Flags for production build
BUILD_FLAGS := -ldflags="-s -w" -trimpath -buildvcs=false -tags=performance

# Flags for test build (debugging, race detection, and more)
TEST_BUILD_FLAGS := -race -gcflags=all="-N -l" -trimpath -tags=debug

# Default target: build for production
all: build

# Build production binary (optimized for runtime speed and size)
build:
	go build $(BUILD_FLAGS) -o bin/percona-mongolink .

# Build test binary with race detection and debugging enabled
test-build:
	go build $(TEST_BUILD_FLAGS) -o bin/percona-mongolink_test .

# Run tests with race detection
test:
	go test -race ./...

pytest:
	poetry run pytest

lint:
	golangci-lint run

# Clean generated files
clean:
	rm -rf bin/*
	go clean -cache -testcache

.PHONY: all build test-build test clean
