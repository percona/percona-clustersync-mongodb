GOOS?=$(shell go env GOOS)
GIT_COMMIT?=$(shell git rev-parse --short HEAD)
GIT_BRANCH?=$(shell git rev-parse --abbrev-ref HEAD 2>/dev/null)
BUILD_TIME := $(shell TZ=UTC date "+%Y-%m-%d_%H:%M_UTC")
BUILD_INFO := \
  -X main.Platform=$(GOOS) \
  -X main.GitCommit=$(GIT_COMMIT) \
  -X main.GitBranch=$(GIT_BRANCH) \
  -X main.BuildTime=$(BUILD_TIME)


# Flags for production build
BUILD_FLAGS := -ldflags="-s -w $(BUILD_INFO)" -trimpath -buildvcs=false -tags=performance

# Flags for test build (debugging, race detection, and more)
TEST_BUILD_FLAGS := -ldflags="$(BUILD_INFO)" -gcflags=all="-N -l" -trimpath -race -tags=debug

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
