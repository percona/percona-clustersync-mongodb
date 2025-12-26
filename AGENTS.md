# AGENTS.md - Percona ClusterSync for MongoDB

Guidelines for AI coding agents working in this repository.

## Project Overview

Percona ClusterSync for MongoDB (PCSM) is a tool for cloning and replicating data between MongoDB clusters. It supports both replica sets and sharded clusters, handling initial data cloning followed by continuous change replication.

## Build Commands

```bash
make build          # Production build (optimized, stripped symbols)
make test-build     # Debug build with race detection
make clean          # Remove binaries and clear caches
```

Build flags:
- Production: `-ldflags="-s -w"`, `-trimpath`, `-tags=performance`
- Debug: `-gcflags=all="-N -l"`, `-race`, `-tags=debug`

## Lint Commands

```bash
make lint           # Run golangci-lint (v2 config)
golangci-lint run   # Direct invocation
```

The project uses golangci-lint v2 with most linters enabled. Key disabled linters:
- `cyclop`, `funlen`, `gocognit` - complexity limits
- `exhaustruct` - struct field exhaustiveness
- `varnamelen`, `wsl` - style preferences

Formatters enabled: `gofmt`, `gofumpt`, `goimports`

## Test Commands

### Go Tests

```bash
go test -race ./...                        # Run all tests
go test -race -run TestName ./package      # Single test
go test -race -v -run TestFilter ./sel     # Verbose single test
```

### Python Tests (E2E)

```bash
poetry run pytest                                      # All tests
poetry run pytest tests/test_file.py::test_name       # Single test
poetry run pytest --runslow                            # Include slow tests
```

Python tests require: `TEST_SOURCE_URI`, `TEST_TARGET_URI`, `TEST_PCSM_URL` env vars.

## Code Style Guidelines

### Imports

Group imports: stdlib, third-party, local (separated by blank lines):
```go
import (
    "context"

    "go.mongodb.org/mongo-driver/v2/bson"

    "github.com/percona/percona-clustersync-mongodb/errors"
)
```

### Error Handling

Use the custom `errors` package (not stdlib `errors`):
```go
errors.New("message")
errors.Wrap(err, "context")      // Returns nil if err is nil
errors.Wrapf(err, "fmt %s", val)
errors.Join(err1, err2)
errors.Is(err, target)
```
When wrapping is not needed (e.g., returning driver errors directly), use `//nolint:wrapcheck`.

### Naming Conventions

- **Unexported**: `camelCase` (e.g., `sourceCluster`, `nsFilter`)
- **Exported**: `PascalCase` (e.g., `CloneStatus`, `NewRepl`)
- **Acronyms**: Stay uppercase (e.g., `URI`, `HTTP`, `PCSM`, `ID`)

### Context Usage

- Pass `context.Context` as the first parameter
- Use `util.CtxWithTimeout` for timeout-scoped operations:
```go
err := util.CtxWithTimeout(ctx, config.DisconnectTimeout, client.Disconnect)
```

### Logging

Use the custom `log` package:
```go
lg := log.New("scope-name")
lg.Info("message")
lg.Error(err, "context message")
lg.With(log.Elapsed(duration), log.NS(db, coll)).Info("message")
log.Ctx(ctx).Info("message")  // From context
```

### Comments and Documentation

Document all exported types/functions with godoc format. Start with the name:
```go
// PCSM manages the replication process.
type PCSM struct { ... }
```

### Struct Tags

Use standard Go struct tags for BSON and JSON:
```go
type checkpoint struct {
    NSInclude []string `bson:"nsInclude,omitempty"`
    State     State    `bson:"state"`
}
```

### Testing Patterns

- Use `testify` for assertions (`assert`, `require`)
- Always run tests with `-race` flag
- Use table-driven tests for multiple cases
- Use `t.Parallel()` when tests are independent
```go
func TestFilter(t *testing.T) {
    t.Parallel()
    tests := []struct {
        name     string
        expected bool
    }{
        {"case 1", true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            assert.Equal(t, tt.expected, result)
        })
    }
}
```

### Nolint Directives

Use sparingly with justification. Common cases:
- `//nolint:wrapcheck` - returning unwrapped driver errors intentionally
- `//nolint:gochecknoglobals` - for CLI command variables (cobra)
- `//nolint:err113` - in custom error package functions
- `//nolint:gosec` - for safe integer conversions with bounds checking

### Formatting

- Use `gofmt` and `gofumpt` for formatting
- Use tabs for indentation
- Use blank lines to separate logical blocks

## Package Structure

- `pcsm/` - Core replication logic (Clone, Repl, Catalog)
- `topo/` - MongoDB topology and connection utilities
- `sel/` - Namespace selection/filtering
- `config/` - Configuration constants and environment variables
- `errors/` - Custom error handling
- `log/` - Logging utilities
- `metrics/` - Prometheus metrics
- `util/` - General utilities
- `tests/` - Python E2E tests

## Environment Variables

- `PCSM_SOURCE_URI` - Source MongoDB connection string
- `PCSM_TARGET_URI` - Target MongoDB connection string
- `PCSM_PORT` - HTTP server port (default: 2242)
- `PCSM_USE_COLLECTION_BULK_WRITE` - Use collection bulk write API
- `PCSM_CLONE_NUM_PARALLEL_COLLECTIONS` - Parallel collection cloning
- `PCSM_CLONE_NUM_READ_WORKERS` - Read workers for cloning
- `PCSM_CLONE_NUM_INSERT_WORKERS` - Insert workers for cloning
