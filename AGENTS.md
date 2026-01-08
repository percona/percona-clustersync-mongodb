# AGENTS.md - Percona ClusterSync for MongoDB

Percona ClusterSync for MongoDB (PCSM) replicates data between MongoDB clusters. Supports replica sets and sharded clusters with initial cloning and continuous change replication.

## Commands

```bash
make build       # Production build
make test-build  # Debug build with race detection
make test        # Run Go tests with race detection
make lint        # Run golangci-lint (formats code automatically)
make pytest      # Run Python E2E tests
make clean       # Remove binaries and caches
```

Single test:

- `go test -race -run TestName ./package`

- poetry run pytest tests/test_file.py::test_name

## Project-Specific Patterns

### Error Handling

Use `github.com/percona/percona-clustersync-mongodb/errors` (not stdlib):

```go
errors.Wrap(err, "context")   // Returns nil if err is nil
errors.Wrapf(err, "fmt %s", v)
errors.Join(err1, err2)
```

Skip wrapping with `//nolint:wrapcheck` when returning driver errors directly.

### Context Timeouts

Use `github.com/percona/percona-clustersync-mongodb/util`:

```go
util.CtxWithTimeout(ctx, config.Timeout, fn)
```

### Logging

```go
lg := log.New("scope")
lg.Info("message")
lg.Error(err, "context")
lg.With(log.Elapsed(d), log.NS(db, coll)).Info("done")
log.Ctx(ctx).Info("message")  // From context
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

## Verification Requirements

When modifying code, always finish with:

1. `make lint` - must pass with no new issues
2. `make test` - all Go tests must pass
3. `make pytest` - all E2E tests must pass (if available)

These checks must be the final tasks before considering work complete.

## Package Structure

| Package   | Purpose                          |
| --------- | -------------------------------- |
| `pcsm/`   | Core replication (Clone, Repl)   |
| `topo/`   | MongoDB topology and connections |
| `sel/`    | Namespace filtering              |
| `config/` | Configuration constants          |
| `errors/` | Custom error handling            |
| `log/`    | Logging utilities                |
| `util/`   | Context helpers                  |
| `tests/`  | Python E2E tests                 |
