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

Start local MongoDB clusters for testing:

```bash
./hack/rs/run.sh     # Start replica set clusters (rs0, rs1)
./hack/sh/run.sh     # Start sharded clusters
```

**IMPORTANT**: Before running E2E tests or testing any PCSM binary functionality manually, you MUST start the MongoDB containers using the scripts above. The binary requires running source and target MongoDB clusters to function.

Single test:

- `go test -race -run TestName ./package`
- `poetry run pytest tests/test_file.py::test_name` (requires MongoDB containers running)
- manually execute binary from `./bin` for cases not covered by tests (requires MongoDB containers running)

Cleanup test environments:

```bash
./hack/cleanup.sh       # Clean all environments (rs, sh)
./hack/cleanup.sh rs    # Clean replica sets only
./hack/cleanup.sh sh    # Clean sharded cluster only
```

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

## CLI Architecture (Server + Client)

PCSM uses a single binary that operates in two modes:

### Server Mode (Root Command)

Running `pcsm --source <uri> --target <uri>` starts a **long-running server process**:

1. Connects to source and target MongoDB clusters
2. Creates `pcsm.PCSM` instance with real MongoDB clients
3. Starts HTTP server on `localhost:<port>` (default 27018)
4. Exposes REST endpoints: `/status`, `/start`, `/pause`, `/resume`, `/finalize`

The server holds MongoDB connections and manages the replication state machine.

### Client Mode (Subcommands)

Running `pcsm start`, `pcsm pause`, etc. operates as an **HTTP client**:

1. Creates `PCSMClient` with just the port number
2. Constructs HTTP request with JSON body
3. Sends request to the already-running server
4. Prints JSON response to stdout

## External References

| Project            | Repository                                    | Documentation                                               |
| ------------------ | --------------------------------------------- | ----------------------------------------------------------- |
| PCSM Docs          | <https://github.com/percona/pcsm-docs>        | <https://docs.percona.com/percona-clustersync-for-mongodb/> |
| PSMDB Testing (QA) | <https://github.com/Percona-QA/psmdb-testing> | -                                                           |
