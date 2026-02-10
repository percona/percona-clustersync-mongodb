# AGENTS.md - Percona ClusterSync for MongoDB

Percona ClusterSync for MongoDB (PCSM) replicates data between MongoDB clusters. Supports replica sets and sharded clusters with initial cloning and continuous change replication.

## Prerequisites

### /etc/hosts

The MongoDB containers use hostnames that must resolve on the host. Add these entries to `/etc/hosts`:

```
# RS topology
127.0.0.1 rs00 rs01 rs02 rs10 rs11 rs12
# Sharded topology
127.0.0.1 src-rs00 src-rs10 src-rs20 src-cfg0 src-mongos
127.0.0.1 tgt-rs00 tgt-rs10 tgt-rs20 tgt-cfg0 tgt-mongos
```

### Tools

- Docker and Docker Compose
- Go (for building the binary)
- Python 3.13+ and [Poetry](https://python-poetry.org/) (for E2E tests and monitoring scripts)

Install Python dependencies:

```bash
poetry install
```

### Environment Variables

| Variable        | Default | Description                                            |
| --------------- | ------- | ------------------------------------------------------ |
| `MONGO_VERSION` | `8.0`   | MongoDB image version for test clusters                |
| `SRC_SHARDS`    | `2`     | Number of source shards (sharded topology only, max 3) |
| `TGT_SHARDS`    | `2`     | Number of target shards (sharded topology only, max 3) |

## Cluster Topology

### Connection URIs

| Topology | Source URI                   | Target URI                   |
| -------- | ---------------------------- | ---------------------------- |
| Sharded  | `mongodb://src-mongos:27017` | `mongodb://tgt-mongos:29017` |
| RS       | `mongodb://rs00:30000`       | `mongodb://rs10:30100`       |

### Health Verification

After starting clusters, verify they are healthy:

**Sharded clusters:**

```bash
docker exec src-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
docker exec tgt-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
```

**RS clusters:**

```bash
docker exec rs00 mongosh --port 30000 --quiet --eval "rs.status().ok"
docker exec rs10 mongosh --port 30100 --quiet --eval "rs.status().ok"
```

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
3. Starts HTTP server on `localhost:<port>` (default 2242)
4. Exposes REST endpoints: `/status`, `/start`, `/pause`, `/resume`, `/finalize`

The server holds MongoDB connections and manages the replication state machine.

### Client Mode (Subcommands)

Running `pcsm start`, `pcsm pause`, etc. operates as an **HTTP client**:

1. Creates `PCSMClient` with just the port number
2. Constructs HTTP request with JSON body
3. Sends request to the already-running server
4. Prints JSON response to stdout

## Manual Testing

### Step-by-Step Runbook

Sharded topology shown as primary. For RS, substitute URIs from the Connection URIs table above.

1. **Clean up previous state**:

    ```bash
    ./hack/cleanup.sh
    ```

2. **Start clusters**:

    ```bash
    ./hack/sh/run.sh     # Sharded (primary)
    # OR
    ./hack/rs/run.sh     # Replica set (alternative)
    ```

3. **Verify cluster health** (see Health Verification section above)

4. **Build PCSM**:

    ```bash
    make build
    ```

5. **Start PCSM server** (runs in foreground — use a separate terminal):

    ```bash
    ./bin/pcsm --source="mongodb://src-mongos:27017" --target="mongodb://tgt-mongos:29017" --reset-state --log-level=debug
    ```

    For RS topology:

    ```bash
    ./bin/pcsm --source="mongodb://rs00:30000" --target="mongodb://rs10:30100" --reset-state --log-level=debug
    ```

    Expected output: `Starting HTTP server at http://localhost:2242`

6. **Trigger replication** (in another terminal):

    ```bash
    curl -s -X POST http://localhost:2242/start -H 'Content-Type: application/json' -d '{}'
    ```

7. **Check status**:

    ```bash
    curl -s http://localhost:2242/status | jq .
    ```

8. **Finalize replication**:

    ```bash
    curl -s -X POST http://localhost:2242/finalize -H 'Content-Type: application/json' -d '{}'
    ```

9. **Clean up**:

    ```bash
    ./hack/cleanup.sh
    ```

### HTTP API

| Endpoint    | Method | Purpose                |
| ----------- | ------ | ---------------------- |
| `/status`   | GET    | Get replication status |
| `/start`    | POST   | Start replication      |
| `/pause`    | POST   | Pause replication      |
| `/resume`   | POST   | Resume replication     |
| `/finalize` | POST   | Finalize replication   |
| `/metrics`  | GET    | Prometheus metrics     |

### PCSM State Machine

```
idle → running → finalizing → finalized
         ↓  ↑
        paused
         ↓
        failed
```

| State        | Description                                                       |
| ------------ | ----------------------------------------------------------------- |
| `idle`       | Server started, waiting for `/start` command                      |
| `running`    | Replication active (cloning or change stream replication)         |
| `paused`     | Replication paused, can be resumed                                |
| `finalizing` | Final catchup in progress after `/finalize` command               |
| `finalized`  | Replication complete, target is caught up                         |
| `failed`     | Error occurred, check logs. Use `/resume --from-failure` to retry |

### Status Response Fields

Key fields in the `/status` response:

| Field                        | Type   | Description                                           |
| ---------------------------- | ------ | ----------------------------------------------------- |
| `state`                      | string | Current PCSM state (see table above)                  |
| `initialSync.completed`      | bool   | Whether initial clone + catchup is fully done         |
| `initialSync.cloneCompleted` | bool   | Whether bulk data clone is finished                   |
| `lagTimeSeconds`             | int    | How far target is behind source (in logical seconds)  |
| `eventsRead`                 | int    | Change stream events read from source                 |
| `eventsApplied`              | int    | Events applied to target                              |
| `lastReplicatedOpTime`       | object | Last operation timestamp applied (`ts` and `isoDate`) |

## Python E2E Tests

### Environment Variables

| Variable          | Required | Description                                         |
| ----------------- | -------- | --------------------------------------------------- |
| `TEST_SOURCE_URI` | Yes      | MongoDB URI for source cluster                      |
| `TEST_TARGET_URI` | Yes      | MongoDB URI for target cluster                      |
| `TEST_PCSM_URL`   | Yes      | PCSM HTTP server URL (e.g. `http://localhost:2242`) |
| `TEST_PCSM_BIN`   | Optional | Path to PCSM binary for auto-managed mode           |

When `TEST_PCSM_BIN` is set, pytest automatically starts and stops the PCSM server process. You do NOT need to start the PCSM server manually. `TEST_PCSM_URL` is still required — pytest uses it to communicate with the auto-started server.

### Running E2E Tests (Sharded)

```bash
# 1. Start sharded clusters
./hack/sh/run.sh

# 2. Build PCSM binary
make build

# 3. Set environment variables
export TEST_SOURCE_URI="mongodb://src-mongos:27017"
export TEST_TARGET_URI="mongodb://tgt-mongos:29017"
export TEST_PCSM_URL="http://localhost:2242"
export TEST_PCSM_BIN="./bin/pcsm"

# 4. Run all E2E tests
make pytest
```

For RS topology, substitute the URIs:

```bash
export TEST_SOURCE_URI="mongodb://rs00:30000"
export TEST_TARGET_URI="mongodb://rs10:30100"
```

Run tests including slow tests (disabled by default):

```bash
poetry run pytest --runslow
```

## Monitoring & Debugging

### Change Stream Watcher

Watch MongoDB change stream events in real-time. Useful for debugging replication event flow:

```bash
poetry run python hack/change_stream.py -u "mongodb://src-mongos:27017"
poetry run python hack/change_stream.py -u "mongodb://tgt-mongos:29017" --show-checkpoints
```

For RS topology:

```bash
poetry run python hack/change_stream.py -u "mongodb://rs00:30000"
```

### Write Operations Monitor

Monitor write operations per second on a cluster:

```bash
poetry run python hack/monitor_writes.py -u "mongodb://src-mongos:27017"
```

## External References

| Project            | Repository                                    | Documentation                                               |
| ------------------ | --------------------------------------------- | ----------------------------------------------------------- |
| PCSM Docs          | <https://github.com/percona/pcsm-docs>        | <https://docs.percona.com/percona-clustersync-for-mongodb/> |
| PSMDB Testing (QA) | <https://github.com/Percona-QA/psmdb-testing> | -                                                           |
