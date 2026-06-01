# AGENTS.md - Percona ClusterSync for MongoDB

Percona ClusterSync for MongoDB (PCSM) replicates data between MongoDB clusters. Supports replica sets (RS) and sharded clusters with initial cloning and continuous change replication.

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
- Go 1.25.0+
- Python 3.13+ and [Poetry](https://python-poetry.org/) (for E2E tests and monitoring scripts)

Install Python dependencies:

```bash
poetry install
```

If `poetry run` fails with a "bad interpreter" error (e.g. after a Python version change), use the virtualenv directly: `.venv/bin/pytest` instead of `poetry run pytest`.

### Test Cluster Environment Variables

| Variable            | Default | Description                                        |
| ------------------- | ------- | -------------------------------------------------- |
| `MONGO_VERSION`     | `8.0`   | Fallback MongoDB image version for both clusters   |
| `SRC_MONGO_VERSION` | unset   | Source MongoDB image version override              |
| `TGT_MONGO_VERSION` | unset   | Target MongoDB image version override              |
| `SRC_SHARDS`        | `2`     | Source shard count (sharded topology only, max 3)  |
| `TGT_SHARDS`        | `2`     | Target shard count (sharded topology only, max 3)  |

Per-side precedence: `SRC_MONGO_VERSION` / `TGT_MONGO_VERSION` → `MONGO_VERSION` → `8.0`.

`MONGO_VERSION` also selects the testcontainers image used by `make test-integration`.

### PCSM Runtime Environment Variables

Common runtime knobs. Every flag has a matching `PCSM_*` env var.

| Env var                          | Flag                          |
| -------------------------------- | ----------------------------- |
| `PCSM_SOURCE_URI`                | `--source`                    |
| `PCSM_TARGET_URI`                | `--target`                    |
| `PCSM_PORT`                      | `--port` (default `2242`)     |
| `PCSM_LOG_LEVEL`                 | `--log-level`                 |
| `PCSM_MONGODB_OPERATION_TIMEOUT` | `--mongodb-operation-timeout` |
| `PCSM_CLONE_SEGMENT_SIZE`        | `--clone-segment-size`        |

Full list lives in [config/config.go](config/config.go).

## Tested MongoDB Versions

CI covers these source → target pairs for both RS and sharded topologies.

| Source | Target |
| ------ | ------ |
| 6.0    | 6.0    |
| 7.0    | 7.0    |
| 8.0    | 8.0    |
| 6.0    | 7.0    |
| 6.0    | 8.0    |
| 7.0    | 8.0    |

Runtime compatibility check (`mdb.CheckVersionCompat`) is major-only. Source major must be less than or equal to target major. No minor/patch enforcement, no allowlist. Combinations outside the tested matrix may start but are not validated.

## Cluster Topology

### Connection URIs

| Topology | Source URI                   | Target URI                   |
| -------- | ---------------------------- | ---------------------------- |
| Sharded  | `mongodb://src-mongos:27017` | `mongodb://tgt-mongos:29017` |
| RS       | `mongodb://rs00:30000`       | `mongodb://rs10:30100`       |

Use these URIs exactly as shown. PCSM strips unsupported URI options (including `directConnection`) and discovers topology through the MongoDB driver. `replicaSet` is accepted by the sanitizer but unnecessary for the local URIs.

The target sharded URI uses host port `29017`. Inside `tgt-mongos`, `mongosh` connects on container port `27017`.

### Health Verification

Sharded clusters:

```bash
docker exec src-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
docker exec tgt-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
```

RS clusters:

```bash
docker exec rs00 mongosh --port 30000 --quiet --eval "rs.status().ok"
docker exec rs10 mongosh --port 30100 --quiet --eval "rs.status().ok"
```

## Commands

| Command                 | Purpose                                                         |
| ----------------------- | --------------------------------------------------------------- |
| `make build`            | Production build                                                |
| `make test-build`       | Debug build with race detection                                 |
| `make test`             | Go unit tests with `-race`                                      |
| `make test-integration` | Catalog integration tests via testcontainers (requires Docker)  |
| `make lint`             | golangci-lint checks                                            |
| `make lint-py`          | Python lint via ruff                                            |
| `make fmt-py`           | Format Python tests and hack scripts                            |
| `make pytest`           | Python E2E tests (slow tests skipped by default)                |
| `make metrics-up`       | Start Prometheus + Grafana stack                                |
| `make metrics-down`     | Stop Prometheus + Grafana stack                                 |
| `make clean`            | Remove binaries and Go caches                                   |

Start local MongoDB clusters:

```bash
./hack/rs/run.sh     # Start RS clusters (rs0, rs1)
./hack/sh/run.sh     # Start sharded clusters
```

Local PCSM runs require source and target MongoDB clusters to be up.

Single test invocations:

- `go test -race -run TestName ./package` (unit tests)
- `go test -v -tags integration -run TestName ./pcsm/catalog/...` (integration, requires Docker)
- `.venv/bin/pytest tests/test_documents.py::test_insert_one` (E2E, requires clusters and `TEST_*` env vars)
- manually execute the binary from `./bin` for cases not covered by tests

Cleanup test environments:

```bash
./hack/cleanup.sh       # Clean rs, sh, and sh-ha if present
./hack/cleanup.sh rs    # RS only
./hack/cleanup.sh sh    # Sharded only
```

Always run `./hack/cleanup.sh` (no arguments) before switching between RS and sharded topologies. Both topologies bind overlapping ports (e.g. 30000), so leftover containers cause "port already allocated" errors. If cleanup doesn't resolve port conflicts, check for orphans with `docker ps -a`.

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
- Run Go unit tests with `-race` (the `make test` target sets this)
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

Run before declaring code changes complete:

1. `make lint`
2. `make test`
3. `make test-integration`
4. `make pytest`

## Package Structure

| Package         | Purpose                                          |
| --------------- | ------------------------------------------------ |
| root            | CLI entrypoint, HTTP server, client commands     |
| `config/`       | Configuration loading and validation             |
| `errors/`       | Project error helpers                            |
| `log/`          | Logging wrapper and log attributes               |
| `mdb/`          | MongoDB connections, topology, version checks    |
| `metrics/`      | Prometheus metrics                               |
| `pcsm/`         | Replication state machine                        |
| `pcsm/catalog/` | Target collections and indexes                   |
| `pcsm/clone/`   | Initial data clone                               |
| `pcsm/repl/`    | Change stream replication                        |
| `sel/`          | Namespace filtering                              |
| `util/`         | Context helpers                                  |
| `tests/`        | Python E2E tests                                 |

## CLI Architecture

PCSM is a single binary with two operating modes plus a few direct commands.

### Server mode

`pcsm --source <uri> --target <uri>` starts a long-running server:

1. Connects to source and target clusters
2. Creates the `pcsm.PCSM` instance
3. Starts HTTP server on `localhost:<port>` (default `2242`)
4. Exposes operational endpoints `/status`, `/start`, `/pause`, `/resume`, `/finalize`, metrics at `/metrics`, and Go pprof under `/debug/pprof/`

### Client subcommands

`status`, `start`, `pause`, `resume`, and `finalize` act as HTTP clients against an already-running server.

### Direct commands

- `pcsm reset`, `pcsm reset recovery`, `pcsm reset heartbeat` connect directly to the target cluster and clear persisted state. Do not run while a server is up.
- `pcsm version` prints local build metadata.

## Manual Testing

### Step-by-Step Runbook

Sharded topology shown as primary. RS variants use the URIs from the Connection URIs table.

1. **Clean up previous state**:

    ```bash
    ./hack/cleanup.sh
    ```

2. **Start clusters**:

    ```bash
    ./hack/sh/run.sh     # Sharded (primary)
    # OR
    ./hack/rs/run.sh     # RS (alternative)
    ```

3. **Verify cluster health** (see Health Verification)

4. **Build PCSM**:

    ```bash
    make build
    ```

5. **Start PCSM server** (runs in foreground, use a separate terminal):

    ```bash
    ./bin/pcsm --source="mongodb://src-mongos:27017" --target="mongodb://tgt-mongos:29017" --reset-state --log-level=debug
    ```

    For RS topology:

    ```bash
    ./bin/pcsm --source="mongodb://rs00:30000" --target="mongodb://rs10:30100" --reset-state --log-level=debug
    ```

    Expected output: `Starting HTTP server at http://localhost:2242`

6. **Trigger replication**:

    ```bash
    curl -s -X POST http://localhost:2242/start -H 'Content-Type: application/json' -d '{}'
    ```

7. **Check status**:

    ```bash
    curl -s http://localhost:2242/status | jq .
    ```

8. **Wait for initial sync to complete** before finalizing:

    ```bash
    until curl -s http://localhost:2242/status | jq -e '.initialSync.completed == true' >/dev/null; do sleep 1; done
    ```

9. **Finalize replication**:

    ```bash
    curl -s -X POST http://localhost:2242/finalize -H 'Content-Type: application/json' -d '{}'
    ```

10. **Clean up**:

     ```bash
     ./hack/cleanup.sh
     ```

### HTTP API

| Endpoint          | Method | Purpose                |
| ----------------- | ------ | ---------------------- |
| `/status`         | GET    | Get replication status |
| `/start`          | POST   | Start replication      |
| `/pause`          | POST   | Pause replication      |
| `/resume`         | POST   | Resume replication     |
| `/finalize`       | POST   | Finalize replication   |
| `/metrics`        | GET    | Prometheus metrics     |
| `/debug/pprof/*`  | GET    | Go pprof endpoints     |

`/start` accepts an optional JSON body with namespace include/exclude lists, clone tuning, repl tuning, and a bulk-write override. See `startRequest` in [main.go](main.go) and the matching CLI flags.

### PCSM State Machine

```
idle ──start──> running ──finalize──> finalizing ──> finalized
                  │ ▲
                  │ └─ resume ─ paused
                  │
                  └─ error ──> failed
```

Additional transitions not shown:

- `finalized → running` via `/start` (starts a new run)
- `failed → running` via `/resume` with `fromFailure: true` (CLI: `pcsm resume --from-failure`)

| State        | Description                                                               |
| ------------ | ------------------------------------------------------------------------- |
| `idle`       | Server started, waiting for `/start`                                      |
| `running`    | Replication active (cloning or change stream replication)                 |
| `paused`     | Replication paused, can be resumed                                        |
| `finalizing` | Final catchup in progress after `/finalize`                               |
| `finalized`  | Replication complete, target caught up                                    |
| `failed`     | Error occurred. Resume with `pcsm resume --from-failure` or `POST /resume` with `{"fromFailure": true}` after checking logs |

### Status Response Fields

| Field                                 | Type   | Description                                      |
| ------------------------------------- | ------ | ------------------------------------------------ |
| `ok`                                  | bool   | Request success flag                             |
| `error`                               | string | Error message when `ok=false`                    |
| `state`                               | string | Current PCSM state                               |
| `info`                                | string | Human-readable state detail                      |
| `lagTimeSeconds`                      | int64  | Logical lag between source and target            |
| `eventsRead`                          | int64  | Change stream events read (excluding tick events) |
| `eventsApplied`                       | int64  | Events applied to target                         |
| `lastReplicatedOpTime.ts`             | string | Last applied operation timestamp                 |
| `lastReplicatedOpTime.isoDate`        | string | Last applied operation time as RFC3339           |
| `initialSync.completed`               | bool   | Initial clone and catchup completed              |
| `initialSync.cloneCompleted`          | bool   | Bulk clone completed                             |
| `initialSync.lagTimeSeconds`          | int64  | Initial sync lag                                 |
| `initialSync.estimatedCloneSizeBytes` | uint64 | Estimated clone size                             |
| `initialSync.clonedSizeBytes`         | uint64 | Bytes cloned so far                              |
| `finalization.completed`              | bool   | Finalize completed successfully                  |
| `finalization.startedAt`              | time   | Finalize start time, when available              |
| `finalization.completedAt`            | time   | Finalize completion time, when available         |
| `finalization.unsuccessfulIndexes[]`  | array  | Indexes that did not complete cleanly            |

## Python E2E Tests

### Environment Variables

| Variable          | Required | Description                                         |
| ----------------- | -------- | --------------------------------------------------- |
| `TEST_SOURCE_URI` | Yes      | MongoDB URI for source cluster                      |
| `TEST_TARGET_URI` | Yes      | MongoDB URI for target cluster                      |
| `TEST_PCSM_URL`   | Yes      | PCSM HTTP server URL (e.g. `http://localhost:2242`) |
| `TEST_PCSM_BIN`   | Optional | Path to PCSM binary for auto-managed mode           |

When `TEST_PCSM_BIN` is set, pytest starts and stops the PCSM server itself. `TEST_PCSM_URL` is still required so pytest can reach the managed server.

Each env var has a matching pytest option that wins when set: `--source-uri`, `--target-uri`, `--pcsm_url`, `--pcsm-bin`.

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

# 4. Run the default E2E suite (slow tests skipped)
make pytest
```

For RS, use the RS URIs from the Connection URIs table.

Run including slow tests:

```bash
.venv/bin/pytest --runslow
```

Note: local `make pytest` and the GitHub Actions matrix do not cover identical scope. CI runs selected files for RS and a single deselect for sharded. Local runs default to the full discovered suite minus slow tests.

## Monitoring & Debugging

### Change Stream Watcher

```bash
poetry run python hack/change_stream.py -u "mongodb://src-mongos:27017"
poetry run python hack/change_stream.py -u "mongodb://tgt-mongos:29017" --show-checkpoints
poetry run python hack/change_stream.py -u "mongodb://rs00:30000"
```

### Write Operations Monitor

```bash
poetry run python hack/monitor_writes.py -u "mongodb://src-mongos:27017"
```

### Metrics Stack

`make metrics-up` brings up the bundled Prometheus + Grafana stack against the local PCSM `/metrics` endpoint. `make metrics-down` stops it.

## CI

### GitHub Actions

| Workflow                          | Purpose                                                                        |
| --------------------------------- | ------------------------------------------------------------------------------ |
| `.github/workflows/go.yml`        | Go unit tests, formatter suggestions, golangci-lint, catalog integration tests |
| `.github/workflows/e2etests.yml`  | Local RS and sharded E2E matrix across 6.0, 7.0, 8.0 and lower-to-higher pairs |
| `.github/workflows/ci.yml`        | Percona-QA `psmdb-testing` functional suite across PSMDB 6.0, 7.0, 8.0         |

`ci.yml` ignores PR changes confined to `tests/**` and `packaging/**`.

### Jenkins (operationally known, repo-unverified)

Functional tests run at `https://psmdb.cd.percona.com/view/PCSM/`.

| Job                             | Default Cloud | Fallback |
| ------------------------------- | ------------- | -------- |
| `hetzner-pcsm-functional-tests` | Hetzner       | AWS      |

Hetzner workers are used by default for cost reasons but sometimes fail to start due to cloud capacity limits. If workers don't start, cancel the stuck build and re-trigger with **AWS** (first cloud option in job parameters).

## QA Test Branch Override

`.github/workflows/ci.yml` selects the `Percona-QA/psmdb-testing` branch in this order:

1. `tests_ver` workflow dispatch input
2. First `PCSM-[0-9]+` key in the PR title, if a matching branch exists in `Percona-QA/psmdb-testing`
3. `main`

Push test fixes to a branch named after the ticket (e.g. `PCSM-286`) in the QA repo and the PCSM CI picks them up. No PR needed on the QA repo, just the branch.

## External References

| Project            | Repository                                    | Documentation                                               |
| ------------------ | --------------------------------------------- | ----------------------------------------------------------- |
| PCSM Docs          | <https://github.com/percona/pcsm-docs>        | <https://docs.percona.com/percona-clustersync-for-mongodb/> |
| PSMDB Testing (QA) | <https://github.com/Percona-QA/psmdb-testing> | -                                                           |
