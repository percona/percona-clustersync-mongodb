# PROJECT KNOWLEDGE BASE

Generated: 2026-09-18T15:07:27Z | Commit: af75d41 | Branch: per_dir_agents_md_files

## OVERVIEW

Percona ClusterSync for MongoDB (PCSM) is a Go service for initial cloning and continuous replication between replica sets or sharded clusters. One binary provides the HTTP server, HTTP client commands, and direct target-state maintenance; MongoDB-backed HA is always enabled.

## STRUCTURE

```text
./
|-- main.go, recovery.go     # CLI/HTTP/HA wiring and checkpoint fencing
|-- config/, ha/, mdb/       # Configuration, election, MongoDB boundary
|-- pcsm/                   # Pipeline lifecycle; catalog/, clone/, repl/
|-- errors/, log/, util/     # Shared error, logging, context conventions
|-- metrics/, sel/          # Telemetry and namespace selection
|-- hack/, tests/           # Local topologies, monitoring, Python E2E
`-- .github/, packaging/    # Root-owned CI and release automation
```

## WHERE TO LOOK

Read the applicable nested guidance before changing its domain; deeper guidance owns local invariants.

| Task | Guidance / entry point |
| --- | --- |
| CLI, HTTP requests, startup | `main.go`; `startRequest` defines `/start` input |
| Configuration flags and env | `config/AGENTS.md` |
| Election, roles, membership | `ha/AGENTS.md` |
| Driver policy and compatibility | `mdb/AGENTS.md` |
| Pipeline lifecycle and recovery | `pcsm/AGENTS.md`; root `recovery.go` |
| Target collections and indexes | `pcsm/catalog/AGENTS.md` |
| Initial data copy | `pcsm/clone/AGENTS.md` |
| Ordered replay and checkpoints | `pcsm/repl/AGENTS.md` |
| Structured logging | `log/AGENTS.md` |
| Telemetry schema | `metrics/AGENTS.md` |
| Topology, HA drills, monitoring | `hack/AGENTS.md` |
| Live-system E2E tests | `tests/AGENTS.md` |
| CI and packaging | `.github/workflows/{go,e2etests,ci}.yml`, `packaging/` |

## CODE MAP

| Symbol / boundary | Responsibility |
| --- | --- |
| `pcsm.PCSM` | Composes clone, replication, and catalog work |
| `recovery.go` | Fences checkpoint persistence against stale HA terms |
| `mdb.CheckVersionCompat` | Major-only compatibility: source major must not exceed target |
| `Membership.SetRole` | Updates HA role/term gauges and transition counter |
| `startRequest` | HTTP start parameters matching CLI replication controls |

## CONVENTIONS

- Use project `errors.Wrap`, `Wrapf`, and `Join`, not stdlib replacements; `Wrap(nil, ...)` returns nil.
- Use `util.CtxWithTimeout(ctx, config.Timeout, fn)` for bounded MongoDB operations.
- Use project structured logging (`log.New`, `log.Ctx`, `log.Elapsed`, `log.NS`); local details belong in `log/AGENTS.md`.
- Go unit tests run with `-race`; use testify and parallelize only independent cases.
- Keep `nolint` exceptions narrow and justified, including intentionally unwrapped driver errors (`wrapcheck`).
- For code changes, run `make lint`, `make test`, `make test-integration`, and `make pytest` with their prerequisites available; report blocked checks explicitly.

## ANTI-PATTERNS (THIS PROJECT)

- Never call a replication checkpoint wholly applied: it is inclusive and its timestamp may be uncommitted.
- Never run `pcsm reset`, `reset recovery`, or `reset members` while a server is running.
- Do not switch RS/sharded topologies without `./hack/cleanup.sh`: their host ports overlap.
- Do not use standby `/status` as a health probe; operational endpoints return HTTP 409 `not_active`. Use `/metrics` for HTTP liveness/readiness.
- Do not require the HA response envelope: `me`/`role`/`group` is optional when only one live member is observed or membership reads fail.
- Do not change metrics schema without updating `hack/metrics/grafana-board.json`.
- Do not infer Go test coverage from packaging builds, or equate local E2E scope with the CI-selected suite.

## UNIQUE STYLES

- `pcsm --source ... --target ...` serves HTTP on localhost port 2242 by default; `status/start/pause/resume/finalize` subcommands are HTTP clients.
- Server startup connects to both clusters even on standbys; standby source connections carry monitoring traffic, not replication reads.
- `/status` is GET; `/start`, `/pause`, `/resume`, `/finalize` are POST. `/metrics` and `/debug/pprof/*` remain available on all roles.
- Wait for `initialSync.completed`, not just `cloneCompleted`, before finalizing. Failed runs resume explicitly with `--from-failure` or `{"fromFailure":true}`.
- Local source/target URIs: RS `mongodb://rs00:30000` / `mongodb://rs10:30100`; sharded `mongodb://src-mongos:27017` / `mongodb://tgt-mongos:29017`.
- Local container names must resolve on the host. Target mongos uses host port 29017 but container port 27017. PCSM strips unsupported URI options including `directConnection` and lets the driver discover topology; `replicaSet` is accepted by the sanitizer but unnecessary for these local URIs.

## COMMANDS

| Command | Purpose / prerequisite |
| --- | --- |
| `make build` | Production binary `bin/pcsm` |
| `make test-build` | Race-enabled debug binary `bin/pcsm_test` |
| `make test` | Race-enabled Go unit tests |
| `make test-integration` | Docker/testcontainers; catalog, clone, repl, mdb, ha; integration tag, uncached, 5m timeout |
| `make lint` | Go lint |
| `make lint-py` / `make fmt-py` | Python lint / formatting |
| `make pytest` | Live-cluster E2E; slow tests skipped by default |
| `make clean` | Remove binaries and Go caches |
| `./hack/rs/run.sh` / `./hack/sh/run.sh` | Start one local topology before PCSM |
| `./hack/cleanup.sh` | Stop local environments before switching topology |
| `make metrics-up` / `make metrics-down` | Bundled Prometheus/Grafana stack |

## NOTES

- Detailed local environment, API, HA, E2E, monitoring, and CI operations follow in [OPERATIONAL REFERENCE](#operational-reference).
- Follow `CONTRIBUTING.md#python-e2e-tests` for Python setup. E2E requires `TEST_SOURCE_URI`, `TEST_TARGET_URI`, and `TEST_PCSM_URL`; `TEST_PCSM_BIN` enables managed-server mode but does not replace the URL.
- `make test-integration` also uses `MONGO_VERSION`; topology per-side overrides take precedence over that fallback. Detailed setup belongs in `hack/AGENTS.md`.
- State from 0.9.0 is incompatible with 0.10.0: stop servers and reset target state before migration.
- QA branch selection in `ci.yml`: `tests_ver` input, then the first PR-title `PCSM-[0-9]+` ticket branch if it exists in `Percona-QA/psmdb-testing`, then `main`. Push fixes directly to that ticket branch; no QA-repository PR is required. Repository: <https://github.com/Percona-QA/psmdb-testing>.
- CI tests same-major 6.0/7.0/8.0 and lower-to-higher pairs on both RS and sharded topologies; runtime acceptance outside that matrix is not validation.

## OPERATIONAL REFERENCE

Use these details when running PCSM locally or diagnosing CI. Generated material above remains the navigation layer; current-source contracts below supersede shorter summaries where they are more precise.

### Local Environment

Required tool versions:

| Tool | Version |
| --- | --- |
| Docker Engine | 29.7.2 |
| Docker Compose | 5.5.0 |
| Go | 1.27.0 |
| Python | 3.13.15 |
| Poetry | 2.4.2 |

Add Docker-advertised MongoDB names to `/etc/hosts`:

```text
127.0.0.1 rs00 rs01 rs02 rs10 rs11 rs12
127.0.0.1 src-rs00 src-rs10 src-rs20 src-cfg0 src-mongos
127.0.0.1 tgt-rs00 tgt-rs10 tgt-rs20 tgt-cfg0 tgt-mongos
```

Topology image and shard controls:

| Variable | Default | Purpose |
| --- | --- | --- |
| `MONGO_VERSION` | `8.0.29-13` | Fallback image for both clusters and testcontainers |
| `SRC_MONGO_VERSION` | unset | Source-side image override |
| `TGT_MONGO_VERSION` | unset | Target-side image override |
| `SRC_SHARDS` | `2` | Source shard count; maximum 3 |
| `TGT_SHARDS` | `2` | Target shard count; maximum 3 |

Per-side image precedence is side override, `MONGO_VERSION`, then `8.0.29-13`. Hidden `PCSM_RECOVERY_CHECKPOINT_INTERVAL` uses 15 seconds for any nonpositive value.

Runtime flag/environment bindings:

| Environment variable | Flag |
| --- | --- |
| `PCSM_SOURCE_URI` | `--source` |
| `PCSM_TARGET_URI` | `--target` |
| `PCSM_PORT` | `--port` (default 2242) |
| `PCSM_LISTEN_HOST` | `--listen-host` |
| `PCSM_LOG_LEVEL` | `--log-level` |
| `PCSM_MONGODB_OPERATION_TIMEOUT` | `--mongodb-operation-timeout` |
| `PCSM_CLONE_SEGMENT_SIZE` | `--clone-segment-size` |
| `PCSM_RECOVERY_CHECKPOINT_INTERVAL` | `--recovery-checkpoint-interval` (hidden) |

Configuration-backed options support `PCSM_*` variables; not every CLI flag does. Namespace filters and `resume --from-failure` are read directly from flags. Config fields and environment bindings live in `config/config.go`; flag declarations, defaults, and command-only options live in `main.go`.

MongoDB compatibility compares major versions only: source major must not exceed target major. There is no runtime version allowlist.

### Cluster Health and Local Server

```bash
# Sharded
docker exec src-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
docker exec tgt-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"

# Replica sets
docker exec rs00 mongosh --port 30000 --quiet --eval "rs.status().ok"
docker exec rs10 mongosh --port 30100 --quiet --eval "rs.status().ok"
```

After starting one topology and building:

```bash
# Sharded
./bin/pcsm --source="mongodb://src-mongos:27017" \
  --target="mongodb://tgt-mongos:29017" --reset-state --log-level=debug

# Replica sets
./bin/pcsm --source="mongodb://rs00:30000" \
  --target="mongodb://rs10:30100" --reset-state --log-level=debug
```

Use these URI pairs exactly. Server startup connects to both clusters, creates the `pcsm.PCSM` pipeline, settles the initial HA role, then starts HTTP. Foreground startup logs `Starting HTTP server at http://localhost:2242`; that line is emitted before successful socket binding and is not by itself a readiness signal.

Use another terminal for lifecycle requests:

```bash
curl -s -X POST http://localhost:2242/start \
  -H 'Content-Type: application/json' -d '{}'
curl -s http://localhost:2242/status | jq .
until curl -s http://localhost:2242/status | jq -e \
  '.initialSync.completed == true' >/dev/null; do sleep 1; done
curl -s -X POST http://localhost:2242/finalize \
  -H 'Content-Type: application/json' -d '{}'
until curl -s http://localhost:2242/status | jq -e \
  '.finalization.completed == true' >/dev/null; do sleep 1; done
# Stop foreground PCSM, then clean Docker environments.
```

Manual run order: clean previous Docker environments; start one topology; verify health; build; start foreground PCSM; POST `/start`; wait for initial sync; POST `/finalize`; wait for finalization; stop PCSM; run `./hack/cleanup.sh`.

### Focused Checks and Code Patterns

```bash
go test -v -tags integration -run TestName ./pcsm/catalog/...
.venv/bin/pytest tests/test_documents.py::test_insert_one

./hack/cleanup.sh             # Default order: pcsm-ha, rs, sh, optional sh-ha.
./hack/cleanup.sh rs
./hack/cleanup.sh sh
./hack/cleanup.sh pcsm-ha
docker ps -a  # Diagnose orphan containers when cleanup does not release ports.
```

Exercise the binary directly from `./bin` when no automated test covers the changed behavior.

`make lint` runs `golangci-lint run`. `make lint-py` runs Ruff check plus format-check over `tests/` and `hack/`; `make fmt-py` applies Ruff fixes and formatting to those paths. `make clean` removes `bin/*` plus Go build and test caches. `make test-integration` covers catalog, clone, repl, mdb, and ha with the integration tag, uncached runs, and a five-minute timeout.

No-argument cleanup stops HA before cluster environments. Explicit arguments retain caller order, so put `pcsm-ha` before `rs` or `sh`. Cleanup is best-effort and suppresses teardown failures; inspect remaining containers, volumes, and overlapping host port 30000 when `port already allocated` persists.

Application code imports project helpers:

```go
import (
    "github.com/percona/percona-clustersync-mongodb/errors"
    "github.com/percona/percona-clustersync-mongodb/util"
)

err0 := errors.Wrap(err, "context")
err1 := errors.Wrapf(err, "format %s", value)
err2 := errors.Join(err0, err1)
err3 := util.CtxWithTimeout(ctx, config.DisconnectTimeout, target.Disconnect)
```

`errors.Wrap` and `Wrapf` preserve nil. Use an operation-specific timeout constant; `config.Timeout` does not exist.

Use table-driven Go tests for multiple cases, `assert`/`require` from testify, and `t.Parallel()` only for independent tests. Common justified lint exceptions:

```go
for _, tt := range tests {
    t.Run(tt.name, func(t *testing.T) {
        t.Parallel()
        result := filterCompressors(tt.input)
        assert.Equal(t, tt.expected, result)
    })
}
```

| Directive | Accepted use |
| --- | --- |
| `wrapcheck` | Intentionally returning a driver error for caller classification |
| `gochecknoglobals` | Build metadata and package-level compressor allowlist |
| `err113` | Project error constructors such as `errors.New` and `Errorf` |
| `gosec` | Bounds-checked integer conversions or a documented false positive |

Keep exceptions narrow and justified as a review convention; `nolintlint` is disabled and does not enforce justifications automatically.

Logging call shapes:

```go
lg := log.New("scope")
lg.Info("message")
lg.Error(err, "context")
lg.With(log.Elapsed(d), log.NS(db, coll)).Info("done")
log.Ctx(ctx).Info("message")
```

### HA Group Drill

`hack/ha/` builds `pcsm:dev` and runs `pcsm0`, `pcsm1`, and `pcsm2` against an existing topology. Containers join that topology's Docker network and use the same MongoDB hostnames as the host. APIs bind host loopback ports 2242-2244; Prometheus reaches the instances through shared `pcsm-metrics`.

```bash
./hack/ha/run.sh rs --reset       # Or: ./hack/ha/run.sh sh --reset
./hack/ha/status-group.sh         # PORT | INSTANCE_ID | HOST | ROLE | STATE | TERM
./hack/ha/kill-active.sh          # Hard-kill ACTIVE; restart after 5s.
./hack/ha/kill-active.sh 15       # Override restart delay.
./hack/ha/kill-active.sh --no-restart
./hack/ha/status-active.sh        # Full status of observed ACTIVE.
docker logs -f pcsm0
./hack/ha/stop.sh
```

`--reset` stops the old HA group before clearing lease, member, and checkpoint state. Cleanup must stop HA containers before removing their cluster network.

The restarted process gets a new instance ID and rejoins initially as STANDBY, unless it later wins election. A five-second restart does not guarantee that another instance has already taken over because lease TTL is ten seconds. Host clients can target any published port, for example `./bin/pcsm status --port 2243`.

HA persistence and fencing contracts:

- Exactly one instance owns target lease `percona_clustersync_mongodb.lease`, becomes ACTIVE, and drives replication; other live members are STANDBY and can take over. Per-instance liveness documents live in `percona_clustersync_mongodb.members`.
- Startup performs one synchronous lease attempt before serving HTTP, so a sole instance becomes ACTIVE without a transient STANDBY API window.
- Every instance connects to source and target before election. Startup performs version and hello reads; after that, STANDBY source connections carry driver monitoring traffic but no replication reads. Unreachable or incompatible sources fail startup rather than failover, and promotion avoids connection setup latency.
- Lease terms are monotonic fencing tokens on checkpoint writes. A stale term cannot overwrite a newer checkpoint. Periodic fencing stops checkpointing and requests a best-effort pipeline pause; membership role changes remain controlled by election reconciliation.
- The five operational endpoints reject otherwise valid STANDBY requests with HTTP 409 and JSON `error: "not_active"`. Optional envelope fields include the responder's `role` and `group.members[]`, which may locate ACTIVE.
- `/status` is also ACTIVE-only because a STANDBY has no meaningful pipeline state.
- The `me`/`role`/`group` envelope appears only when more than one live member is observed. Membership-read failure or degradation to one observed member omits it; a lone instance therefore keeps the pre-HA API byte-for-byte.
- Group identity is advisory through `--group-name` / `PCSM_GROUP_NAME`. `status-active.sh` and `kill-active.sh` depend on observing the multi-member envelope and can report no ACTIVE when it is omitted.

Reset commands bypass HTTP and connect directly to the target. Version is local:

| Command | Effect |
| --- | --- |
| `pcsm reset` | Clear persisted replication state |
| `pcsm reset recovery` | Clear persisted recovery state |
| `pcsm reset members` | Clear HA membership state |
| `pcsm version` | Print local build metadata |

Stop every server before running reset commands.

For 0.9-to-0.10 migration, startup rejects a sufficiently recent legacy heartbeat as evidence of a live pre-0.10 instance; stale heartbeat markers are accepted. Stop old servers and run the full reset before upgrading. Do not describe this as universal checkpoint-format rejection: a missing checkpoint term decodes as term zero.

### State and Status Semantics

```text
idle --start--> running --finalize--> finalizing --> finalized
                   |  ^
                   |  `--resume-- paused
                   `-----error----> failed
```

| State | Meaning |
| --- | --- |
| `idle` | Server is waiting for `/start` |
| `running` | Clone or change replication is active |
| `paused` | Replication is paused and can resume |
| `finalizing` | Replication has paused; catalog/index completion is in progress |
| `finalized` | Catalog finalization finished; this does not cover source writes made after replication paused. `/start` begins a new run and returns to `running` |
| `failed` | Check logs first. Resume eligible replication-phase failures with `{"fromFailure":true}`; restart an unfinished interrupted clone with `/start` to re-clone |

`/start` accepts optional namespace include/exclude lists, clone tuning, replication tuning, and a bulk-write override. `startRequest` in `main.go` defines the HTTP body and matching CLI flags.

Operational API contract:

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/status` | GET | Pipeline status |
| `/start` | POST | Resolve options and start a run |
| `/pause` | POST | Pause running change replication, not an in-flight clone |
| `/resume` | POST | Resume paused replication or eligible explicit failure recovery |
| `/finalize` | POST | Initiate asynchronous catalog finalization after initial sync |
| `/metrics` | GET scrape surface | Prometheus metrics on every role |
| `/debug/pprof/*` | Go handlers | Profiling/debugging on every role; PCSM delegates method handling |

Wrong-method checks run before HA role checks. A malformed-method request need not return `not_active`.

Status response contract:

| Field | Type | Meaning |
| --- | --- | --- |
| `ok` | bool | Request success |
| `error` | string | Error text when `ok=false` |
| `state` | string | Current pipeline state |
| `info` | string | Human-readable state detail |
| `lagTimeSeconds` | int64 | Logical source-to-target lag |
| `eventsRead` | int64 | Change-stream events read, excluding tick events |
| `eventsApplied` | int64 | Events applied to target |
| `lastReplicatedOpTime.ts` | string | Reported replication frontier timestamp; ticks may advance it without a real apply |
| `lastReplicatedOpTime.isoDate` | string | Reported frontier time in RFC3339 |
| `initialSync.completed` | bool | Clone and catch-up completed |
| `initialSync.cloneCompleted` | bool | Bulk clone completed |
| `initialSync.lagTimeSeconds` | int64 | Initial-sync logical lag |
| `initialSync.estimatedCloneSizeBytes` | uint64 | Estimated clone size |
| `initialSync.clonedSizeBytes` | uint64 | Bytes cloned |
| `finalization.completed` | bool | Finalize completed successfully |
| `finalization.startedAt` | time | Finalize start, when available |
| `finalization.completedAt` | time | Finalize completion, when available |
| `finalization.unsuccessfulIndexes[]` | array | Index reports with namespace, name, failed/incomplete/inconsistent type, optional keys, and reason |

Idle status omits initial-sync, optime, and finalization objects while numeric counters remain zero. Recovered finalized state retains completion but not finalization timestamps or unsuccessful-index reports.

### E2E and Diagnostics

Pytest CLI options override matching environment variables:

| Environment variable | Pytest option |
| --- | --- |
| `TEST_SOURCE_URI` | `--source-uri` |
| `TEST_TARGET_URI` | `--target-uri` |
| `TEST_PCSM_URL` | `--pcsm_url` |
| `TEST_PCSM_BIN` | `--pcsm-bin` |

If Poetry fails with `"bad interpreter"` after a Python version change because its interpreter path is stale, invoke `.venv/bin/pytest` directly. Unqualified local pytest discovers the full `tests/` tree, including sharded modules, while skipping slow tests by default. For CI-comparable scope:

- RS runs an explicit seven-file selection.
- Normal sharded jobs discover broadly with one deselection.
- The 8.0 unequal-shard matrix entry runs only `tests/test_presplit_sharded.py`.

Live-system tests use default non-system database cleanup; `tests/test_hack_clis.py` overrides it. Existing polling and fixed waits are legacy behavior, not patterns to copy into new async tests.

```bash
.venv/bin/pytest --runslow
```

```bash
hack/change_stream.py -u "mongodb://src-mongos:27017"
hack/change_stream.py -u "mongodb://tgt-mongos:29017" --show-checkpoints
hack/change_stream.py -u "mongodb://rs00:30000"
hack/monitor_writes.py -u "mongodb://src-mongos:27017"
```

### Metrics and CI Operations

- `make metrics-up` and `hack/ha/run.sh` independently create external `pcsm-metrics`; metrics and HA can therefore start in either order. MongoDB clusters must still start before HA. `make metrics-down` stops monitoring but keeps the network.
- Bundled Prometheus reaches `pcsm0:2242`, `pcsm1:2243`, and `pcsm2:2244` through Docker DNS. Targets remain down until HA starts. It does not scrape a host PCSM bound to loopback because that listener is unreachable from the Prometheus container.
- Only ACTIVE drives replication. Replication and clone gauge/rate panels filter with `<metric> and on(instance) (percona_clustersync_mongodb_ha_active == 1)` so frozen gauges from a demoted former ACTIVE do not render. Rate panels retain instance identity rather than summing across instances; historical increments can remain in `rate()` until the window moves past them, after which a demoted instance's rate becomes zero.

Every role exports the HA metrics:

| Metric | Type | Meaning |
| --- | --- | --- |
| `percona_clustersync_mongodb_ha_active` | Gauge | 1 for ACTIVE, 0 for STANDBY |
| `percona_clustersync_mongodb_ha_term` | Gauge | Current lease term |
| `percona_clustersync_mongodb_ha_role_transitions_total` | Counter | Per-instance role transitions |
| `percona_clustersync_mongodb_ha_info` | Gauge | Constant 1 with `instance_id` and `group` labels |

`ha_info` is published once during startup in `main.go`. Grafana's **High Availability** row contains:

- **Instance Roles**: per-scrape-instance state timeline; this is a scrape-series roster, not a membership-collection query.
- **Lease Term**: ACTIVE-only `max(...)` stat retaining the last nonnull sample through a brief gap and excluding STANDBY's initial term-zero value.
- **Role Transitions**: per-instance `increase(...[$__range])`; 0 is stable, 1 is warning, and 3 or more indicates flapping.

Dashboard capacity lines are fixed reference thresholds, not values discovered from runtime queue configuration.

CI workflows and operations:

- `.github/workflows/go.yml` runs race-enabled unit tests, formatter suggestions, golangci-lint, and integration tests for catalog, clone, repl, mdb, and ha across MongoDB 6/7/8.
- `.github/workflows/e2etests.yml` runs local RS/sharded E2E across the version matrix.
- `.github/workflows/ci.yml` runs the external functional suite `Percona-QA/psmdb-testing` against PSMDB 6.0, 7.0, and 8.0 in five pytest partitions; this `shard` is test partitioning, not MongoDB shard count.
- `ci.yml` ignores PR changes confined to `tests/**` and `packaging/**`.
- QA branch precedence is `tests_ver`, then the first case-sensitive PR-title `PCSM-[0-9]+` branch (for example `PCSM-286`) found through the GitHub branch API, then `main`. A later eligible PCSM CI run consumes that branch; a QA-only push is not a trigger declared here.
- Jenkins `hetzner-pcsm-functional-tests` is operationally known at <https://psmdb.cd.percona.com/view/PCSM/> but not verified from this repo. Hetzner is preferred for cost; cancel capacity-stuck builds and retry with AWS, the first cloud option in job parameters.

External references:

| Project | Repository | Documentation |
| --- | --- | --- |
| PCSM Docs | <https://github.com/percona/pcsm-docs> | <https://docs.percona.com/percona-clustersync-for-mongodb/> |
| PSMDB Testing | <https://github.com/Percona-QA/psmdb-testing> | External functional-test repository |
