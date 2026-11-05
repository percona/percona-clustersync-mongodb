# PROJECT KNOWLEDGE BASE

**Generated:** 2026-11-05T06:25:51.505Z
**Commit:** a84b36e
**Branch:** main

## OVERVIEW

Percona ClusterSync for MongoDB (PCSM): Go service for initial clone plus continuous replication between replica sets or sharded clusters. One binary = HTTP server, HTTP client subcommands, direct target-state reset; MongoDB-backed HA always on.

## STRUCTURE

```text
./
|-- main.go               # CLI, HTTP server/client, startup, reset subcommands
|-- recovery.go           # Checkpoint claim/restore and single checkpoint writer
|-- tenure.go             # ACTIVE tenure lifecycle controller
|-- config/ ha/ mdb/      # Flags/env, lease+membership, driver boundary
|-- pcsm/                 # Pipeline state machine; catalog/ clone/ repl/
|-- errors/ util/ sel/    # Global helpers (root-owned), namespace selection
|-- log/ metrics/         # Structured logging, telemetry contract
|-- hack/ tests/          # Local topologies/HA drill/monitoring, Python E2E
`-- .github/ packaging/   # Root-owned CI and release automation
```

## WHERE TO LOOK

Deeper guidance owns local invariants; read it before changing that domain.

| Task                                 | Location                                               | Notes                                |
| ------------------------------------ | ------------------------------------------------------ | ------------------------------------ |
| CLI, HTTP routing, startup           | `main.go`                                              | `startRequest` defines `/start` body |
| Checkpoint fencing, tenures          | `recovery.go`, `tenure.go`; `ha/AGENTS.md`             | Read `ha/README.md`                   |
| Flags, env bindings, defaults        | `config/AGENTS.md`                                     | Hidden/deprecated flags there        |
| Lease, membership, failure timing    | `ha/AGENTS.md`                                         |                                      |
| Driver policy, retry, compat         | `mdb/AGENTS.md`                                        |                                      |
| States, pause/finalize, recovery     | `pcsm/AGENTS.md`                                       | Includes `pausing`                   |
| Target collections, indexes          | `pcsm/catalog/AGENTS.md`                               |                                      |
| Initial copy, presplit               | `pcsm/clone/AGENTS.md`                                 |                                      |
| Ordered replay, frontier             | `pcsm/repl/AGENTS.md`                                  |                                      |
| Logging                              | `log/AGENTS.md`                                        |                                      |
| Metrics, Grafana panels              | `metrics/AGENTS.md`                                    |                                      |
| Hosts, topologies, cleanup, HA drill | `hack/AGENTS.md`                                       |                                      |
| E2E fixtures, CI vs local scope      | `tests/AGENTS.md`                                      |                                      |
| CI, packaging                        | `.github/workflows/{go,e2etests,ci}.yml`, `packaging/` | No child guide                       |
| PR/issue automation | `.github/opencode/*.md`, `.github/workflows/opencode*.yml`, `.github/workflows/jira-*.yml` | Agent artifacts; workflows publish |

## CODE MAP

Refs unmeasured: reference centrality not measured with LSP/ast-grep.

| Symbol                                          | Type        | Location                   | Refs       | Role                                                                  |
| ----------------------------------------------- | ----------- | -------------------------- | ---------- | --------------------------------------------------------------------- |
| `pcsm.PCSM` / `pcsm.New`                        | struct/func | `pcsm/pcsm.go`             | unmeasured | Pipeline, fresh per tenure                                            |
| `createServer`                                  | func        | `main.go`                  | unmeasured | Connect both sides, version compat, join membership, first lease tick |
| `runServer`                                     | func        | `main.go`                  | unmeasured | Serves HTTP                                                           |
| `server.Handler`                                | method      | `main.go`                  | unmeasured | Routes API, `/metrics`, pprof                                         |
| `server.admit` / `writeNotActive`               | method      | `main.go`                  | unmeasured | 409 `not_active` gate                                                 |
| `startRequest`                                  | struct      | `main.go`                  | unmeasured | `/start` body; pointer fields = omitted vs zero                       |
| `Restore`                                       | func        | `recovery.go`              | unmeasured | Claim checkpoint at term, then recover                                |
| `RunCheckpointing` / `observeOwnership`         | func/method | `recovery.go`, `tenure.go` | unmeasured | Single writer / lifecycle controller                                  |
| `openTenure` / `endTenure` / `onCheckpointLost` | method      | `tenure.go`                | unmeasured | ACTIVE epoch; `minTerm = term+1`                                      |
| `mdb.CheckVersionCompat`                        | func        | `mdb/version.go`           | unmeasured | Source major must not exceed target                                   |
| `Membership.SetRole`                            | method      | `ha/membership.go`         | unmeasured | Role/term gauges, transition counter                                  |
| `errors.Wrap*`                                  | func        | `errors/errors.go`         | unmeasured | Project error wrapping                                                |
| `util.CtxWithTimeout`                           | func        | `util/context.go`          | unmeasured | Bounded op; nil parent -> Background                                  |

## CONVENTIONS

- Use project `errors.Wrap`/`Wrapf`/`Join`, not stdlib; `Wrap(nil, ...)` returns nil.
- Bound MongoDB ops with `util.CtxWithTimeout(ctx, <op-specific const>, fn)`, e.g. `config.DisconnectTimeout`.
- Format: `goimports -local github.com/percona` plus `gofumpt -extra` (CI suggests).
- Go unit tests: `-race`, testify `assert`/`require`, table-driven; `t.Parallel()` only for independent cases.
- `nolint` narrow and justified (`wrapcheck` for intentional driver errors, `gochecknoglobals`, `err113`, `gosec`); `nolintlint` disabled, so review enforces it.
- Code changes: run `make lint`, `make test`, `make test-integration`, `make pytest` where prerequisites exist; report blocked checks.

## ANTI-PATTERNS (THIS PROJECT)

- Never call a replication checkpoint wholly applied: inclusive, timestamp may be uncommitted.
- Never run any `pcsm reset*` while a server runs; resets are stopped-system procedures. Prefer full `pcsm reset` (or `--reset-state`) over hidden `reset lease`: lease-only reset restarts terms below a surviving checkpoint term.
- Never switch RS/sharded topologies without `./hack/cleanup.sh`: host ports overlap.
- Never probe health with `/status`: STANDBY returns 409 `not_active`. Use `/metrics`.
- Never require the `me`/`role`/`group` envelope: present only with >1 observed live member.
- Never change telemetry without checking `metrics/AGENTS.md` and its dashboard contract.
- Never infer Go coverage from packaging builds or equate local E2E scope with CI selection.

## UNIQUE STYLES

- Server on localhost:2242 by default; `status/start/pause/resume/finalize` subcommands are HTTP clients; reset/version bypass HTTP.
- `/status` GET; `/start`, `/pause`, `/resume`, `/finalize` POST; `/metrics`, `/debug/pprof/*` on every role. Wrong-method check precedes role check.
- Pause lifecycle: `running -> pausing -> paused`. `pausing` means the request was accepted and work is draining; only `paused` confirms the stop.
- Finalize needs `initialSync.completed`; a running pipeline pauses first. Deadline and retry behavior: [pipeline reference](pcsm/README.md#pausing-replication).
- Failed replication resumes only explicitly: `--from-failure` / `{"fromFailure":true}`; interrupted clone restores as failed, needs new `/start`.
- Local topology and exact URI/hosts setup: [CONTRIBUTING.md](CONTRIBUTING.md#running-pcsm-locally).

## COMMANDS

```bash
make build              # bin/pcsm
make test-build         # race debug bin/pcsm_test
make test               # race unit tests
make test-integration   # Docker; -tags integration -count=1 -timeout 5m: . catalog clone repl mdb ha
make lint               # golangci-lint
make lint-py; make fmt-py
make pytest             # live-cluster E2E; slow skipped unless --runslow
./hack/rs/run.sh       # one topology; sharded alternative: ./hack/sh/run.sh
./hack/cleanup.sh       # before switching topology
make metrics-up; make metrics-down
```

## NOTES

- `Starting HTTP server at ...` logs before socket bind: not a readiness signal.

<!-- End of init-deep baseline. -->

## PROJECT NOTES

### Regeneration

Regenerate the baseline above the marker; preserve and reconcile this tail separately.
The marker is a project convention, not built-in `init-deep` protection. Before any
`--create-new` run, retain authored tails; afterward compare them before accepting
the result. Existing whole-file line limits still include these short supplements.
The generated header records the baseline scan, not later authored-note revisions.

### Reference owners

Read [HA](ha/README.md) before changing election, tenure, recovery, HA-facing HTTP,
or failover tooling, including files outside `ha/`.

| Topic | Canonical reference |
| --- | --- |
| States, status, pause/finalize, target write concern | [Pipeline/API](pcsm/README.md) |
| HA, fencing, failover, resets/upgrades, monitoring, drills | [HA](ha/README.md) |
| Local setup, configuration, diagnostics | [Contributing](CONTRIBUTING.md#running-pcsm-locally) |
| Tests, CI matrix and QA branch selection | [Contributing](CONTRIBUTING.md#running-tests-locally) |

### Maintainer context

These practices require maintainer access and explicit authorization; verify current
external policy and availability before acting. They are not public contributor requirements.
- Existing QA practice pushes approved fixes directly to the selected ticket branch in `Percona-QA/psmdb-testing`, without a QA PR.
- Jenkins entry: `hetzner-pcsm-functional-tests` at <https://psmdb.cd.percona.com/view/PCSM/> (not verified by this repository). Existing hosting preference is Hetzner for cost, with AWS for capacity-stuck builds.
