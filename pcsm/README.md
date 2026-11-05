# Pipeline and API reference

Pipeline state and recovery live in [pcsm.go](pcsm.go); HTTP handlers and JSON
responses live in [main.go](../main.go). This reference covers their public
contract.

## States and status

```text
idle --start--> running --pause--> pausing --> paused
                   ^                            |
                   `-----------resume-----------'
                                                |
                                             finalize
                                                v
                                           finalizing --> finalized

running / pausing / finalizing --error--> failed
finalized --start--> running
```

`/finalize` can also accept a running pipeline once initial sync is complete.
It requests a pause and waits for the drain before entering `finalizing`.
See [Pausing replication](#pausing-replication) for request deadlines and retries.

| State | Meaning |
| --- | --- |
| `idle` | Waiting for `/start` |
| `running` | Initial clone or change replication is active |
| `pausing` | Pause accepted; queued writes and monitors are still draining |
| `paused` | Replication has stopped and can resume |
| `finalizing` | Replication has stopped; catalog and index completion is in progress |
| `finalized` | Catalog finalization completed; source writes after replication paused are not included |
| `failed` | An execution failed; inspect logs before recovery |

`/start` begins a new run from `finalized`. Resume eligible replication-phase
failures explicitly with `{"fromFailure":true}` or `pcsm resume --from-failure`.
An interrupted clone requires a new `/start` to re-clone. Wait for
`initialSync.completed`, not just `cloneCompleted`, before finalizing.

| Endpoint | Method | Purpose |
| --- | --- | --- |
| `/status` | GET | Pipeline status |
| `/start` | POST | Start with optional namespace filters, clone/repl tuning, and write concern |
| `/pause` | POST | Accept a pause of running change replication, not an in-flight clone |
| `/resume` | POST | Resume paused replication or eligible explicit failure recovery |
| `/finalize` | POST | Wait for replication to stop, then schedule catalog finalization |
| `/metrics` | GET | Prometheus scrape surface |
| `/debug/pprof/*` | Handler-specific | Go profiling and debugging |

The [`startRequest`](../main.go) type defines the `/start` body.
HA role checks and the optional response envelope are documented in
[API on STANDBY](../ha/README.md#api-on-standby).

| Status field | Type | Meaning |
| --- | --- | --- |
| `ok` | bool | No pipeline error is reported |
| `error` | string | Error text, omitted when empty |
| `state` | string | Current pipeline state |
| `info` | string | Human-readable detail, omitted when empty |
| `lagTimeSeconds` | int64 | Logical source-to-target lag |
| `eventsRead` | int64 | Change-stream events read, excluding tick events |
| `eventsApplied` | int64 | Events applied to target |
| `lastReplicatedOpTime.ts` | string | Reported frontier timestamp; ticks can advance it without a real apply |
| `lastReplicatedOpTime.isoDate` | string | Reported frontier time in RFC3339 |
| `initialSync.completed` | bool | Clone and catch-up completed |
| `initialSync.cloneCompleted` | bool | Bulk clone completed |
| `initialSync.lagTimeSeconds` | int64 | Initial-sync logical lag, omitted when zero |
| `initialSync.estimatedCloneSizeBytes` | uint64 | Estimated clone size, omitted when zero |
| `initialSync.clonedSizeBytes` | uint64 | Bytes cloned |
| `finalization.completed` | bool | Finalization completed successfully |
| `finalization.startedAt` | time | Finalization start, when available |
| `finalization.completedAt` | time | Finalization completion, when available |
| `finalization.unsuccessfulIndexes[]` | array | Reports with `namespace`, `indexName`, `type` (`failed`, `incomplete`, `inconsistent`), optional `keys`, and `reason` |

Idle status omits initial-sync, optime, and finalization objects while numeric
counters remain zero. Optime is absent until a frontier is available.
Finalization is absent until requested; empty index reports are omitted.
Recovered finalized state retains completion, but not finalization timestamps
or unsuccessful-index reports.

## Pausing replication

`POST /pause` and `pcsm pause` acknowledge the pause request immediately.
Status changes from `running` to `pausing` while queued writes drain, then to
`paused` once replication has stopped. A drain failure changes status to
`failed` instead.

Wait for `state: "paused"` before treating the pause as complete. A resume
request received during `pausing` waits for completion within its request
deadline; a timeout stops that wait, not the accepted pause. State-aware clients
must recognize `pausing`, including when `pauseOnInitialSync` is enabled.
A start request during `pausing` is rejected immediately as already running.

`POST /finalize` also waits for replication and monitor cleanup, with a
five-second HTTP request deadline. If that wait expires, the pause continues,
but no catalog finalization is scheduled. Wait for `paused` and retry
`/finalize`, or inspect the error if the state becomes `failed`. Once
`/finalize` succeeds, catalog finalization proceeds asynchronously.

## Target data write concern

Clone inserts and replication data writes default to `majority`. To change their
acknowledgment requirement for a run, pass `--target-write-concern=1` to
`pcsm start` (or set `PCSM_TARGET_WRITE_CONCERN=1` for it), or send
`{"targetWriteConcern":"1"}` to `/start`. Values are `majority` or a positive
integer. Unacknowledged writes (`0`) are not supported. The server has no write
concern setting, so a run started without one uses `majority`. The value is
stored with the run and retained during checkpoint recovery and HA takeover.
Start a new run to change it. Checkpoints, HA state, and catalog DDL always use
majority write concern.

Using `1` can reduce stalls when target secondaries lag, but acknowledgment from
the primary alone does not make data majority-durable. A target primary failure
can roll back acknowledged data, and a majority checkpoint does not guarantee
that earlier data writes on other shards are durable. Keep `majority` when
automatic recovery must preserve that guarantee. Lower it only for a controlled
migration that can tolerate restarting and validating the copy after a target
failure. Retain the source and verify target consistency before cutover.

If a numeric write concern is unsatisfiable while a majority remains available
(for example, `w:3` with one of three target nodes down), HA and checkpoint
writes can still succeed. Replication bulk writes retry transient errors until
their execution context is canceled or an unrecoverable error occurs, so the
pipeline can remain `running`. Clone inserts make at most three attempts,
with 5s and 10s backoffs between attempts, and fail the run. Each attempt is
subject to the MongoDB operation timeout and can end earlier.
