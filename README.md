# Percona ClusterSync for MongoDB

[![Go Report Card](https://goreportcard.com/badge/github.com/percona/percona-clustersync-mongodb)](https://goreportcard.com/report/github.com/percona/percona-clustersync-mongodb) [![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/percona-clustersync-mongodb)](https://cla-assistant.percona.com/percona/percona-clustersync-mongodb)

Percona ClusterSync for MongoDB (PCSM) is a tool for cloning and replicating data between MongoDB clusters. It supports both replica sets and sharded clusters, handling initial data cloning followed by continuous change replication.

For more information about PCSM and how to use it, see [Percona ClusterSync for MongoDB documentation](https://docs.percona.com/percona-clustersync-for-mongodb/).

Percona ClusterSync for MongoDB includes the following **Features**:

- Clone data from source to target MongoDB cluster
- Real-time change replication via [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- Support for both replica sets and sharded clusters
- Namespace filtering (include/exclude databases and collections)
- Automatic index management on target cluster
- CLI tool as well as HTTP API

## Installation

You can install Percona ClusterSync for MongoDB in the following ways:

- from Percona repository (recommended)
- build from source code

Find the installation instructions in the [official documentation](https://docs.percona.com/percona-clustersync-for-mongodb/installation.html).

## API

PCSM is a CLI tool, but also exposes HTTP API as well.

For reference see [PCSM commands](https://docs.percona.com/percona-clustersync-for-mongodb/pcsm-commands.html) and [HTTP API](https://docs.percona.com/percona-clustersync-for-mongodb/api.html) docs.

### States and status

```text
idle --start--> running --pause--> pausing --> paused
                   ^                            |
                   `-----------resume-----------'
                                                |
                                             finalize
                                                v
                                           finalizing --> finalized
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

The [`startRequest`](main.go) type defines the `/start` body.
HA role checks and the optional response envelope are documented in
[API on STANDBY](ha/README.md#api-on-standby).

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

### Pausing replication

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

### Target data write concern

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

## High availability

See [HA architecture and operations](ha/README.md) for election, tenure and
checkpoint fencing, failover timing, reset and upgrade procedures, monitoring,
and local failover drills.

## Submit Bug Report / Feature Request

If you find a bug in Percona ClusterSync for MongoDB, submit a report to the project's [JIRA issue tracker](https://jira.percona.com/projects/PCSM).

As a general rule of thumb, please try to create bug reports that are:

- Reproducible. Include steps to reproduce the problem.
- Specific. Include as much detail as possible: which version, what environment, etc.
- Unique. Do not duplicate existing tickets.
- Scoped to a Single Bug. One bug per report.

When submitting a bug report or a feature, please attach the following information:

- The output of the `pcsm status` command
- The output of the `pcsm logs` command

## Licensing

Percona is dedicated to **keeping open source open**. Whenever possible, we strive to include permissive licensing for both our software and documentation. For this project, we are using the Apache License 2.0 license.

## How to get involved

We encourage contributions and are always looking for new members who are as dedicated to serving the community as we are.

The [Contributing Guide](CONTRIBUTING.md) contains the guidelines for contributing.

## Contact

You can reach us:

- on [Percona ClusterSync for MongoDB Community Forum](https://forums.percona.com/c/mongodb/percona-clustersync-mongodb-pcsm/87)
- or [Contact Form](https://www.percona.com/about/contact)
