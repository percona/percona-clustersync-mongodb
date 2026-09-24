# CHANGE REPLICATION KNOWLEDGE

## OVERVIEW

Score: 11; distinct ordered-replay domain. Owns change-stream consumption, event routing, bulk application, barriers, and the safe resume frontier.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change repl lifecycle | `repl.go` | Options, start/pause/resume, stream dispatch, checkpoint state |
| Change event decoding | `events.go` | Change-event and pseudo-event representation |
| Change worker ordering | `worker.go` | Hash routing, async bulk queue, writer goroutine, barrier protocol |
| Change bulk execution | `bulk.go` | Client-level and collection-level bulk writers |
| Test checkpoint safety | `worker_checkpoint_test.go` | Applied frontier behavior |
| Test async writer behavior | `worker_async_test.go` | Pending bulks and writer completion |
| Test barriers and failures | `worker_test.go` | Drain, release, dead-worker, and error paths |
| Test live streams | `*_integration_test.go` | Integration-tagged MongoDB cases |

## CONVENTIONS

- `Options` uses zero values as auto/default selectors; `applyDefaults` resolves them once.
- Events for one document route to the same worker by document-key hash to preserve order.
- Each worker builds bulks in its main loop and executes sealed bulks in a separate writer goroutine.
- `lastReplicatedOpTime` is the reported frontier; `checkpointOpTime` is the inclusive resume floor, not proof the event at that timestamp was applied. Replay detection keeps strict `<` and applies on every topology: `watchWithRetry` reopens inclusively from the checkpoint on replica sets too, and a redelivered DDL behind newer committed writes must be dropped, not re-applied.
- `pool.Checkpoint` skips never-routed workers, uses the first routed timestamp for workers with no commit, and otherwise uses their committed timestamp; the minimum is the safe floor. `pool.ReportedFrontier` is the same bound over workers with outstanding events only, so a drained worker cannot pin reporting; when all are drained it is the newest commit.
- A run-owned tracker advances the resume floor from `Checkpoint` and the reported frontier from `ReportedFrontier` every 500ms, independently of dispatcher activity; cleanup stops and joins the tracker before stopping the pool.
- `Route`'s per-worker publishes and the `Checkpoint`, `ReportedFrontier`, and `Idle` scans are mutually exclusive under `routeMu`, so a scan never skips a worker that a concurrent `Route` is making visible.
- `@tick` carries the server's postBatchResumeToken timestamp from an empty change-stream batch. That the source has no undelivered events before it is inferred from the token's documented resume purpose, not stated by the spec. `run()` applies it to `lastReplicatedOpTime` only when the worker pool is idle. `appendOplogNote` only stimulates the oplog and is never the tick value.
- A barrier drains routed events, seals pending work, waits for the writer, then waits for explicit resume.
- Worker completion and errors use owned channels; dead workers must remain observable to barrier callers.

## ANTI-PATTERNS

- Never advance `checkpointOpTime` from tick events or unread change-stream batches.
- Never release a barrier before every pre-barrier bulk has completed.
- Never block on `barrierReq`, `barrierDone`, or `resumeCh` without also accounting for worker exit or context cancellation.
- Never close a worker queue from more than one owner.
- Never change routing without preserving per-document operation order.
- Never treat a writer error as a successful drain.
- Never count operations after the first ordered-bulk failure as executed.
