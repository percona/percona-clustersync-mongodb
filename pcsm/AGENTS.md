# PCSM PIPELINE KNOWLEDGE

## OVERVIEW

Score 8; distinct orchestration domain. Owns the state machine and composes catalog, clone, repl, checkpoint, and finalization.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change lifecycle behavior | `pcsm.go` | `PCSM.Start`, `Pause`, `Resume`, `Finalize`, and state transitions |
| Change persisted pipeline state | `pcsm.go` | `Checkpoint` and `Recover` compose catalog, clone, and repl checkpoints |
| Add a pipeline test double | `mocks_test.go` | Local `Cloner` and `Replicator` substitutes |
| Test state transitions | `state_test.go` | State sequencing and invalid-operation coverage |
| Test finalization status | `finalize_status_test.go` | Started/completed timestamps and unsuccessful indexes |
| Change lag monitoring | `pcsm.go` | `monitorInitialSync` uses clone finish time; `monitorLagTime` uses source cluster time |
| Work on initial copy | `clone/` | Child guidance applies |
| Work on change replay | `repl/` | Child guidance applies |
| Work on target schema state | `catalog/` | Child guidance applies |

## CONVENTIONS

- `PCSM` is the synchronization boundary for lifecycle state; transition checks and mutations stay under `p.lock`.
- `Cloner` and `Replicator` interfaces are the seams used by orchestration tests.
- A fresh `Start` builds one catalog shared by clone and repl.
- `Checkpoint` returns nil while idle and serializes catalog, clone, repl, state, filters, and the current error together under the lifecycle lock. Snapshots carry no sequence number: the checkpoint writer in the root `recovery.go` orders them by lease term and by writing one at a time.
- Component checkpoint acquisition and BSON marshaling remain one coordinated operation; catalog lock mechanics live in child guidance.
- Recovery reconstructs catalog, clone, and repl before applying component checkpoints.
- `FinalizeStatus` is absent until finalize starts; copy it before returning status to callers.
- `lifecycleCtx` is the deliberate `containedctx` exception for background pipeline ownership.
- Lock order is `lock -> execMu`; `execMu` guards execution handles only.

## ANTI-PATTERNS

- Do not mutate `state`, `err`, component pointers, or finalization status outside the lifecycle lock.
- Do not add a state transition without matching invalid-state coverage.
- Do not restore clone workers from an interrupted run; their progress is not persisted.
- Do not launch a run or finalizer outside `startRun`/`startFinalize`: they derive the execution from the request's epoch (`WithEpoch`) and publish the handle `Stop` cancels and joins.
- Do not treat a component error recorded while the execution was being canceled as a failure: `Stop` settles nothing, and the pipeline it stopped is discarded with its tenure.
- Do not report finalization complete before `CompletedAt` and unsuccessful-index results are available.
- Do not make orchestration tests depend on live MongoDB when the local interfaces cover the seam.

<!-- End of init-deep baseline. -->

## PROJECT NOTES

Read [states and status](README.md#states-and-status) before changing `State*`,
and [pause/finalize deadlines](README.md#pausing-replication) before changing
request completion. For tenure cancellation and recovery, read [HA](../ha/README.md).

- `Checkpoint` persists a pending `pausing` as `paused`; HTTP status must keep reporting the live drain.
- `Finalize` joins the captured run outside the lifecycle lock and rechecks the execution before scheduling catalog work.
- Non-majority target data write concern is stored in the checkpoint and restored into clone/repl options; checkpoints, HA, and DDL stay majority.
