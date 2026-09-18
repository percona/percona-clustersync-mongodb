# PCSM PIPELINE KNOWLEDGE

## OVERVIEW

Score: 14; distinct pipeline-orchestration domain. Owns the replication state machine and coordinates catalog, clone, repl, recovery, and finalization.

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
- `Checkpoint` returns nil while idle and serializes catalog, clone, repl, state, filters, and the current error together.
- Component checkpoint acquisition and BSON marshaling remain one coordinated operation; catalog lock mechanics live in child guidance.
- Recovery reconstructs catalog, clone, and repl before applying component checkpoints.
- `FinalizeStatus` is absent until finalize starts; copy it before returning status to callers.
- `lifecycleCtx` is the deliberate `containedctx` exception for background pipeline ownership.

## ANTI-PATTERNS

- Do not mutate `state`, `err`, component pointers, or finalization status outside the lifecycle lock.
- Do not add a state transition without matching invalid-state coverage.
- Do not resume an interrupted initial clone as though its copy workers had persisted their progress.
- Do not report finalization complete before `CompletedAt` and unsuccessful-index results are available.
- Do not make orchestration tests depend on live MongoDB when the local interfaces cover the seam.
