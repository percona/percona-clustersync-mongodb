# HIGH AVAILABILITY KNOWLEDGE

Read [README.md](README.md) before changing HA behavior, timing, reset, metrics, or the `hack/ha/` drill. It owns the operational contract: fencing, reset generations, failure timing, STANDBY API, and metrics. Update it in the same change when behavior moves.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| HA data model | `ha.go` | `Role`, `Term`, `Member`, `Lease` |
| Membership lifecycle | `membership.go` | Join, heartbeat, `SetRole`, liveness queries |
| Election | `lease.go` | Acquire/renew, bootstrap, deadline, `Ownership` reports, `AdvanceTerm` |
| Tenure controller | `../tenure.go` | `observeOwnership`, `minTerm`, epoch, admission |
| Checkpoint fence | `../recovery.go` | `Restore` claim, `RunCheckpointing`; contract in `checkpoint` doc comment |
| Timing constants | `../config/const.go` | Renew, heartbeat, `HAOperationTimeout`, default TTL |
| Unit tests | `lease_test.go`, `membership_test.go` | Attempt outcomes and what each reports |
| Live failover tests | `*_integration_test.go` | `make test-integration` (Docker) |

## INVARIANTS

- The lease document is election truth. `Member.Role` and `Member.Term` only advertise what the observer set.
- `ha` reports `Ownership` as a level after every attempt that settles it. It never decides a role.
- A canceled attempt reports nothing. A failed one reports nothing while held within the local deadline, and reports lost past it or when not held.
- Compare expiry only against server time (`$$NOW`). Bootstrap timestamps stay provisional until a server-clock renewal.
- Bootstrap inserts, never upserts. Duplicate key means another contender won, not a retryable failure.
- Terms only rise. `AdvanceTerm` uses `$max` and works only for the current owner.
- Every lease and member operation is bounded by `HAOperationTimeout`.
- `Release` stops competing for shutdown. `RelinquishLease` gives up the lease and keeps the loop.
- `SetRole` counts a transition only on a role change; metrics depend on it.
- A changed timing constant, metric, or reset path must update `README.md`; a metric change also updates `../hack/metrics/grafana-board.json`.
