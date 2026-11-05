# HIGH AVAILABILITY KNOWLEDGE

## OVERVIEW

Score 8; distinct election domain. Owns membership liveness, leases, ownership reports, and monotonic fencing terms.

## WHERE TO LOOK

| Task | Location | Notes |
| --- | --- | --- |
| HA data model | `ha.go` | `Role`, `Term`, `Member`, `Lease` |
| Membership lifecycle | `membership.go` | Join, heartbeat, `SetRole`, liveness queries |
| Election | `lease.go` | Acquire/renew, bootstrap, deadline, ownership, `AdvanceTerm` |
| Tenure controller | `../tenure.go` | `observeOwnership`, `minTerm`, epoch, admission |
| Checkpoint fence | `../recovery.go` | `Restore` claim, `RunCheckpointing`, checkpoint contract |
| Timing constants | `../config/const.go` | Renew, heartbeat, `HAOperationTimeout`, default TTL |
| Unit tests | `lease_test.go`, `membership_test.go` | Attempt outcomes and ownership reports |
| Live failover tests | `*_integration_test.go` | MongoDB-backed contention and takeover |

## CONVENTIONS

- The lease document is election truth; member role and term advertise the observer's state.
- Ownership is a level reported after every attempt that settles it.
- Canceled attempts report nothing. Failed attempts report nothing while held within the deadline, and report lost past it or when not held.
- Expiry uses server time (`$$NOW`); bootstrap timestamps remain provisional until a server-clock renewal.
- `AdvanceTerm` uses `$max` and works only for the current owner.
- Lease attempts, release, term advance, and heartbeat use `HAOperationTimeout`. Member reads, member removal, and reset helpers inherit caller/client deadlines.
- `Release` stops competing for shutdown; `RelinquishLease` gives up the lease and keeps the loop.

## ANTI-PATTERNS

- Never decide a role inside `ha`; report ownership and let the observer derive it.
- Never upsert during bootstrap or retry duplicate-key acquisition as a service failure.
- Never decrement or reuse a fencing term after another instance acquires the lease.
- Never count renewals as role transitions; `SetRole` increments only on a role change.

<!-- End of init-deep baseline. -->

## PROJECT NOTES

Read [README.md](README.md) before changing HA behavior, timing, resets, or drills.
Keep the operational contract there current. For collector or dashboard changes,
follow [metrics guidance](../metrics/AGENTS.md).
