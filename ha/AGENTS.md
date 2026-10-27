# HIGH AVAILABILITY KNOWLEDGE

## OVERVIEW

Score: 8; distinct election domain. Owns membership liveness, MongoDB-backed leases, ownership reports, and monotonic fencing terms.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change HA data model | `ha.go` | `Role`, `Term`, `Member`, and `Lease` |
| Change membership lifecycle | `membership.go` | Join, heartbeat, role state, liveness queries |
| Change election behavior | `lease.go` | Acquire/renew, bootstrap, deadline, ownership reports |
| Test lease logic | `lease_test.go` | Attempt outcomes and what each reports |
| Test membership behavior | `membership_test.go` | Heartbeats, members, and role state |
| Test live failover | `*_integration_test.go` | MongoDB-backed contention and takeover |

## CONVENTIONS

- The lease document is election truth; `Member.Role` and `Member.Term` are informational.
- `Term` increases on takeover and is passed downstream as the checkpoint fencing token.
- Lease expiry and renewal use MongoDB server time (`$$NOW`), not client clocks.
- Startup performs `FirstLeaseTick` before serving HTTP so ownership is settled.
- The lease loop reports `Ownership` (held or not, and at which term) to its `Observer` after every attempt that settles it, synchronously and as a level: an unchanged outcome is reported again, and the observer reconciles against the latest report rather than tracking transitions. A canceled attempt, or a failed one while the lease is held within its deadline, reports nothing. The observer (the server's lifecycle controller) derives the role from it; membership only advertises what it is told through `SetRole`.
- A non-cancellation acquire/renew error keeps the lease within its local deadline and reports nothing; past the deadline, or when not held, it reports the lease lost. `context.Canceled` reports nothing.
- Acquire/renew attempts use `HAOperationTimeout`; bootstrap timestamps are provisional until server-clock renewal succeeds.
- Bootstrap uses insert, not upsert; duplicate key means another contender won.
- Advertised role state and term are read and updated through `Membership` synchronization methods; `SetRole` counts a transition only when the role changes.
- `Release` stops lease competition for shutdown; `RelinquishLease` gives the lease up without stopping the loop, and the next attempt reports the outcome.
- `AdvanceTerm(ctx, to)` raises the lease term to at least `to` only while this instance owns the lease (`$max`, never lowers). The controller uses it so no two ACTIVE tenures of a process share a term: a renewal after a loss keeps the term when nobody else took the lease. The next renewal reports the advanced term.
- Terms are never reused within one reset generation: lease, recovery and member state are reset together, with every instance stopped. A lease-only reset restarts terms below a surviving checkpoint's, and nothing guards a reset under running instances; the root `AGENTS.md` HA contracts describe what either does.

## ANTI-PATTERNS

- Never infer ACTIVE status from a member document alone.
- Never compare lease expiry against local wall-clock time.
- Never treat duplicate-key during lease bootstrap as a retryable service failure.
- Never treat a failed lease operation as proof of ownership or bypass its fail-safe demotion.
- Never decrement or reuse a fencing term after another instance acquires the lease.
- Never decide a role inside `ha`; report ownership and let the observer derive it.
