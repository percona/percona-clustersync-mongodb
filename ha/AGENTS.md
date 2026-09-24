# HIGH AVAILABILITY KNOWLEDGE

## OVERVIEW

Score: 8; distinct election domain. Owns membership liveness, MongoDB-backed leases, role transitions, and monotonic fencing terms.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change HA data model | `ha.go` | `Role`, `Term`, `Member`, and `Lease` |
| Change membership lifecycle | `membership.go` | Join, heartbeat, role state, liveness queries |
| Change election behavior | `lease.go` | Acquire/renew, bootstrap, demotion, role-change emission |
| Test lease logic | `lease_test.go` | Pipeline and transition cases |
| Test membership behavior | `membership_test.go` | Heartbeats, members, and role state |
| Test live failover | `*_integration_test.go` | MongoDB-backed contention and takeover |

## CONVENTIONS

- The lease document is election truth; `Member.Role` and `Member.Term` are informational.
- `Term` increases on takeover and is passed downstream as the checkpoint fencing token.
- Lease expiry and renewal use MongoDB server time (`$$NOW`), not client clocks.
- Startup performs `FirstLeaseTick` before serving HTTP so the initial role is settled.
- A non-cancellation acquire/renew error demotes to STANDBY with the last term; `context.Canceled` returns without reconciliation.
- Acquire/renew attempts use `HAOperationTimeout`; bootstrap timestamps are provisional until server-clock renewal succeeds.
- Same-role renewals update state without emitting duplicate role-change events.
- Bootstrap uses insert, not upsert; duplicate key means another contender won.
- Role state and term are read and updated through `Membership` synchronization methods.
- `RoleChanges` coalesces to the latest transition in a one-slot channel; consumers must not require a complete transition history.
- `Release` stops lease competition for shutdown; `RelinquishLease` demotes without stopping the election loop.

## ANTI-PATTERNS

- Never infer ACTIVE status from a member document alone.
- Never compare lease expiry against local wall-clock time.
- Never treat duplicate-key during lease bootstrap as a retryable service failure.
- Never treat a failed lease operation as proof of ownership or bypass its fail-safe demotion.
- Never decrement or reuse a fencing term after another instance acquires the lease.
- Never emit role changes for ordinary same-owner renewals.
