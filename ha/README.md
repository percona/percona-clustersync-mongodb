# PCSM High Availability

Service-wide HA spans three places: [`ha/`](.) (lease, membership, ownership reports), [`../tenure.go`](../tenure.go) (ACTIVE tenure lifecycle), and [`../recovery.go`](../recovery.go) (checkpoint claim, restore, single writer). HA is always on. A lone instance is a group of one.

For pause/finalize deadlines and the target data write concern, see the root README: [Pausing replication](../README.md#pausing-replication) and [Target data write concern](../README.md#target-data-write-concern).

## Roles and persistence

Exactly one instance owns the target lease `percona_clustersync_mongodb.lease`. It's ACTIVE and drives replication. Every other live member is STANDBY and can take over. Each instance keeps a liveness document in `percona_clustersync_mongodb.members`.

- Startup does one synchronous lease attempt before serving HTTP, so a sole instance with an available lease becomes ACTIVE without a transient STANDBY window.
- Every instance connects to source and target before election and runs the version and hello reads. A STANDBY source connection then carries only driver monitoring traffic. An unreachable or incompatible source fails startup, not failover, and promotion skips connection setup.
- Lease expiry and renewal use MongoDB server time (`$$NOW`). A contender reads the stored `expiresAt`, never its own clock or TTL.
- Restarting a process gives it a new instance ID. It rejoins as STANDBY unless it later wins the election.

## Tenures and fencing

The lease term is a monotonic fencing token stored on the checkpoint document, and it's the only ordering of that document's writes.

- **Claim before restore.** A tenure claims the checkpoint at its term in `Restore` (creating the document when absent), then restores the snapshot the claim returned. From then on every older-term write is rejected. A write a previous tenure left on the wire either landed before the claim, so the tenure restored it, or lands after and is fenced.
- **One writer.** Within a tenure, `RunCheckpointing` saves snapshots one at a time. It stops at the first write it can't account for, fenced or failed without a settled outcome, and the tenure ends with it (`onCheckpointLost`). No later snapshot of that term is ever written. Snapshots carry no sequence number, and a write never creates the document.
- **Level, not events.** The lease loop reports `ha.Ownership` (held or not, and at which term) after each attempt that settles it. `observeOwnership` in `tenure.go` owns role, term and tenure, and reconciles against the latest report.
- **Epoch.** Every ACTIVE tenure builds a fresh `pcsm.PCSM`, restores it, and owns one epoch context. Pipeline executions, the checkpoint loop and the state-change checkpoint derive from it.
- **Request admission.** Lifecycle requests are admitted once the restore and any deferred `--start` finish. A request that reaches a tenure still restoring waits, within its own context, so a client that found a just-promoted ACTIVE is served. Otherwise the request gets 409 `not_active`.
- **Ending a tenure.** Any ownership loss (lease lost, lease deadline passed, ownership reported at another term) or the checkpoint writer ending cancels the epoch, joins its executions and discards the pipeline. It's never resumed. The next tenure restores from the checkpoint, the same path a restarted process takes, so an interrupted clone restores as failed and needs a new `/start`.
- **`minTerm`.** No two tenures of one process write under the same term. A tenure opens only at or above `minTerm`, which sits above every term this process already ran. Ownership reported below it is raised through the owner-only `AdvanceTerm`, and the tenure opens on the next report.
- **Containment only.** Cancellation isn't server-side fencing. A command already on the wire when the tenure ends may still apply.

## Resets and upgrades

The fence assumes terms are never reused. That holds within one reset generation, where lease, recovery and member state are reset together (`pcsm reset`, or `--reset-state` at startup) with every instance stopped.

- A lease-only reset (`pcsm reset lease`) restarts terms at 1, below any surviving checkpoint term, even with every instance stopped. The next instance then opens one tenure per term, each fenced by its claim, and climbs one term per two lease reports until it reaches the checkpoint's term. Use the full reset instead.
- Nothing guards a reset under running instances, by design. A running instance's tenure ends on the report at the new term and reopens above its `minTerm`. Another instance that takes the reset lease climbs as above. Worse, a checkpoint write still on the wire from a pre-reset tenure outranks a post-reset claim at a lower term and brings the cleared snapshot back.

Treat every reset as a stopped-system procedure.

| Command               | Effect                                              |
| --------------------- | --------------------------------------------------- |
| `pcsm reset`          | Clear lease, recovery, member and legacy heartbeat state (preferred) |
| `pcsm reset lease`    | Hidden. Clear HA lease state only                   |
| `pcsm reset recovery` | Hidden. Clear recovery state only                   |
| `pcsm reset members`  | Hidden. Clear HA member state only                  |

Reset commands bypass HTTP and connect straight to the target.

### 0.9 to 0.10

Stop every old server and run the full reset before upgrading. Startup rejects a legacy heartbeat younger than `StaleHeartbeatDuration` (35s) as evidence of a live pre-0.10 instance and accepts a stale marker. The full reset removes the legacy heartbeat collection. A checkpoint without a term decodes as term zero; startup does not universally reject that format.

## Lease TTL and failure timing

`--ha-lease-ttl` / `PCSM_HA_LEASE_TTL` defaults to 10s, must be at least 10s, and is truncated to whole milliseconds. Set the same value on every instance, because the owner's value sets both its local deadline and the stored expiry.

Fixed inputs from [`../config/const.go`](../config/const.go): lease renew and member heartbeat every 3s, `HAOperationTimeout` 5s per lease or member operation, stale member after 9s. Checkpointing defaults to every 15s plus state changes; `--recovery-checkpoint-interval` overrides the interval.

HA and checkpoint writes inherit the client's majority write concern without a `wtimeout` ([`../mdb/connect.go`](../mdb/connect.go)). Checkpoint operations obey tenure cancellation and `--mongodb-operation-timeout` (5m default); server selection can fail earlier. Clone and replication data writes use the run's [target write concern](../README.md#target-data-write-concern).

T is the lease TTL. The lease deadline and the checkpoint writer run independently, and whichever fails first ends the tenure. These are conditional estimates, not guarantees, and an immediate error can end a tenure sooner.

| Failure                                                                               | T = 10s                                                   | T raised                                                                                                              | Notes                                                                                                                                                         |
| ------------------------------------------------------------------------------------- | --------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| No target server selectable (partition, no primary)                                   | 7 to 10s, lease deadline                                  | min(T less up to 3s, checkpoint writer: 30 to 45s with default intervals for a write issued during the outage, up to the operation timeout for one already on the wire) | The driver defaults to 30s server selection; PCSM does not expose an override. A shorter operation deadline can end selection earlier. |
| Writable primary, majority acknowledgement stalls (primary keeps its majority view)   | 7 to 10s, lease deadline                                  | min(T less up to 3s, operation timeout)                                                                               | Failed lease attempts report nothing until the deadline. If the primary loses its majority and steps down, the row above applies.                            |
| Majority up, data write concern unsatisfiable (`w:3`, one node down)                  | no transition                                             | no transition                                                                                                         | Lease and checkpoint writes still succeed. Data-write behavior is in [Target data write concern](../README.md#target-data-write-concern).                      |
| ACTIVE process crash                                                                  | lease takeable T after the crashed owner's last renewal   | same, longer                                                                                                          | Takeover adds up to 3s of polling, the acquisition RPC and checkpoint restore before requests are admitted. A restarted sole instance stays STANDBY up to T. |

The 30 to 45s estimate combines the driver's 30s server-selection timeout with
up to one default 15s checkpoint interval before issuing the write. It assumes
no earlier write is still in flight. State-change saves can start sooner.

## API on STANDBY

`/status`, `/start`, `/pause`, `/resume` and `/finalize` reject otherwise valid STANDBY requests with HTTP 409 and JSON `error: "not_active"`. Wrong-method checks run first, so a malformed-method request need not return `not_active`. `/metrics` and `/debug/pprof/*` serve on every role, so probe health with `/metrics`, not `/status`.

The optional `me` / `role` / `group` envelope appears only when more than one live member is observed. `group.members[]` can locate ACTIVE. A membership read failure, or seeing just one member, omits it, so a lone instance keeps the pre-HA API byte for byte. `--group-name` / `PCSM_GROUP_NAME` is advisory.

## Monitoring

Every role exports these ([`../metrics/`](../metrics)):

| Metric                                                  | Type    | Meaning                                          |
| ------------------------------------------------------- | ------- | ------------------------------------------------ |
| `percona_clustersync_mongodb_ha_active`                 | Gauge   | 1 for ACTIVE, 0 for STANDBY                      |
| `percona_clustersync_mongodb_ha_term`                   | Gauge   | Current lease term                               |
| `percona_clustersync_mongodb_ha_role_transitions_total` | Counter | Per-instance role transitions                    |
| `percona_clustersync_mongodb_ha_info`                   | Gauge   | Constant 1 with `instance_id` and `group` labels |

`ha_info` is published once at startup in `main.go`. The Grafana **High Availability** row in [`../hack/metrics/grafana-board.json`](../hack/metrics/grafana-board.json) has:

- **Instance Roles**: a per-scrape-instance timeline. It's a roster of scrape series, not a membership query.
- **Lease Term**: ACTIVE-only `max(...)`, holding the last sample through a brief gap and skipping STANDBY's initial zero.
- **Role Transitions**: per-instance `increase(...[$__range])`. 0 is stable, 1 is a warning, 3 or more means flapping.

Replication and clone panels filter with `<metric> and on(instance) (percona_clustersync_mongodb_ha_active == 1)` so a demoted ACTIVE's frozen gauges don't render. Rate panels keep instance identity rather than summing. Old increments stay in `rate()` until the window passes them.

Dashboard capacity lines are fixed reference thresholds, not values discovered
from runtime queue configuration.

`make metrics-up` and `hack/ha/run.sh` each create the external `pcsm-metrics` network, so either can start first. MongoDB clusters must start before HA. `make metrics-down` keeps the network. Prometheus scrapes `pcsm0:2242`, `pcsm1:2243` and `pcsm2:2244` every 5s over Docker DNS; targets stay down until HA starts, and a host PCSM bound to loopback is unreachable from the container.

The bundled stack binds Prometheus to `127.0.0.1:9090` and Grafana to
`127.0.0.1:3000`. Grafana grants anonymous Admin access; keep these bindings on
loopback.

## Local HA drill

Start one topology first, per [Running PCSM locally](../CONTRIBUTING.md#running-pcsm-locally). Run these from the repository root. `hack/ha/` builds `pcsm:dev` and runs `pcsm0`, `pcsm1` and `pcsm2` on the topology's Docker network with host loopback API ports 2242 to 2244.

Stop any standalone PCSM server before using `--reset`; the script stops only
its managed HA group.

```bash
./hack/ha/run.sh rs --reset       # Or: ./hack/ha/run.sh sh --reset
./hack/ha/status-group.sh         # PORT | INSTANCE_ID | HOST | ROLE | STATE | TERM
./hack/ha/kill-active.sh          # Hard-kill ACTIVE; restart after 5s
./hack/ha/kill-active.sh 15       # Override restart delay
./hack/ha/kill-active.sh --no-restart
./hack/ha/status-active.sh        # Full status of observed ACTIVE
docker logs -f pcsm0
./hack/ha/stop.sh
./bin/pcsm status --port 2243     # Any published port works from the host
```

- `--reset` stops the old group before clearing lease, member and checkpoint state.
- A 5s restart doesn't mean another instance already took over. Takeover waits for the TTL.
- `status-active.sh` and `kill-active.sh` need the multi-member envelope and can report no ACTIVE when it's omitted.
- Stop HA containers before removing their cluster network; `./hack/cleanup.sh` does that by default.
