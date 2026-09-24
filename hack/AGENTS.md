# LOCAL ENVIRONMENT TOOLING KNOWLEDGE

## OVERVIEW

Score: 8; distinct operator-tooling domain. Owns disposable MongoDB topologies, HA drills, metrics tooling, and diagnostics.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Shared shell helpers | `util` | Compose, mongosh, replica-set init, and readiness helpers |
| Start replica sets | `rs/run.sh` | Source `rs0` and target `rs1` |
| Start sharded clusters | `sh/run.sh` | Config servers, shards, mongos, variable shard counts |
| Start PCSM HA group | `ha/run.sh` | Three PCSM instances attached to an existing cluster network |
| Run failover drill | `ha/kill-active.sh` | Active-instance termination and restart behavior |
| Inspect HA roles | `ha/status-group.sh` | Port, instance, role, state, and term |
| Clean environments | `cleanup.sh` | Ordered removal of PCSM, RS, sharded, and HA resources |
| Run metrics stack | `metrics/` | Prometheus and Grafana compose/config |
| Ad hoc MongoDB tools | `*.py` | Change streams, write monitoring, data load/compare |

## CONVENTIONS

- Shell topology scripts source `hack/util` for Docker Compose and mongosh helpers.
- Sharded topology uses Compose project `s1` and network `s1_default`; RS uses `rs_default`. HA joins the selected existing network.
- `SRC_SHARDS` and `TGT_SHARDS` select one to three shards independently.
- Side-specific MongoDB version variables override `MONGO_VERSION`.
- Host and container ports differ for target mongos: host `29017`, container `27017`.
- HA scripts expect clusters to exist first and publish PCSM APIs on ports 2242-2244.
- `ha/run.sh --reset` stops the old HA group before clearing lease, members, and checkpoints.

## ANTI-PATTERNS

- Never switch RS and sharded topologies without running `./hack/cleanup.sh`.
- Never remove a cluster network before stopping PCSM HA containers attached to it.
- Never use host port `29017` from inside the target mongos container.
- Never reset target state while an old HA group is still running.
- Never raise shard counts beyond the three configured service/port slots.
- Never treat `hack/reference.md` snippets as canonical when scripts or Make targets disagree.
