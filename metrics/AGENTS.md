# METRICS CONTRACT KNOWLEDGE

## OVERVIEW

Score: 8; distinct telemetry-schema domain. Owns Prometheus collectors shared by copy, replication, and HA, including their exported names and labels.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Add or register collectors | `metrics.go:Init` | Registers application, Go-runtime, and process collectors |
| Track copy progress | `metrics.go:AddCopy*` | Separate read/insert documents and bytes |
| Track replication work | `metrics.go:IncEventsRead`, `AddEventsApplied` | Source-read versus target-applied counters |
| Observe queue pressure | `metrics.go:SetRepl*QueueSize` | Reader, worker input, and sealed-bulk queue gauges |
| Observe bulk flushes | `metrics.go:ObserveReplWorkerFlush*` | Batch-size and duration histograms |
| Report HA state | `metrics.go:SetHARoleAndTerm`, `SetHAInfo` | Role/term gauges and identity metadata |
| Update dashboard consumers | `../hack/metrics/grafana-board.json` | Shipped visualization of metric names |

## CONVENTIONS

- Application metric namespace is `percona_clustersync_mongodb`.
- Worker vectors use the single `worker` label; HA identity uses `instance_id` and `group`.
- Collectors are package-level instances; `Init` attaches those instances to the supplied registerer.
- `Init` also registers Go and process collectors; process metrics use the application namespace.
- Copy batch durations are latest-value gauges; replication flush durations are histograms.
- Durations are seconds, sizes are bytes, and lag is measured in logical seconds.
- Reader-to-dispatcher, worker input, and pending sealed-bulk queues are separate measurements.
- Worker applied-event counters are separate from the aggregate applied counter; do not substitute queue depth or flush batch size for completed operations.
- `ha_active` is 1 or 0; `ha_info` publishes a constant 1 with identity labels.
- `SetHARoleAndTerm` updates gauges; transition counting is a separate caller action.

## ANTI-PATTERNS

- Do not invoke `Init` twice on the same registry; registration uses `MustRegister`.
- Do not rename metric strings or labels without updating dashboard consumers.
- Do not combine read and inserted counters; they expose different copy-pipeline stages.
- Do not count ordinary lease renewals as HA role transitions.
