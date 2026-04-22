# Replication BufBuilder Fix Validation Guide

This document defines the final confidence workflow for the BufBuilder / pipeline-chunking / worker-contention fix.

## Scope

This guide covers:

- CI/CD regression gates
- staging (Atlas/mongos) validation
- dashboard and alerting recommendations
- operator interpretation of chunking/follow-up safety signals

## 1. CI/CD Regression Gates

### Fast PR gate (merge-blocking)

Run on every PR that touches replication/config/metrics paths:

```bash
make test-repl-regression-fast
```

The fast gate validates high-risk deterministic paths:

- oversized array update chunking
- mixed standard + pipeline follow-up generation
- worker flush-path synthetic integration
- chunk-limit boundary behavior
- follow-up guard fail/warn semantics

Workflow:

- `.github/workflows/repl-regression.yml` (`repl-fast-gate`)

### Extended chaos gate (nightly or manual)

Run nightly and via manual workflow dispatch:

```bash
make test-repl-regression-chaos
make bench-repl-chunking
```

This high-repeat gate catches flakiness and behavior drift under repeated pathological patterns.

Workflow:

- `.github/workflows/repl-regression.yml` (`repl-chaos-gate`)

## 2. Staging Validation (Atlas/mongos)

Run existing pytest integration suites in staging topology (replicaset + sharded).

Minimum recommendation:

- run existing E2E suites in `.github/workflows/e2etests.yml`
- include `tests/test_pipeline_updates.py`
- run with production-like worker settings and tuned variants

Suggested staging matrix:

1. conservative workers
2. auto workers
3. high workers (stress)

For each run, capture:

- `/status` progression
- `/metrics` snapshots
- logs for chunking warnings and overflow action behavior

## 3. Metrics to Watch

Chunking/overflow metrics:

- `percona_clustersync_mongodb_repl_update_chunking_triggered_total`
- `percona_clustersync_mongodb_repl_update_follow_up_ops_total{type=standard|pipeline}`
- `percona_clustersync_mongodb_repl_update_chunk_limit_hits_total{target=array_pipeline|non_array_set,reason=bytes|stages}`
- `percona_clustersync_mongodb_repl_update_follow_up_overflow_total{action=fail|warn}`
- `percona_clustersync_mongodb_repl_update_follow_up_per_event`
- `percona_clustersync_mongodb_repl_update_array_chunks_per_event`
- `percona_clustersync_mongodb_repl_update_array_max_stages_per_chunk`

## 4. Alerting Recommendations

Example PromQL alert ideas:

1. Overflow fail triggered

```promql
increase(percona_clustersync_mongodb_repl_update_follow_up_overflow_total{action="fail"}[5m]) > 0
```

2. Sudden chunking spike

```promql
rate(percona_clustersync_mongodb_repl_update_chunking_triggered_total[5m]) > <baseline_threshold>
```

3. High follow-up per event (p95)

```promql
histogram_quantile(
  0.95,
  sum(rate(percona_clustersync_mongodb_repl_update_follow_up_per_event_bucket[10m])) by (le)
) > <expected_threshold>
```

4. Stage-limit pressure sustained

```promql
increase(percona_clustersync_mongodb_repl_update_chunk_limit_hits_total{reason="stages"}[15m]) > <threshold>
```

## 5. New Safety Valve

Optional hard cap for pathological events:

- `--repl-max-follow-up-ops-per-event` / `PCSM_REPL_MAX_FOLLOW_UP_OPS_PER_EVENT`
- `--repl-follow-up-overflow-action` / `PCSM_REPL_FOLLOW_UP_OVERFLOW_ACTION`
  - `fail` (recommended for strict production safety)
  - `warn` (observability-first mode)

Recommended rollout policy:

1. Start with `warn` in staging to characterize workloads
2. Move to `fail` in production for strict guardrails
3. Tune threshold only after observing real metrics distribution

## 6. Final Confidence Checklist

Before production rollout:

- fast gate green on PR
- chaos gate green in nightly/manual run
- staging runs completed on Atlas/mongos with `test_pipeline_updates.py`
- alert rules configured and tested
- overflow counter verified at zero under expected workload profile
