# Replication BufBuilder Fix Validation Guide

This document defines the final confidence workflow for the BufBuilder / pipeline-chunking / worker-contention fix.

## Scope

This guide covers:

- CI/CD regression gates
- deterministic old-vs-new reproducibility
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

## 2. Deterministic Old-vs-New Reproducibility

Use the Python comparator to run the same deterministic regression suite on two refs.

```bash
python3 hack/repro_bufbuilder_fix.py compare-refs \
  --repo-root . \
  --old-ref v0.8.0 \
  --new-ref <new-ref-or-commit> \
  --output /tmp/pcsm_repro_compare.json
```

What it does:

1. Creates temporary worktrees for old and new refs
2. Runs deterministic high-risk regression tests
3. Runs high-repeat chaos regression tests
4. Runs chunking micro-benchmarks
5. Extracts pathological chunk stats snapshot from test logs
6. Emits structured JSON for comparison and CI artifacting

What it proves:

- whether the target ref passes critical BufBuilder/chunking regression tests
- whether follow-up chunking behavior is present in pathological synthetic events
- rough before-vs-after performance trend for chunking logic (`ns/op`, allocs)

What it does not prove:

- full production behavior under live Atlas/mongos network variance
- exact server-side AST memory behavior for every real customer workload

## 3. Staging Validation (Atlas/mongos)

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

## 4. Metrics to Watch

Chunking/overflow metrics:

- `percona_clustersync_mongodb_repl_update_chunking_triggered_total`
- `percona_clustersync_mongodb_repl_update_follow_up_ops_total{type=standard|pipeline}`
- `percona_clustersync_mongodb_repl_update_chunk_limit_hits_total{target=array_pipeline|non_array_set,reason=bytes|stages}`
- `percona_clustersync_mongodb_repl_update_follow_up_overflow_total{action=fail|warn}`
- `percona_clustersync_mongodb_repl_update_follow_up_per_event`
- `percona_clustersync_mongodb_repl_update_array_chunks_per_event`
- `percona_clustersync_mongodb_repl_update_array_max_stages_per_chunk`

## 5. Alerting Recommendations

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

## 6. New Safety Valve

Optional hard cap for pathological events:

- `--repl-max-follow-up-ops-per-event` / `PCSM_REPL_MAX_FOLLOW_UP_OPS_PER_EVENT`
- `--repl-follow-up-overflow-action` / `PCSM_REPL_FOLLOW_UP_OVERFLOW_ACTION`
  - `fail` (recommended for strict production safety)
  - `warn` (observability-first mode)

Recommended rollout policy:

1. Start with `warn` in staging to characterize workloads
2. Move to `fail` in production for strict guardrails
3. Tune threshold only after observing real metrics distribution

## 7. Final Confidence Checklist

Before production rollout:

- fast gate green on PR
- chaos gate green in nightly/manual run
- old-vs-new comparator report generated and archived
- staging runs completed on Atlas/mongos with `test_pipeline_updates.py`
- alert rules configured and tested
- overflow counter verified at zero under expected workload profile

