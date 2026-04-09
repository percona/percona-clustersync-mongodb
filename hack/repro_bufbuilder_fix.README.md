# BufBuilder Fix Reproduction and Validation Guide

## Overview
`hack/repro_bufbuilder_fix.py` is the engineering validation tool for the BufBuilder / oversized update pipeline fix.

It validates protections around:
- large mixed update events
- array-path pipeline chunking
- follow-up operation generation and overflow safeguards

It supports two workflows:
1. `compare-refs`: run deterministic regression/chaos/benchmark suites on **old vs new git refs**.
2. `compare-targets`: run a representative scenario command against **old vs new targets** (binary, deployment, or environment), with optional metrics scraping.

## What This Script Proves (and What It Does NOT Prove)
What it proves:
- old vs new code-path behavior can be compared in a repeatable way.
- regression suites for chunking/follow-up safety pass or fail deterministically.
- BufBuilder-like error signatures are detected from command output.
- metric deltas can show safeguard activity (chunking/follow-up counters).

What it does not prove:
- exact replay parity with all production oplog patterns.
- end-to-end behavior under every Atlas/sharded topology variant without staging validation.

## Prerequisites
- Python 3.10+
- Go toolchain available in PATH (for `compare-refs` mode)
- Git repo checked out locally (for `compare-refs`)
- Access to old/new service endpoints and optional `/metrics` endpoints (for `compare-targets`)

No external Python dependencies are required.

## Configuration
Key options:
- Ref mode:
  - `--repo-root`
  - `--old-ref`
  - `--new-ref`
  - `--chaos-count`
- Target mode:
  - `--old-command`
  - `--new-command`
  - `--old-metrics-url`
  - `--new-metrics-url`
  - `--command-timeout-sec`
  - `--metrics-timeout-sec`
  - `--env KEY=VALUE` (repeatable)

## How to Run (Step-by-Step)
### Step 1 — Run against OLD vs NEW refs
```bash
cd /path/to/percona-clustersync-mongodb
python3 hack/repro_bufbuilder_fix.py compare-refs \
  --repo-root . \
  --old-ref v0.8.0 \
  --new-ref HEAD \
  --output /tmp/pcsm_compare_refs.json
```

### Step 2 — Capture/report output
- Console prints comparison conclusions.
- Detailed JSON is written to `--output`.

### Step 3 — Run target/environment comparison (optional but recommended)
```bash
python3 hack/repro_bufbuilder_fix.py compare-targets \
  --old-command "./bin/pcsm-old run-pathological-scenario --config ./old.yaml" \
  --new-command "./bin/pcsm-new run-pathological-scenario --config ./new.yaml" \
  --old-metrics-url "http://old-host:8081/metrics" \
  --new-metrics-url "http://new-host:8081/metrics" \
  --output /tmp/pcsm_compare_targets.json
```

### Step 4 — Compare results
- Validate old target shows failure signal or unsafe behavior.
- Validate new target completes without BufBuilder signature and with expected safeguards.

## Expected Results
Old code (vulnerable baseline):
- may fail regression checks or emit BufBuilder-related errors.
- may show weak/no safe chunking behavior in pathological paths.

New code (fixed):
- regression suites pass.
- no BufBuilder error signature.
- chunking/follow-up metrics indicate safeguards are active when needed.

## Output Interpretation
Important fields in JSON output:
- `ok`: scenario success for that ref/target.
- `bufbuilder_signal_detected`: true means matching failure signature was found.
- `error_signals`: matched patterns (for triage).
- `metrics_delta`: before/after deltas for key counters.

Useful metrics:
- `repl_update_chunk_limit_hits_total`
- `repl_update_follow_up_per_event_count/sum`
- `repl_update_follow_up_overflow_total`
- `repl_update_chunking_triggered_total`

Good outcome:
- new target: `ok=true`, `bufbuilder_signal_detected=false`
- metrics show controlled chunking activity, no overflow failures

Bad outcome:
- new target: `ok=false` or `bufbuilder_signal_detected=true`
- overflow counters increase unexpectedly

## Troubleshooting
Connection failures:
- verify target URI/host/ports
- confirm `/metrics` endpoint accessibility

No reproduction difference observed:
- increase pathological workload size/intensity in scenario commands
- increase run count / chaos count
- ensure old target actually runs pre-fix code

Script command errors:
- test commands manually first
- verify `--cwd` and executable paths
- use `--env` for required runtime variables

## Staging / Atlas Validation
Run `compare-targets` against staging old/new environments:
- keep scenario controlled and repeatable
- run during low-risk validation windows
- capture both JSON outputs and service logs
- confirm no BufBuilder signals and healthy safeguard metric behavior on new build

## CI / Automation Usage
Recommended:
- PR gate: `compare-refs` (or equivalent targeted test subset)
- nightly/extended: higher `--chaos-count` and benchmark capture
- pre-release staging: `compare-targets` against real staging services

Example CI artifact:
- `/tmp/pcsm_compare_refs.json` uploaded for every validation run

## Quick Checklist
- [ ] Python and Go environments are ready
- [ ] Old ref/target validated
- [ ] New ref/target validated
- [ ] JSON outputs captured
- [ ] BufBuilder signal check reviewed
- [ ] Chunk/follow-up metrics deltas reviewed
- [ ] No overflow/failure signal on new target
- [ ] Staging run completed (for release confidence)
