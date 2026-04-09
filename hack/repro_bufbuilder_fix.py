#!/usr/bin/env python3
"""
Production-grade reproduction/validation tool for BufBuilder and update pipeline chunking safety.

This tool supports two practical validation paths:
1) compare-refs: deterministic old-vs-new validation by running regression tests/benchmarks on two git refs.
2) compare-targets: run a pathological scenario command against old/new targets (binary/service/env),
   optionally scrape Prometheus metrics before/after, and produce a structured comparison report.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from typing import Any


FAST_TEST_PATTERN = (
    "Test(CollectUpdateOpsWithPipeline_ChunksArrayStagesIntoFollowUpPipelines|"
    "CollectUpdateOpsWithPipeline_StatsTrackStageLimitChunking|"
    "CollectUpdateOpsWithPipeline_StatsTrackByteLimitChunking|"
    "CollectionBulkWriterUpdate_MixedFollowUpsAreAccepted|"
    "CollectionBulkWriterUpdate_FollowUpGuardFailFast|"
    "CollectionBulkWriterUpdate_FollowUpGuardWarnMode|"
    "Worker_EndToEndLargeMixedUpdateEvent_ChunkingAndFlush|"
    "ChunkPipelineStages_BoundaryAtMaxFields|"
    "ChunkPipelineStages_BoundaryAboveMaxFields|"
    "CollectUpdateOpsWithPipeline_ExtremeArrayUpdate_10k|"
    "OptionsApplyDefaults_FollowUpOverflowAction|"
    "CollectUpdateOpsWithPipeline_PathologicalStatsSnapshot)"
)

CHAOS_TEST_PATTERN = (
    "Test(CollectUpdateOpsWithPipeline_ChunksArrayStagesIntoFollowUpPipelines|"
    "CollectUpdateOpsWithPipeline_StatsTrackStageLimitChunking|"
    "CollectUpdateOpsWithPipeline_StatsTrackByteLimitChunking|"
    "CollectionBulkWriterUpdate_MixedFollowUpsAreAccepted|"
    "CollectionBulkWriterUpdate_FollowUpGuardFailFast|"
    "CollectionBulkWriterUpdate_FollowUpGuardWarnMode|"
    "Worker_EndToEndLargeMixedUpdateEvent_ChunkingAndFlush|"
    "ChunkPipelineStages_BoundaryAtMaxFields|"
    "ChunkPipelineStages_BoundaryAboveMaxFields|"
    "CollectUpdateOpsWithPipeline_ExtremeArrayUpdate_10k)"
)

PATHOLOGICAL_STATS_RE = re.compile(r"CHUNK_STATS:\s+(\{.*\})")
BENCH_RE = re.compile(
    r"^(BenchmarkCollectUpdateOpsWithPipeline_[^\s]+)\s+\d+\s+([\d.]+)\s+ns/op\s+([\d]+)\s+B/op\s+([\d]+)\s+allocs/op$"
)

BUFBUILDER_PATTERNS = [
    re.compile(r"BufBuilder attempted to grow", re.IGNORECASE),
    re.compile(r"125MB limit", re.IGNORECASE),
]

METRIC_FAMILIES = [
    "repl_update_chunk_limit_hits_total",
    "repl_update_follow_up_overflow_total",
    "repl_update_chunking_triggered_total",
    "repl_update_follow_up_ops_total",
    "repl_update_follow_up_per_event_count",
    "repl_update_follow_up_per_event_sum",
    "repl_update_array_chunks_per_event_count",
    "repl_update_array_chunks_per_event_sum",
    "repl_update_array_max_stages_per_chunk_count",
    "repl_update_array_max_stages_per_chunk_sum",
]


@dataclass
class CommandResult:
    command: list[str]
    returncode: int
    duration_seconds: float
    stdout: str
    stderr: str


@dataclass
class BenchPoint:
    name: str
    ns_per_op: float
    bytes_per_op: int
    allocs_per_op: int


@dataclass
class RefRunSummary:
    ref: str
    worktree: str
    fast_tests_ok: bool
    chaos_tests_ok: bool
    benchmark_ok: bool
    pathological_stats: dict[str, Any]
    benchmarks: list[BenchPoint]
    fast_tests: CommandResult
    chaos_tests: CommandResult
    benchmark: CommandResult


@dataclass
class ComparisonSummary:
    old_ref: str
    new_ref: str
    old_ok: bool
    new_ok: bool
    conclusions: list[str]


@dataclass
class TargetRunSummary:
    name: str
    command: list[str]
    cwd: str
    metrics_url: str | None
    ok: bool
    returncode: int
    duration_seconds: float
    bufbuilder_signal_detected: bool
    error_signals: list[str]
    metrics_before: dict[str, float]
    metrics_after: dict[str, float]
    metrics_delta: dict[str, float]
    stdout_tail: str
    stderr_tail: str


def run_cmd(cmd: list[str], cwd: pathlib.Path, timeout_sec: int | None = None, env: dict[str, str] | None = None) -> CommandResult:
    start = time.time()
    proc = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
        timeout=timeout_sec,
        env=env,
    )
    return CommandResult(
        command=cmd,
        returncode=proc.returncode,
        duration_seconds=round(time.time() - start, 3),
        stdout=proc.stdout,
        stderr=proc.stderr,
    )


def parse_pathological_stats(output: str) -> dict[str, Any]:
    for line in output.splitlines():
        m = PATHOLOGICAL_STATS_RE.search(line)
        if not m:
            continue

        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            return {}

    return {}


def parse_benchmarks(output: str) -> list[BenchPoint]:
    points: list[BenchPoint] = []
    for line in output.splitlines():
        m = BENCH_RE.match(line.strip())
        if not m:
            continue

        points.append(
            BenchPoint(
                name=m.group(1),
                ns_per_op=float(m.group(2)),
                bytes_per_op=int(m.group(3)),
                allocs_per_op=int(m.group(4)),
            )
        )

    return points


def with_worktree(repo: pathlib.Path, ref: str, fn):
    wt_root = pathlib.Path(tempfile.mkdtemp(prefix="pcsm-repro-"))
    try:
        add = run_cmd(["git", "worktree", "add", "--detach", str(wt_root), ref], repo)
        if add.returncode != 0:
            raise RuntimeError(f"failed to create worktree for {ref}: {add.stderr}\n{add.stdout}")

        return fn(wt_root)
    finally:
        run_cmd(["git", "worktree", "remove", "--force", str(wt_root)], repo)
        shutil.rmtree(wt_root, ignore_errors=True)


def run_ref_suite(repo: pathlib.Path, ref: str, chaos_count: int) -> RefRunSummary:
    def _run(worktree: pathlib.Path) -> RefRunSummary:
        fast_cmd = [
            "go",
            "test",
            "./pcsm/repl",
            "-count=1",
            "-run",
            FAST_TEST_PATTERN,
            "-v",
        ]
        chaos_cmd = [
            "go",
            "test",
            "./pcsm/repl",
            f"-count={chaos_count}",
            "-run",
            CHAOS_TEST_PATTERN,
        ]
        bench_cmd = [
            "go",
            "test",
            "./pcsm/repl",
            "-run",
            "^$",
            "-bench",
            "BenchmarkCollectUpdateOpsWithPipeline_",
            "-benchmem",
            "-count=1",
        ]

        fast = run_cmd(fast_cmd, worktree)
        chaos = run_cmd(chaos_cmd, worktree)
        bench = run_cmd(bench_cmd, worktree)

        return RefRunSummary(
            ref=ref,
            worktree=str(worktree),
            fast_tests_ok=fast.returncode == 0,
            chaos_tests_ok=chaos.returncode == 0,
            benchmark_ok=bench.returncode == 0,
            pathological_stats=parse_pathological_stats(fast.stdout),
            benchmarks=parse_benchmarks(bench.stdout),
            fast_tests=fast,
            chaos_tests=chaos,
            benchmark=bench,
        )

    return with_worktree(repo, ref, _run)


def compare_refs(old: RefRunSummary, new: RefRunSummary) -> ComparisonSummary:
    conclusions: list[str] = []

    old_ok = old.fast_tests_ok and old.chaos_tests_ok and old.benchmark_ok
    new_ok = new.fast_tests_ok and new.chaos_tests_ok and new.benchmark_ok

    if not old_ok:
        conclusions.append("Old ref failed one or more suites (expected for vulnerable versions).")
    if not new_ok:
        conclusions.append("New ref failed one or more suites (fix validation failed).")

    if old.pathological_stats and new.pathological_stats:
        old_follow = int(old.pathological_stats.get("followUpTotal", 0))
        new_follow = int(new.pathological_stats.get("followUpTotal", 0))
        conclusions.append(f"Pathological follow-up ops snapshot: old={old_follow}, new={new_follow}.")

    old_bench = {p.name: p for p in old.benchmarks}
    new_bench = {p.name: p for p in new.benchmarks}
    for key in sorted(set(old_bench) & set(new_bench)):
        o = old_bench[key]
        n = new_bench[key]
        delta = ((n.ns_per_op - o.ns_per_op) / o.ns_per_op) * 100.0 if o.ns_per_op else 0.0
        conclusions.append(f"{key}: old={o.ns_per_op:.0f}ns/op new={n.ns_per_op:.0f}ns/op delta={delta:+.2f}%")

    if not conclusions:
        conclusions.append("Comparison completed with no additional notes.")

    return ComparisonSummary(
        old_ref=old.ref,
        new_ref=new.ref,
        old_ok=old_ok,
        new_ok=new_ok,
        conclusions=conclusions,
    )


def parse_prometheus_metrics(text: str) -> dict[str, float]:
    out: dict[str, float] = {}
    for line in text.splitlines():
        raw = line.strip()
        if not raw or raw.startswith("#"):
            continue

        parts = raw.split()
        if len(parts) < 2:
            continue

        name_with_labels = parts[0]
        value_raw = parts[-1]

        metric_name = name_with_labels.split("{", 1)[0]
        try:
            value = float(value_raw)
        except ValueError:
            continue

        out[metric_name] = out.get(metric_name, 0.0) + value

    return out


def scrape_metrics(url: str, timeout_sec: int) -> dict[str, float]:
    req = urllib.request.Request(url, headers={"Accept": "text/plain"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            payload = resp.read().decode("utf-8", errors="replace")
    except urllib.error.URLError as exc:
        raise RuntimeError(f"unable to scrape metrics from {url}: {exc}") from exc

    parsed = parse_prometheus_metrics(payload)
    selected = {k: parsed.get(k, 0.0) for k in METRIC_FAMILIES}
    return selected


def merge_env(extra: list[str]) -> dict[str, str]:
    env = dict()
    for item in extra:
        if "=" not in item:
            raise ValueError(f"invalid --env value {item!r}; expected KEY=VALUE")
        key, value = item.split("=", 1)
        env[key] = value
    return env


def extract_signals(output: str) -> list[str]:
    signals: list[str] = []
    for pat in BUFBUILDER_PATTERNS:
        if pat.search(output):
            signals.append(pat.pattern)
    return signals


def tail(s: str, lines: int = 60) -> str:
    parts = s.splitlines()
    if len(parts) <= lines:
        return s
    return "\n".join(parts[-lines:])


def run_target(
    name: str,
    cmd: list[str],
    cwd: pathlib.Path,
    metrics_url: str | None,
    metrics_timeout_sec: int,
    timeout_sec: int,
    env_overrides: dict[str, str],
) -> TargetRunSummary:
    env = None
    if env_overrides:
        env = os.environ.copy()
        env.update(env_overrides)

    metrics_before: dict[str, float] = {}
    metrics_after: dict[str, float] = {}
    metrics_delta: dict[str, float] = {}

    if metrics_url:
        metrics_before = scrape_metrics(metrics_url, metrics_timeout_sec)

    try:
        result = run_cmd(cmd, cwd, timeout_sec=timeout_sec, env=env)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout or ""
        stderr = exc.stderr or ""
        error_signals = extract_signals(stdout + "\n" + stderr)
        return TargetRunSummary(
            name=name,
            command=cmd,
            cwd=str(cwd),
            metrics_url=metrics_url,
            ok=False,
            returncode=124,
            duration_seconds=float(timeout_sec),
            bufbuilder_signal_detected=bool(error_signals),
            error_signals=error_signals + ["command_timeout"],
            metrics_before=metrics_before,
            metrics_after=metrics_after,
            metrics_delta=metrics_delta,
            stdout_tail=tail(stdout),
            stderr_tail=tail(stderr),
        )

    if metrics_url:
        metrics_after = scrape_metrics(metrics_url, metrics_timeout_sec)
        for key in set(metrics_before) | set(metrics_after):
            metrics_delta[key] = round(metrics_after.get(key, 0.0) - metrics_before.get(key, 0.0), 6)

    combined = result.stdout + "\n" + result.stderr
    error_signals = extract_signals(combined)

    ok = result.returncode == 0 and not error_signals

    return TargetRunSummary(
        name=name,
        command=cmd,
        cwd=str(cwd),
        metrics_url=metrics_url,
        ok=ok,
        returncode=result.returncode,
        duration_seconds=result.duration_seconds,
        bufbuilder_signal_detected=bool(error_signals),
        error_signals=error_signals,
        metrics_before=metrics_before,
        metrics_after=metrics_after,
        metrics_delta=metrics_delta,
        stdout_tail=tail(result.stdout),
        stderr_tail=tail(result.stderr),
    )


def parse_cmd(raw: str) -> list[str]:
    parsed = shlex.split(raw)
    if not parsed:
        raise ValueError("empty command is not allowed")
    return parsed


def cmd_compare_refs(args: argparse.Namespace) -> int:
    repo = pathlib.Path(args.repo_root).resolve()

    old_summary = run_ref_suite(repo, args.old_ref, args.chaos_count)
    new_summary = run_ref_suite(repo, args.new_ref, args.chaos_count)
    cmp_summary = compare_refs(old_summary, new_summary)

    payload = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "compare-refs",
        "old": asdict(old_summary),
        "new": asdict(new_summary),
        "comparison": asdict(cmp_summary),
    }

    output = pathlib.Path(args.output).resolve()
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(asdict(cmp_summary), indent=2))
    print(f"\nWrote detailed report: {output}")

    return 0 if cmp_summary.new_ok else 2


def cmd_compare_targets(args: argparse.Namespace) -> int:
    cwd = pathlib.Path(args.cwd).resolve()

    old = run_target(
        name="old",
        cmd=parse_cmd(args.old_command),
        cwd=cwd,
        metrics_url=args.old_metrics_url,
        metrics_timeout_sec=args.metrics_timeout_sec,
        timeout_sec=args.command_timeout_sec,
        env_overrides=merge_env(args.env),
    )
    new = run_target(
        name="new",
        cmd=parse_cmd(args.new_command),
        cwd=cwd,
        metrics_url=args.new_metrics_url,
        metrics_timeout_sec=args.metrics_timeout_sec,
        timeout_sec=args.command_timeout_sec,
        env_overrides=merge_env(args.env),
    )

    conclusions: list[str] = []
    if old.bufbuilder_signal_detected and not new.bufbuilder_signal_detected:
        conclusions.append("Old target emitted BufBuilder-like signals while new target did not.")
    if old.ok and new.ok:
        conclusions.append("Both targets completed successfully for this scenario.")
    if not new.ok:
        conclusions.append("New target still failed scenario or emitted failure signal.")

    if old.metrics_delta or new.metrics_delta:
        conclusions.append("Metrics deltas captured; compare chunking/follow-up counters for validation.")

    payload = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "mode": "compare-targets",
        "old": asdict(old),
        "new": asdict(new),
        "conclusions": conclusions,
    }

    output = pathlib.Path(args.output).resolve()
    output.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(json.dumps(payload["conclusions"], indent=2))
    print(f"\nWrote detailed report: {output}")

    return 0 if new.ok else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BufBuilder/chunking reproduction and validation tool")
    sub = parser.add_subparsers(dest="command", required=True)

    compare_refs = sub.add_parser(
        "compare-refs",
        help="Run deterministic regression suites on old and new git refs and compare outputs",
    )
    compare_refs.add_argument("--repo-root", default=".", help="Path to repo root")
    compare_refs.add_argument("--old-ref", required=True, help="Old git ref (for example v0.8.0)")
    compare_refs.add_argument("--new-ref", required=True, help="New git ref (for example HEAD)")
    compare_refs.add_argument("--chaos-count", type=int, default=20, help="Repeat count for chaos suite")
    compare_refs.add_argument(
        "--output",
        default="./bufbuilder_repro_compare.json",
        help="Output JSON report path",
    )
    compare_refs.set_defaults(func=cmd_compare_refs)

    compare_targets = sub.add_parser(
        "compare-targets",
        help="Run a pathological scenario command against old and new targets and compare metrics/signals",
    )
    compare_targets.add_argument("--old-command", required=True, help="Command to run old target scenario")
    compare_targets.add_argument("--new-command", required=True, help="Command to run new target scenario")
    compare_targets.add_argument("--cwd", default=".", help="Working directory for scenario commands")
    compare_targets.add_argument("--old-metrics-url", default=None, help="Old target Prometheus /metrics URL")
    compare_targets.add_argument("--new-metrics-url", default=None, help="New target Prometheus /metrics URL")
    compare_targets.add_argument("--metrics-timeout-sec", type=int, default=10, help="Timeout for metrics scrape")
    compare_targets.add_argument("--command-timeout-sec", type=int, default=1800, help="Scenario command timeout")
    compare_targets.add_argument(
        "--env",
        action="append",
        default=[],
        help="Environment override KEY=VALUE (can be repeated)",
    )
    compare_targets.add_argument(
        "--output",
        default="./bufbuilder_target_compare.json",
        help="Output JSON report path",
    )
    compare_targets.set_defaults(func=cmd_compare_targets)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
