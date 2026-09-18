# PYTHON E2E KNOWLEDGE

## OVERVIEW

Score: 9; distinct live-system test domain. Owns source-to-target behavior tests, optional PCSM process management, HA drills, and equivalence assertions.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change suite lifecycle | `conftest.py` | CLI/env options, Mongo clients, cleanup, managed PCSM process |
| Change HTTP test client | `pcsm.py` | API wrapper, state waits, clone/apply `Runner` phases |
| Change data assertions | `testing.py` | DB/collection/index/content comparison helpers |
| Change HA orchestration | `ha.py` | Multi-instance role and failover helpers |
| Add general E2E coverage | `test_*.py` | Feature-focused source operations and target assertions |
| Add sharded-only coverage | `test_*_sharded.py` | Shard metadata and sharded DDL cases |
| Add opt-in slow coverage | `test_slow.py` | Requires `--runslow` |
| Work on Go benchmarks | `perf_test.go` | Separate from pytest discovery; uses MongoDB helpers |

## CONVENTIONS

- Required connection inputs are `TEST_SOURCE_URI`, `TEST_TARGET_URI`, and `TEST_PCSM_URL`; pytest options override env values.
- `TEST_PCSM_BIN` enables suite-owned server startup and teardown.
- The autouse fixture drops non-system databases on source and target before each test.
- Use `Runner.Phase.CLONE` and `Runner.Phase.APPLY` when behavior must work during both initial copy and live replay.
- `PCSM.status()` is strict; HA tests use `raw_status()` when a STANDBY 409 body is the assertion target.
- `Testing.compare_all*` compares namespaces, options, indexes, counts, and raw-batch content hashes; raw batches default to `_id` ascending order.
- Request `suspend_managed_pcsm` for self-managed HA groups; its teardown restores the suite-owned process.
- `test_ha.py` is marked slow and needs `--runslow`; its group fixture also requires a PCSM binary.

## ANTI-PATTERNS

- Do not assume `/status` succeeds on every HA member; STANDBY is intentionally HTTP 409.
- Do not leave the managed PCSM suspended after an HA test.
- Do not compare documents without deterministic ordering or raw-batch hashing.
- Do not add unbounded waits; every state/data wait needs a diagnostic timeout.
- Do not copy legacy polling sleeps into new async tests; subscribe before triggering an action and await its exact state/event signal with a bounded timeout.
- Do not include slow tests in default scope without the `slow` marker and `--runslow`.
