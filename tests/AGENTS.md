# PYTHON E2E KNOWLEDGE

## OVERVIEW

Score 5; retained below the creation threshold for the distinct live-system test domain: behavior tests, managed PCSM, HA drills, and assertions.

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
- Indirectly parametrize `managed_pcsm_args` with a list of server args to restart the suite-owned process for one test; teardown restores defaults. Requires `TEST_PCSM_BIN` or `--pcsm-bin`.
- `test_ha.py` is marked slow and needs `--runslow`; its group fixture also requires a PCSM binary.
- `test_hack_clis.py` overrides the autouse database-drop fixture.
- Keep CI scope in the contributing guide aligned with `.github/workflows/e2etests.yml` when test selection changes.
- `perf_test.go` benchmarks: `go test -run='^$' -bench=. -benchmem ./tests/`; `TEST_TARGET_URI` is required and copy benchmarks also need `TEST_SOURCE_URI`.

## ANTI-PATTERNS

- Do not leave the managed PCSM suspended after an HA test.
- Do not compare documents without deterministic ordering or raw-batch hashing.
- Do not add unbounded waits; every state/data wait needs a diagnostic timeout.
- Do not copy legacy polling sleeps into new async tests; subscribe before triggering an action and await its exact state/event signal with a bounded timeout.
- Do not include slow tests in default scope without the `slow` marker and `--runslow`.

<!-- End of init-deep baseline. -->

## PROJECT NOTES

Setup, environment/CLI precedence, and CI selection:
[CONTRIBUTING.md](../CONTRIBUTING.md#python-e2e-tests).
Read [the STANDBY API contract](../ha/README.md#api-on-standby) when testing HA responses.
