"""End-to-end coverage for movePrimary replay on sharded clusters.

movePrimary changes a collection's UUID on the recipient shard. On pre-8 mongos
sources the change stream emits a phantom ``create`` (new UUID) followed by a
``drop`` (old UUID); on 8.x only the phantom ``create`` surfaces. PCSM-249
(steps E/F/G) recovers from the invalidate, skips the replayed drop, and
resolves DML write-target namespaces by UUID at worker execution. These tests
exercise that full path end to end: movePrimary -> phantom create/drop DDL ->
replay skip -> source/target parity.

CI vs. local scope (design option c): only ``test_move_primary_steady_state_parity``
(4a) is non-slow and is collected by the tracked sharded CI lane on every
version pair (including the pre-8 bug surface). The slow scenarios
(concurrent writes, mixed sharded/unsharded, double-move replay) are
local/``--runslow`` hardening and are NOT run in CI.
"""

import itertools
import threading
import time

import pytest
from pymongo.errors import OperationFailure, WriteError
from testing import Testing, compare_namespace

from pcsm import Runner

# MongoDB rejects writes to a namespace while its movePrimary critical section is
# in progress (MovePrimaryInProgress, code 319). The concurrent-writes test
# treats an in-window rejection as expected proof of overlap, not a fatal error.
_MOVE_PRIMARY_IN_PROGRESS = 319


def _wait_until(predicate, timeout=30.0, interval=0.05):
    """Block until ``predicate()`` is truthy or ``timeout`` elapses.

    Returns the final predicate value (truthy on success, falsy on timeout) so
    callers can assert on it. This is a bounded poll, not a fixed-duration
    sleep: it returns as soon as the predicate holds.
    """
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= deadline:
            return value
        time.sleep(interval)


def _current_primary(config_db, db):
    """Return the primary shard id for ``db`` from config.databases."""
    entry = config_db.databases.find_one({"_id": db})
    assert entry is not None, f"database {db} missing from config.databases"
    return entry["primary"]


def _pick_target_shard(config_db, db):
    """Return ``(target_shard, original_primary)`` for a movePrimary off ``db``.

    Skips the test only when the deployment has no alternate shard (e.g.
    ``SRC_SHARDS=1``): movePrimary needs a destination other than the current
    primary. This pre-move None-guard is the ONLY legitimate skip. A movePrimary
    that the server refuses on a multi-shard deployment is a real failure and
    must propagate (design section 9 #3: no runtime OperationFailure skip).
    """
    original_primary = _current_primary(config_db, db)
    shard_ids = [s["_id"] for s in config_db.shards.find()]
    target_shard = next((s for s in shard_ids if s != original_primary), None)
    if target_shard is None:
        pytest.skip("movePrimary requires >= 2 source shards (e.g. SRC_SHARDS=1)")
    return target_shard, original_primary


@pytest.mark.timeout(180)
def test_move_primary_steady_state_parity(t: Testing):
    """movePrimary in steady-state apply, then post-move DML, then full parity.

    The CI regression guard. After the move changes the collection UUID it runs
    insert/update/delete so the step-F UUID-resolved DML write-target routing is
    exercised, not just the DDL replay skip.
    """
    db = "mp_db"
    coll = "docs"
    config_db = t.source.get_database("config")

    t.source.admin.command("enableSharding", db)
    target_shard, original_primary = _pick_target_shard(config_db, db)
    t.source[db][coll].insert_many([{"_id": i} for i in range(50)])

    with t.run(Runner.Phase.APPLY, wait_timeout=180):
        t.wait_target_count(db, coll, 50)

        t.source.admin.command("movePrimary", db, to=target_shard)

        # Post-move DML exercises UUID-resolved write-target routing (step F).
        t.source[db][coll].insert_many([{"_id": i} for i in range(50, 60)])
        t.wait_target_count(db, coll, 60)
        t.source[db][coll].update_many({"_id": {"$lt": 10}}, {"$set": {"v": 1}})
        t.source[db][coll].delete_many({"_id": {"$gte": 55}})

    primary_after = _current_primary(config_db, db)
    assert primary_after == target_shard and primary_after != original_primary
    t.compare_all_sharded()
    compare_namespace(t.source, t.target, db, coll)
    assert t.source[db][coll].count_documents({}) == 55


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_move_primary_concurrent_writes(t: Testing):
    """Writes overlapping the in-flight movePrimary must not be lost or duplicated.

    Overlap is proven at the command boundary: a writer thread inserts
    continuously while a dedicated mover thread runs movePrimary, and the test
    asserts at least one write either committed or was rejected with
    MovePrimaryInProgress (code 319) strictly between ``move_started`` and
    ``move_returned``. MongoDB blocks writes to the moving namespace during the
    movePrimary critical section, so an in-window rejection is expected and is
    itself proof of overlap. Other writer and mover exceptions are marshaled to
    the main thread and re-raised; any movePrimary failure fails the test (no
    skip branch, per design section 9 #3).
    """
    db = "mp_cc_db"
    coll = "docs"
    seed_count = 2000
    config_db = t.source.get_database("config")

    t.source.admin.command("enableSharding", db)
    target_shard, original_primary = _pick_target_shard(config_db, db)
    t.source[db][coll].insert_many([{"_id": i} for i in range(seed_count)])

    errors: list[Exception] = []
    mover_errors: list[Exception] = []
    committed = 0
    in_flight_commits = 0
    in_flight_blocked = 0
    lock = threading.Lock()
    stop_event = threading.Event()
    move_started = threading.Event()
    move_returned = threading.Event()
    id_seq = itertools.count(seed_count)

    def _writer():
        nonlocal committed, in_flight_commits, in_flight_blocked
        while not stop_event.is_set():
            try:
                t.source[db][coll].insert_one({"_id": next(id_seq)})
            except (OperationFailure, WriteError) as exc:
                # movePrimary blocks writes to the moving namespace during its
                # critical section (code 319). That rejection is expected and
                # proves the write attempt overlapped the move: count it and
                # keep going rather than treating it as a fatal error.
                if getattr(exc, "code", None) == _MOVE_PRIMARY_IN_PROGRESS:
                    with lock:
                        if move_started.is_set() and not move_returned.is_set():
                            in_flight_blocked += 1
                    continue
                errors.append(exc)
                return
            except Exception as exc:  # noqa: BLE001 - marshal to main thread
                errors.append(exc)
                return
            with lock:
                committed += 1
                if move_started.is_set() and not move_returned.is_set():
                    in_flight_commits += 1

    def _mover():
        try:
            move_started.set()
            t.source.admin.command("movePrimary", db, to=target_shard)
        except Exception as exc:  # noqa: BLE001 - marshal to main thread
            mover_errors.append(exc)
        finally:
            move_returned.set()

    writer = threading.Thread(target=_writer)
    mover = threading.Thread(target=_mover)
    mover_started = False
    final_count = seed_count

    with t.run(Runner.Phase.APPLY, wait_timeout=300):
        writer.start()
        try:
            assert _wait_until(lambda: committed >= 1 or errors, timeout=30), (
                "writer did not commit any insert before movePrimary"
            )
            if errors:
                raise errors[0]
            mover_started = True
            mover.start()
            mover.join(timeout=120)
            assert move_returned.is_set(), "movePrimary did not return in time"
            if mover_errors:
                raise mover_errors[0]
            with lock:
                assert in_flight_commits >= 1 or in_flight_blocked >= 1, (
                    "no write attempt overlapped the movePrimary window "
                    f"(committed_in_window={in_flight_commits}, "
                    f"blocked_in_window={in_flight_blocked})"
                )
            stop_event.set()
            writer.join(timeout=5)
            assert not writer.is_alive(), "writer thread did not exit after stop_event"
            with lock:
                final_count = seed_count + committed
            t.wait_target_count(db, coll, final_count, timeout=120)
        finally:
            stop_event.set()
            writer.join(timeout=5)
            if mover_started:
                mover.join(timeout=5)

    if errors:
        raise errors[0]
    assert final_count > seed_count
    primary_after = _current_primary(config_db, db)
    assert primary_after == target_shard and primary_after != original_primary
    t.compare_all_sharded()
    assert t.source[db][coll].count_documents({}) == final_count
    assert t.target[db][coll].count_documents({}) == final_count


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_move_primary_mixed_sharded_unsharded(t: Testing):
    """A moved database with both sharded and unsharded collections stays in parity."""
    db = "mp_mix_db"
    config_db = t.source.get_database("config")

    t.source.admin.command("enableSharding", db)
    target_shard, original_primary = _pick_target_shard(config_db, db)
    t.source[db]["plain"].insert_many([{"_id": i} for i in range(30)])
    t.source.admin.command("shardCollection", f"{db}.keyed", key={"_id": "hashed"})
    t.source[db]["keyed"].insert_many([{"_id": i} for i in range(30)])

    with t.run(Runner.Phase.APPLY, wait_timeout=300):
        t.wait_target_count(db, "plain", 30)
        t.wait_target_count(db, "keyed", 30)
        t.source.admin.command("movePrimary", db, to=target_shard)

    primary_after = _current_primary(config_db, db)
    assert primary_after == target_shard and primary_after != original_primary
    t.compare_all_sharded()


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_move_primary_replay_skip_no_dup_no_loss(t: Testing):
    """Two movePrimary cycles: replay skip yields no duplicates and no loss."""
    db = "mp_replay_db"
    coll = "docs"
    config_db = t.source.get_database("config")

    t.source.admin.command("enableSharding", db)
    target_shard, original_primary = _pick_target_shard(config_db, db)
    t.source[db][coll].insert_many([{"_id": i} for i in range(100)])

    with t.run(Runner.Phase.APPLY, wait_timeout=300):
        t.wait_target_count(db, coll, 100)

        t.source.admin.command("movePrimary", db, to=target_shard)
        t.source[db][coll].insert_many([{"_id": i} for i in range(100, 150)])
        t.wait_target_count(db, coll, 150, timeout=120)

        t.source.admin.command("movePrimary", db, to=original_primary)
        t.wait_target_count(db, coll, 150, timeout=120)

    assert _current_primary(config_db, db) == original_primary
    t.compare_all_sharded()
    compare_namespace(t.source, t.target, db, coll)
    assert t.source[db][coll].count_documents({}) == 150
    assert t.target[db][coll].count_documents({}) == 150
