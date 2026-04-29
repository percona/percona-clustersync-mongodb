# pylint: disable=missing-docstring,redefined-outer-name
import subprocess
import tempfile
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import conftest
import pytest
from pymongo.errors import OperationFailure
from requests import RequestException
from testing import Testing

from pcsm import PCSM, Runner

SHARD_KEY_LOG_MESSAGE = "could not extract exact shard key"


@contextmanager
def _borrow_pcsm_port(pcsm_bin: str, request: pytest.FixtureRequest):
    if not pcsm_bin:
        pytest.skip("TEST_PCSM_BIN or --pcsm-bin is required")

    if conftest.PCSM_PROC and conftest.PCSM_PROC.poll() is None:
        conftest.stop_pcsm(conftest.PCSM_PROC)
    conftest.PCSM_PROC = None

    try:
        yield
    finally:
        conftest.PCSM_PROC = conftest.start_pcsm(pcsm_bin, request)


@contextmanager
def _captured_pcsm(pcsm_bin: str, request: pytest.FixtureRequest, pcsm: PCSM):
    log_handle = tempfile.NamedTemporaryFile(
        "w", prefix="pcsm-move-primary-", suffix=".log", delete=False, encoding="utf-8"
    )
    log_path = Path(log_handle.name)
    proc = subprocess.Popen(
        [
            pcsm_bin,
            "--source",
            conftest.source_uri(request),
            "--target",
            conftest.target_uri(request),
            "--reset-state",
            "--log-level=debug",
        ],
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )

    try:
        _wait_for_pcsm(pcsm, proc, log_path)
        yield log_path
    finally:
        _stop_process(proc)
        log_handle.close()


def _wait_for_pcsm(pcsm: PCSM, proc: subprocess.Popen, log_path: Path):
    for _ in range(60):
        if proc.poll() is not None:
            log_content = log_path.read_text(encoding="utf-8", errors="replace")
            raise RuntimeError(f"PCSM exited before becoming ready:\n{log_content}")
        try:
            if pcsm.status()["state"] == PCSM.State.IDLE:
                return
        except (ConnectionError, RequestException):
            pass
        time.sleep(0.5)

    log_content = log_path.read_text(encoding="utf-8", errors="replace")
    raise RuntimeError(f"PCSM did not become ready:\n{log_content}")


def _stop_process(proc: subprocess.Popen):
    if proc.poll() is not None:
        return

    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def _assert_log_clean(*log_paths: Path):
    for log_path in log_paths:
        content = log_path.read_text(encoding="utf-8", errors="replace")
        matches = content.count(SHARD_KEY_LOG_MESSAGE)
        try:
            log_path.unlink()
        except FileNotFoundError:
            pass
        assert matches == 0, f"Found shard key error in PCSM logs: {log_path}"


def _move_primary_target(t: Testing, db_name: str):
    config_db = t.source.get_database("config")
    shards = list(config_db.shards.find())
    db_info = config_db.databases.find_one({"_id": db_name})

    if db_info is None:
        raise AssertionError(f"Database {db_name} not found in config.databases")

    current_primary = db_info.get("primary")
    target_shard = next((shard["_id"] for shard in shards if shard["_id"] != current_primary), None)
    if target_shard is None:
        pytest.skip("movePrimary requires at least two shards")

    return target_shard


def _move_primary(t: Testing, db_name: str, target_shard: str):
    try:
        t.source.admin.command("movePrimary", db_name, to=target_shard)
    except OperationFailure as exc:
        error = str(exc)
        if (
            "movePrimary" in error
            or "not supported" in error.lower()
            or "command not found" in error.lower()
        ):
            pytest.skip(f"movePrimary not supported: {exc}")
        raise

    db_info = t.source.get_database("config").databases.find_one({"_id": db_name})
    assert db_info is not None, f"Database {db_name} not found after movePrimary"
    assert db_info.get("primary") == target_shard, (
        f"Primary shard should be {target_shard}, got {db_info.get('primary')}"
    )


def _wait_for_count(collection, minimum_count: int, timeout: int = 30):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if collection.count_documents({}) >= minimum_count:
            return
        time.sleep(0.1)

    actual = collection.count_documents({})
    raise AssertionError(f"expected at least {minimum_count} docs, got {actual}")


def test_move_primary_forced_replay_pre_uuid_update(
    t: Testing, pcsm_bin: str, request: pytest.FixtureRequest
):
    db_name = "move_primary_replay_pre_uuid"
    coll_name = "test_coll"

    with _borrow_pcsm_port(pcsm_bin, request):
        t.source.admin.command("enableSharding", db_name)
        t.source[db_name][coll_name].insert_many(
            [{"_id": i, "value": f"doc-{i}"} for i in range(20)]
        )

        target_shard = _move_primary_target(t, db_name)
        _move_primary(t, db_name, target_shard)

        with _captured_pcsm(pcsm_bin, request, t.pcsm) as log_path:
            with t.run(Runner.Phase.CLONE):
                pass
            t.compare_all()

        _assert_log_clean(log_path)


def test_move_primary_forced_replay_post_uuid_update(
    t: Testing, pcsm_bin: str, request: pytest.FixtureRequest
):
    db_name = "move_primary_replay_post_uuid"
    coll_name = "test_coll"

    with _borrow_pcsm_port(pcsm_bin, request):
        t.source.admin.command("enableSharding", db_name)
        t.source[db_name][coll_name].insert_many(
            [{"_id": i, "value": f"doc-{i}"} for i in range(20)]
        )
        target_shard = _move_primary_target(t, db_name)

        with _captured_pcsm(pcsm_bin, request, t.pcsm) as first_log:
            with t.run(Runner.Phase.APPLY):
                _move_primary(t, db_name, target_shard)
            t.compare_all()

        with _captured_pcsm(pcsm_bin, request, t.pcsm) as second_log:
            with t.run(Runner.Phase.CLONE):
                pass
            t.compare_all()

        _assert_log_clean(first_log, second_log)


@pytest.mark.timeout(120)  # 6.0 invalidates change stream on movePrimary; reconnect overhead ~14s
def test_move_primary_concurrent_writes(t: Testing, pcsm_bin: str, request: pytest.FixtureRequest):
    db_name = "move_primary_concurrent_writes"
    coll_name = "test_coll"
    total_docs = 150
    collection = t.source[db_name][coll_name]
    progress = {"count": 0}
    progress_lock = threading.Lock()
    writer_errors = []

    def writer():
        try:
            for i in range(total_docs):
                collection.insert_one({"_id": i, "value": f"doc-{i}"})
                with progress_lock:
                    progress["count"] = i + 1
                time.sleep(0.03)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            writer_errors.append(exc)

    with _borrow_pcsm_port(pcsm_bin, request):
        t.source.admin.command("enableSharding", db_name)
        target_shard = _move_primary_target(t, db_name)
        writer_thread = threading.Thread(target=writer)
        writer_thread.start()
        _wait_for_count(collection, 10)

        with _captured_pcsm(pcsm_bin, request, t.pcsm) as log_path:
            with t.run(Runner.Phase.APPLY, wait_timeout=30):
                _move_primary(t, db_name, target_shard)
                with progress_lock:
                    after_move_primary_count = progress["count"]
                _wait_for_count(
                    collection,
                    min(after_move_primary_count + 30, total_docs),
                    timeout=60,
                )
                writer_thread.join(timeout=30)
                assert not writer_thread.is_alive(), "writer did not finish"
                assert not writer_errors, writer_errors

            source_count = collection.count_documents({})
            target_count = t.target[db_name][coll_name].count_documents({})
            assert source_count == target_count
            t.compare_all()

        _assert_log_clean(log_path)


def test_move_primary_multi_collection_same_db(
    t: Testing, pcsm_bin: str, request: pytest.FixtureRequest
):
    db_name = "move_primary_multi_collection"
    sharded_coll = "sharded_coll"
    unsharded_coll = "unsharded_coll"
    sharded_ns = f"{db_name}.{sharded_coll}"

    with _borrow_pcsm_port(pcsm_bin, request):
        t.source.admin.command("enableSharding", db_name)
        t.source.admin.command("shardCollection", sharded_ns, key={"_id": 1})
        t.source[db_name][sharded_coll].insert_many(
            [{"_id": i, "value": f"sharded-{i}"} for i in range(20)]
        )
        t.source[db_name][unsharded_coll].insert_many(
            [{"_id": i, "value": f"unsharded-{i}"} for i in range(20)]
        )
        target_shard = _move_primary_target(t, db_name)

        with _captured_pcsm(pcsm_bin, request, t.pcsm) as log_path:
            with t.run(Runner.Phase.APPLY):
                _move_primary(t, db_name, target_shard)

            t.compare_all_sharded()
            source_config = t.source["config"]["collections"].find_one({"_id": sharded_ns})
            target_config = t.target["config"]["collections"].find_one({"_id": sharded_ns})
            assert source_config is not None
            assert target_config is not None
            assert source_config["key"] == {"_id": 1}
            assert target_config["key"] == source_config["key"]

        _assert_log_clean(log_path)
