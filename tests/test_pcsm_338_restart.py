import subprocess
import threading
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from typing import Final, NotRequired, TypedDict

import pytest
import requests
from bson.timestamp import Timestamp
from pymongo import ReplaceOne
from testing import Testing

pytestmark = pytest.mark.self_managed_pcsm

SOURCE_URI, TARGET_URI = "mongodb://rs00:30000", "mongodb://rs10:30100"
DATABASE, COLLECTION = "pcsm_338", "documents"
WRITE_PAYLOAD: Final = "w" * 4096


class StatusPayload(TypedDict):
    state: str
    initialSync: NotRequired[dict[str, object]]


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    document_id: int
    revision: int
    value: int
    operation_time: Timestamp


@dataclass(frozen=True, slots=True)
class CheckpointSnapshot:
    clone_finish_time: Timestamp
    checkpoint_op_time: Timestamp


type WaitCondition = tuple[str, float, Callable[[StatusPayload, CheckpointSnapshot | None], bool]]


def _checkpoint(t: Testing) -> CheckpointSnapshot | None:
    document = t.target["percona_clustersync_mongodb"]["checkpoints"].find_one({"_id": "pcsm"})
    if document is None:
        return None

    data = document["data"]
    clone = data.get("clone", {})
    repl = data.get("repl", {})
    return CheckpointSnapshot(
        clone_finish_time=clone.get("finishTS", Timestamp(0, 0)),
        checkpoint_op_time=repl.get("checkpointOpTS", Timestamp(0, 0)),
    )


def _wait(t: Testing, condition: WaitCondition) -> StatusPayload:
    name, timeout_seconds, predicate = condition
    deadline = time.monotonic() + timeout_seconds
    last_status: StatusPayload | None = None
    last_checkpoint: CheckpointSnapshot | None = None
    while time.monotonic() < deadline:
        try:
            status: StatusPayload = t.pcsm.status()
            last_status = status
            last_checkpoint = _checkpoint(t)
            if predicate(status, last_checkpoint):
                return status
        except requests.RequestException:
            pass
        time.sleep(0.1)

    pytest.fail(
        f"timeout waiting for {name}; status={last_status!r}; checkpoint={last_checkpoint!r}"
    )


def _start_pcsm(*, reset_state: bool) -> subprocess.Popen[bytes]:
    command = ["bin/pcsm_test", "--source", SOURCE_URI, "--target", TARGET_URI, "--log-level=trace"]
    if reset_state:
        command.append("--reset-state")
    return subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)


def _terminate(process: subprocess.Popen[bytes]) -> None:
    assert process.poll() is None, f"PCSM exited before terminate: {process.returncode}"
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=10)
        raise


@pytest.mark.timeout(300)
def test_documents_match_after_graceful_restart(t: Testing) -> None:
    source = t.source[DATABASE][COLLECTION]
    target = t.target[DATABASE][COLLECTION]
    seed_documents = [
        {"_id": i, "revision": 0, "value": i * 17, "payload": "s" * 65536} for i in range(2048)
    ]
    source.insert_many(seed_documents)

    ledger: list[LedgerEntry] = []
    checkpoint_marker: Future[LedgerEntry] = Future()
    writer_started, start_issued = threading.Event(), threading.Event()
    recovery_observed, abort_writes = threading.Event(), threading.Event()
    process: subprocess.Popen[bytes] | None = None

    try:
        process = _start_pcsm(reset_state=True)
        _wait(t, ("initial idle state", 15, lambda status, _cp: status["state"] == "idle"))
        assert process.poll() is None, f"initial PCSM exited with {process.returncode}"

        def write_through_restart() -> int:
            writes_after_recovery = 0
            sequence = 0
            burst_remaining = 0
            while True:
                if abort_writes.is_set():
                    return sequence
                batch_size = min(64, burst_remaining) if burst_remaining else 1
                recovery_active = recovery_observed.is_set()
                mark_write = start_issued.is_set() and target.find_one({"_id": 0}) is not None
                pending = [
                    pair
                    for revision in range(sequence + 1, sequence + batch_size + 1)
                    for pair in (
                        (10_000 + revision % 64, revision),
                        (1_000_000 + revision, revision),
                    )
                ]
                sequence += batch_size
                with t.source.start_session() as session:
                    source.bulk_write(
                        [
                            ReplaceOne(
                                {"_id": document_id},
                                {
                                    "_id": document_id,
                                    "revision": revision,
                                    "value": document_id * 338 + revision,
                                    "payload": WRITE_PAYLOAD,
                                },
                                upsert=True,
                            )
                            for document_id, revision in pending
                        ],
                        ordered=True,
                        session=session,
                    )
                    operation_time = session.operation_time
                assert isinstance(operation_time, Timestamp)
                ledger.extend(
                    LedgerEntry(document_id, revision, document_id * 338 + revision, operation_time)
                    for document_id, revision in pending
                )
                if not writer_started.is_set():
                    writer_started.set()
                if not start_issued.is_set():
                    continue
                if mark_write and not checkpoint_marker.done():
                    checkpoint_marker.set_result(ledger[-1])
                    burst_remaining = 8_192
                if burst_remaining:
                    burst_remaining -= batch_size
                if recovery_active:
                    writes_after_recovery += batch_size
                    if writes_after_recovery >= 64:
                        return sequence

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(write_through_restart)
            try:
                assert writer_started.wait(10), "writer did not acknowledge pre-start write"
                response = requests.post(
                    f"{t.pcsm.uri}/start",
                    json={
                        "includeNamespaces": [f"{DATABASE}.{COLLECTION}"],
                        "cloneNumInsertWorkers": 1,
                        "replNumWorkers": 1,
                        "replBulkOpsSize": 1,
                    },
                    timeout=10,
                )
                assert response.status_code == 200 and response.json() == {"ok": True}
                start_issued.set()
                marker = checkpoint_marker.result(timeout=10)
                _wait(
                    t,
                    (
                        f"post-clone catchup beyond marker {marker.document_id}/{marker.revision}",
                        45,
                        lambda status, checkpoint: (
                            checkpoint is not None
                            and checkpoint.clone_finish_time > Timestamp(0, 0)
                            and marker.operation_time <= checkpoint.clone_finish_time
                            and checkpoint.checkpoint_op_time >= marker.operation_time
                            and status.get("initialSync", {}).get("completed") is False
                        ),
                    ),
                )
                checkpoint_before_restart = _checkpoint(t)
                assert checkpoint_before_restart is not None

                assert not future.done(), f"writer stopped before restart: {future.exception()!r}"
                _terminate(process)
                process = _start_pcsm(reset_state=False)
                _wait(
                    t,
                    (
                        "recovered running state",
                        90,
                        lambda status, checkpoint: (
                            status["state"] == "running"
                            and checkpoint is not None
                            and checkpoint.checkpoint_op_time
                            > checkpoint_before_restart.checkpoint_op_time
                        ),
                    ),
                )
                assert process.poll() is None, f"recovered PCSM exited with {process.returncode}"
                recovery_observed.set()
                future.result(timeout=20)
            finally:
                abort_writes.set()

        result = source.replace_one(
            {"_id": -2},
            {"_id": -2, "revision": 2, "value": 676, "payload": WRITE_PAYLOAD},
            upsert=True,
        )
        final_time = (result.raw_result or {}).get("operationTime")
        assert isinstance(final_time, Timestamp)
        ledger.append(LedgerEntry(-2, 2, 676, final_time))
        _wait(
            t,
            (
                "final marker",
                45,
                lambda status, _checkpoint: (
                    status.get("initialSync", {}).get("completed") is True
                    and target.find_one({"_id": -2})
                    == {"_id": -2, "revision": 2, "value": 676, "payload": WRITE_PAYLOAD}
                ),
            ),
        )
        t.pcsm.finalize()
        _wait(t, ("finalized state", 30, lambda status, _cp: status["state"] == "finalized"))

        expected = {document["_id"]: document for document in seed_documents}
        for entry in ledger:
            expected[entry.document_id] = {
                "_id": entry.document_id,
                "revision": entry.revision,
                "value": entry.value,
                "payload": WRITE_PAYLOAD,
            }

        source_documents = {document["_id"]: document for document in source.find()}
        target_documents = {document["_id"]: document for document in target.find()}
        assert source_documents == expected, "source state diverged from acknowledged write ledger"

        source_ids = set(source_documents)
        target_ids = set(target_documents)
        missing_ids = sorted(source_ids - target_ids)
        unexpected_ids = sorted(target_ids - source_ids)
        mismatched_ids = sorted(
            document_id
            for document_id in source_ids & target_ids
            if source_documents[document_id] != target_documents[document_id]
        )
        if missing_ids or unexpected_ids or mismatched_ids:
            pytest.fail(
                f"post-restart mismatch: missing_ids={missing_ids}; "
                f"unexpected_ids={unexpected_ids}; value_mismatched_ids={mismatched_ids}; "
                f"checkpoint_before_restart={checkpoint_before_restart!r}; "
                f"checkpoint_after_finalize={_checkpoint(t)!r}"
            )
        t.compare_all()
    finally:
        abort_writes.set()
        if process is not None and process.poll() is None:
            _terminate(process)
