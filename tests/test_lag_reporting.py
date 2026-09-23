"""Black-box regression coverage for truthful replication lag and frontier progress."""

import time
from dataclasses import dataclass

import bson
import pytest
from testing import Testing

from pcsm import PCSM, Runner


@pytest.mark.slow
@pytest.mark.timeout(120)
def test_lag_reporting_during_resume_backlog(t: Testing):
    """The reported frontier must not overtake acknowledged target writes."""
    seed_count = 10
    total_docs = 60_000
    batch_size = 1_000
    source_coll = t.source["db_1"]["coll_1"]
    source_coll.insert_many([{"_id": i} for i in range(seed_count)])

    with t.run(Runner.Phase.APPLY, wait_timeout=20, options={"repl_num_workers": 1}) as runner:
        runner.wait_for_initial_sync()
        t.pcsm.pause()
        runner.wait_for_state(PCSM.State.PAUSED)

        batch_op_times: list[bson.Timestamp] = []
        for start in range(seed_count, seed_count + total_docs, batch_size):
            docs = [{"_id": i, "payload": "x" * 1024} for i in range(start, start + batch_size)]
            with t.source.start_session() as session:
                source_coll.insert_many(docs, session=session)
                op_time = session.operation_time
                assert op_time is not None, "acknowledged insert has no session operationTime"
                batch_op_times.append(op_time)

        # Fresh writes can legitimately have <= 1s lag, so the backlog must be
        # at least 3s old on the source's logical clock before lag is asserted
        # (3s also covers PCSM's +1s oplog-note rounding). The logical clock
        # follows wall time but only ticks on writes: wait out the wall-clock
        # gap, then append one oplog note to materialize the advance. Reads such
        # as `hello` do not move the clock, so polling them would depend on the
        # server's periodic no-op writer.
        age_target = batch_op_times[-1].time + 3
        remaining = age_target - time.time()
        if remaining > 0:
            time.sleep(remaining)
        note = t.source.admin.command({"appendOplogNote": 1, "data": {"msg": "test:age_backlog"}})
        assert note["$clusterTime"]["clusterTime"].time >= age_target, (
            f"source clock {note['$clusterTime']['clusterTime']} did not reach {age_target}"
        )

        before_resume = t.pcsm.status()
        assert before_resume["state"] == PCSM.State.PAUSED, before_resume
        applied_before_resume = before_resume["eventsApplied"]
        deadline = time.monotonic() + 90
        backlog_samples = 0
        t.pcsm.resume()

        # Use one status response for both the counter and the frontier. No
        # sleeps: every sample while catch-up is in progress is checked.
        while time.monotonic() < deadline:
            status = t.pcsm.status()
            assert status["state"] == PCSM.State.RUNNING, status
            applied_since_resume = status["eventsApplied"] - applied_before_resume
            assert applied_since_resume >= 0, status
            if applied_since_resume >= total_docs:
                break

            backlog_samples += 1
            reported_t, reported_i = status["lastReplicatedOpTime"]["ts"].split(".")
            reported = bson.Timestamp(int(reported_t), int(reported_i))
            batch_index = min(len(batch_op_times) - 1, applied_since_resume // batch_size)
            upper_bound = batch_op_times[batch_index].time + 1
            detail = (
                f"applied_since_resume={applied_since_resume}/{total_docs}, "
                f"batch={batch_index}, batch_op_time={batch_op_times[batch_index]}, "
                f"reported={reported}, lagTimeSeconds={status['lagTimeSeconds']}"
            )
            assert reported.time <= upper_bound, f"replication frontier ran ahead: {detail}"
            if applied_since_resume < total_docs - batch_size:
                assert status["lagTimeSeconds"] > 1, f"lag vanished with a backlog: {detail}"
        else:
            pytest.fail(f"backlog did not catch up within 90s; last status: {status}")

        assert backlog_samples > 0, "catch-up completed before any backlog status was sampled"
        assert t.target["db_1"]["coll_1"].count_documents({}) == seed_count + total_docs


@pytest.mark.slow
@pytest.mark.timeout(120)
def test_lag_frontier_advances_during_catchup_barrier(t: Testing):
    """PCSM-383: worker commits must advance the frontier during a DDL barrier."""
    seed_count = 10
    total_docs = 4_000
    batch_size = 500
    source_coll = t.source["db_1"]["coll_1"]
    source_coll.insert_many([{"_id": i} for i in range(seed_count)])

    # The backlog fits in the worker queue, letting the dispatcher reach the
    # create event while the single worker still has many small bulks to apply.
    options = {"repl_num_workers": 1, "repl_bulk_ops_size": 1}
    with t.run(Runner.Phase.APPLY, wait_timeout=20, options=options) as runner:
        runner.wait_for_initial_sync()
        t.pcsm.pause()
        runner.wait_for_state(PCSM.State.PAUSED)

        batch_op_times: list[bson.Timestamp] = []
        for start in range(seed_count, seed_count + total_docs, batch_size):
            docs = [{"_id": i, "payload": "x" * 200} for i in range(start, start + batch_size)]
            with t.source.start_session() as session:
                source_coll.insert_many(docs, session=session)
                op_time = session.operation_time
                assert op_time is not None, "acknowledged insert has no session operationTime"
                batch_op_times.append(op_time)

        # This must be a new collection, created AFTER the backlog and BEFORE
        # resume: its create DDL parks the dispatcher behind the queued writes.
        t.source["db_1"]["barrier_coll"].insert_one({"_id": 0})
        before_resume = t.pcsm.status()
        assert before_resume["state"] == PCSM.State.PAUSED, before_resume
        applied_before_resume = before_resume["eventsApplied"]
        initial_optime = before_resume["lastReplicatedOpTime"]["ts"]
        frozen_window = _FrozenWindow(initial_optime, applied_before_resume, time.monotonic())
        deadline = frozen_window.since + 90
        progress_samples = 0
        frozen_failure = None
        t.pcsm.resume()

        while time.monotonic() < deadline:
            status = t.pcsm.status()
            sampled_at = time.monotonic()
            assert status["state"] == PCSM.State.RUNNING, status
            applied = status["eventsApplied"]
            applied_since_resume = applied - applied_before_resume
            assert applied_since_resume >= 0, status
            optime = status["lastReplicatedOpTime"]["ts"]
            if frozen_failure is None:
                frozen_failure = frozen_window.sample(
                    optime,
                    applied,
                    sampled_at,
                    f"applied_since_resume={applied_since_resume}/{total_docs}, "
                    f"lagTimeSeconds={status['lagTimeSeconds']}",
                )
            if applied_since_resume >= total_docs:
                break

            if applied_since_resume > 0:
                progress_samples += 1
            reported = _parse_optime(optime)
            batch_index = min(len(batch_op_times) - 1, applied_since_resume // batch_size)
            upper_bound = batch_op_times[batch_index].time + 1
            assert reported.time <= upper_bound, (
                "replication frontier ran ahead: "
                f"applied_since_resume={applied_since_resume}/{total_docs}, "
                f"batch={batch_index}, batch_op_time={batch_op_times[batch_index]}, "
                f"reported={reported}, lagTimeSeconds={status['lagTimeSeconds']}"
            )
            # Elapsed time is the behavior under test: sample the frozen
            # frontier window at the same cadence as its 500ms progress timer.
            time.sleep(0.5)
        else:
            pytest.fail(f"backlog did not catch up within 90s; last status: {status}")

        assert progress_samples > 0, "no worker progress sampled before backlog completion"

        # The backlog counter can complete before the DDL and its insert apply.
        runner.wait_for_current_optime()
        assert t.target["db_1"]["coll_1"].count_documents({}) == seed_count + total_docs
        assert t.target["db_1"]["barrier_coll"].count_documents({}) == 1

    # Drain before failing: Runner's error cleanup otherwise finalizes while
    # the dispatcher is still in the barrier and can mask this assertion.
    assert frozen_failure is None, frozen_failure


def _worker_for(doc_id, num_workers: int) -> int:
    """Mirror pcsm/repl hashDocumentKey: FNV-1a 32 over the raw documentKey bytes."""
    h = 0x811C9DC5
    for b in bson.encode({"_id": doc_id}):
        h = ((h ^ b) * 0x01000193) & 0xFFFFFFFF
    return h % num_workers


def _ids_for_worker(prefix: str, worker: int, num_workers: int, count: int) -> list[str]:
    ids: list[str] = []
    n = 0
    while len(ids) < count:
        doc_id = f"{prefix}-{n}"
        n += 1
        if _worker_for(doc_id, num_workers) == worker:
            ids.append(doc_id)
    return ids


def _parse_optime(ts: str) -> bson.Timestamp:
    t_part, i_part = ts.split(".")
    return bson.Timestamp(int(t_part), int(i_part))


@dataclass
class _FrozenWindow:
    optime: str
    applied: int
    since: float

    def sample(self, optime: str, applied: int, sampled_at: float, detail: str) -> str | None:
        if optime != self.optime:
            self.optime, self.applied, self.since = optime, applied, sampled_at

        # Flat applied samples remain in the window; only a frontier change resets its start.
        if applied > self.applied and (duration := sampled_at - self.since) >= 3:
            return (
                f"lastReplicatedOpTime stood still at {self.optime} for "
                f"{duration:.1f}s while {applied - self.applied} events were applied; {detail}"
            )
        return None


@pytest.mark.slow
@pytest.mark.timeout(120)
def test_lag_frontier_not_pinned_by_drained_worker(t: Testing):
    """PCSM-383: a worker that drained early must not freeze reporting.

    Two workers. Worker 0 gets one document and drains at once; worker 1 gets
    the whole backlog and applies it one bulk at a time behind a create DDL
    barrier. The reported frontier must follow worker 1's commits, never run
    ahead of them, and the persisted resume floor is out of scope here.
    """
    num_workers = 2
    seed_count = 10
    hot_docs = 3_000
    batch_size = 500
    source_coll = t.source["db_1"]["coll_1"]
    source_coll.insert_many([{"_id": i} for i in range(seed_count)])
    cold_id = _ids_for_worker("cold", 0, num_workers, 1)[0]
    hot_ids = _ids_for_worker("hot", 1, num_workers, hot_docs)

    options = {"repl_num_workers": num_workers, "repl_bulk_ops_size": 1}
    with t.run(Runner.Phase.APPLY, wait_timeout=20, options=options) as runner:
        runner.wait_for_initial_sync()
        t.pcsm.pause()
        runner.wait_for_state(PCSM.State.PAUSED)

        with t.source.start_session() as session:
            source_coll.insert_one({"_id": cold_id, "payload": "c"}, session=session)
            cold_op_time = session.operation_time
            assert cold_op_time is not None, "acknowledged insert has no session operationTime"

        batch_op_times: list[bson.Timestamp] = []
        for start in range(0, hot_docs, batch_size):
            docs = [{"_id": i, "payload": "x" * 200} for i in hot_ids[start : start + batch_size]]
            with t.source.start_session() as session:
                source_coll.insert_many(docs, session=session)
                op_time = session.operation_time
                assert op_time is not None, "acknowledged insert has no session operationTime"
                batch_op_times.append(op_time)

        # Parks the dispatcher in a barrier behind the queued hot backlog, so
        # nothing but worker commits can move reporting until it drains.
        t.source["db_1"]["barrier_coll"].insert_one({"_id": 0})
        before_resume = t.pcsm.status()
        assert before_resume["state"] == PCSM.State.PAUSED, before_resume
        applied_before_resume = before_resume["eventsApplied"]
        initial_optime = before_resume["lastReplicatedOpTime"]["ts"]
        frozen_window = _FrozenWindow(initial_optime, applied_before_resume, time.monotonic())
        deadline = frozen_window.since + 90
        total_docs = hot_docs + 1
        moved_past_cold = False
        frozen_failure = None
        t.pcsm.resume()

        while time.monotonic() < deadline:
            status = t.pcsm.status()
            sampled_at = time.monotonic()
            assert status["state"] == PCSM.State.RUNNING, status
            applied = status["eventsApplied"]
            applied_since_resume = applied - applied_before_resume
            assert applied_since_resume >= 0, status
            optime = status["lastReplicatedOpTime"]["ts"]
            reported = _parse_optime(optime)
            if frozen_failure is None:
                frozen_failure = frozen_window.sample(
                    optime,
                    applied,
                    sampled_at,
                    f"cold worker optime={cold_op_time}, "
                    f"applied_since_resume={applied_since_resume}/{total_docs}",
                )
            if applied_since_resume >= total_docs:
                break

            hot_applied = max(applied_since_resume - 1, 0)
            if hot_applied > 0 and reported > cold_op_time:
                moved_past_cold = True
            # Full-timestamp bound: worker 1 commits in stream order, so the
            # frontier cannot pass the acknowledged batch its progress sits in.
            batch_index = min(len(batch_op_times) - 1, hot_applied // batch_size)
            assert reported <= batch_op_times[batch_index], (
                "replication frontier ran ahead: "
                f"hot_applied={hot_applied}/{hot_docs}, batch={batch_index}, "
                f"batch_op_time={batch_op_times[batch_index]}, reported={reported}, "
                f"lagTimeSeconds={status['lagTimeSeconds']}"
            )
            # Elapsed time is the behavior under test: sample the frozen
            # frontier window at the same cadence as its 500ms progress timer.
            time.sleep(0.5)
        else:
            pytest.fail(f"backlog did not catch up within 90s; last status: {status}")

        runner.wait_for_current_optime()
        assert t.target["db_1"]["coll_1"].count_documents({"_id": cold_id}) == 1
        assert t.target["db_1"]["coll_1"].count_documents({}) == seed_count + total_docs
        assert t.target["db_1"]["barrier_coll"].count_documents({}) == 1

    assert frozen_failure is None, frozen_failure
    assert moved_past_cold, "reporting never passed the drained worker's event during catch-up"
