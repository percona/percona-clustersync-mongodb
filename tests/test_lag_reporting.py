"""Black-box regression coverage for PCSM-367 truthful replication lag."""

import time

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
