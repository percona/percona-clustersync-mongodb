# pylint: disable=missing-docstring,redefined-outer-name
import time

import pytest
from testing import Testing

from pcsm import PCSM, Runner


def test_finalization_section_absent_before_finalize(t: Testing):
    with t.run(Runner.Phase.APPLY):
        status = t.pcsm.status()

        assert status["state"] == PCSM.State.RUNNING, status
        assert "finalization" not in status, status


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_finalization_section_present_after_clean_finalize(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1})

    status = t.pcsm.status()
    assert status["state"] == PCSM.State.FINALIZED, status

    fin = status.get("finalization")
    assert fin is not None, status
    assert fin["completed"] is True, fin
    assert fin.get("startedAt"), fin
    assert fin.get("completedAt"), fin
    assert not fin.get("unsuccessfulIndexes"), fin

    t.compare_all()


def test_finalization_section_reports_failed_unique_index(t: Testing):
    db = "db_failed_idx"
    coll = "coll_failed_idx"

    t.source[db][coll].insert_many([{"_id": 1, "x": 1}, {"_id": 2, "x": 2}])

    with t.run(Runner.Phase.APPLY):
        # Pre-populate the target with values that will conflict with the unique
        # index we are about to create on the source.
        t.target[db][coll].insert_many([{"_id": 100, "x": 10}, {"_id": 101, "x": 10}])

        # Wait briefly so the inserts are visible to the apply path before the
        # createIndex command arrives via the change stream.
        time.sleep(0.5)

        t.source[db][coll].create_index({"x": 1}, unique=True, name="x_unique_idx")

    status = t.pcsm.status()
    assert status["state"] == PCSM.State.FINALIZED, status

    fin = status.get("finalization")
    assert fin is not None, status
    assert fin["completed"] is True, fin

    unsuccessful = fin.get("unsuccessfulIndexes") or []
    matching = [
        idx
        for idx in unsuccessful
        if idx["namespace"] == f"{db}.{coll}" and idx["indexName"] == "x_unique_idx"
    ]
    assert matching, (
        f"expected unsuccessful index entry for {db}.{coll}.x_unique_idx, got: {unsuccessful}"
    )

    entry = matching[0]
    assert entry["type"] == "failed", entry
