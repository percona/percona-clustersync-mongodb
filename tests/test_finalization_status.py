# pylint: disable=missing-docstring,redefined-outer-name
"""Tests for the `finalization` section of the /status response.

The `finalization` section is added in PCSM-176 to make failed/incomplete/
inconsistent indexes visible after `/finalize` finishes.

Behaviour under test:
- The section is absent before /finalize is triggered.
- Once finalize completes successfully it appears with completed=true,
  startedAt, completedAt and (when applicable) unsuccessfulIndexes.
- A unique index whose data violates uniqueness on the target produces an
  entry with type="failed" in unsuccessfulIndexes.
"""

import time

from pcsm import PCSM, Runner
from testing import Testing


def test_finalization_section_absent_before_finalize(t: Testing):
    """Status returned in IDLE state must not contain the finalization section."""
    status = t.pcsm.status()

    assert status["state"] == PCSM.State.IDLE
    assert "finalization" not in status, status


def test_finalization_section_present_after_clean_finalize(t: Testing):
    """A normal finalize run must produce a finalization section with no unsuccessful indexes."""
    with t.run(Runner.Phase.APPLY):
        t.source["db_1"]["coll_1"].create_index({"i": 1})

    status = t.pcsm.status()
    assert status["state"] == PCSM.State.FINALIZED, status

    assert "finalization" in status, status
    fin = status["finalization"]

    assert fin["completed"] is True, fin
    assert fin.get("startedAt"), fin
    assert fin.get("completedAt"), fin
    # Either omitted or empty list — both are acceptable since omitempty drops empty slices.
    assert not fin.get("unsuccessfulIndexes"), fin


def test_finalization_section_reports_failed_unique_index(t: Testing):
    """When the source creates a unique index that conflicts with target data,
    the finalization section must surface it as type=failed.

    We force the failure by inserting duplicate values into the target collection
    (after PCSM cloned data) and then creating a unique index on the source.
    PCSM's apply path attempts the index on the target, fails, and flags it.
    """
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
        idx for idx in unsuccessful if idx["namespace"] == f"{db}.{coll}" and idx["indexName"] == "x_unique_idx"
    ]
    assert matching, f"expected unsuccessful index entry for {db}.{coll}.x_unique_idx, got: {unsuccessful}"

    entry = matching[0]
    assert entry["type"] == "failed", entry
