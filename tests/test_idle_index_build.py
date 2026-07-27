# pylint: disable=missing-docstring,redefined-outer-name
from typing import Final

import pytest
from pymongo import MongoClient
from pymongo.write_concern import WriteConcern
from testing import Testing

from pcsm import PCSM, Runner

DB_NAME: Final = "pcsm322_idle_probe"
COLL_NAME: Final = "guard"
INDEX_NAME: Final = "pcsm322_guard_1"
FAILPOINT: Final = "hangIndexBuildAfterSignalPrimaryForCommitReadiness"
INDEX_COMMENT: Final = "PCSM-322 idle index build"


def _matching_index_builds(
    primary: MongoClient,
    *,
    idle_connections: bool,
) -> list[tuple[str, bool]]:
    pipeline = [
        {"$currentOp": {"allUsers": True, "idleConnections": idle_connections}},
        {
            "$match": {
                "op": "command",
                "command.createIndexes": COLL_NAME,
                "command.$db": DB_NAME,
            }
        },
        {"$unwind": "$command.indexes"},
        {"$match": {"command.indexes.name": INDEX_NAME}},
        {"$project": {"_id": 0, "active": 1, "name": "$command.indexes.name"}},
    ]
    return [
        (entry["name"], entry["active"])
        for entry in primary.admin.aggregate(pipeline, maxTimeMS=5_000)
    ]


# Fixed namespace plus cluster-wide failpoint make this test RS-global and intentionally serial.
@pytest.mark.timeout(300)
def test_idle_index_build_is_reported_incomplete(t: Testing):
    hello = t.source.admin.command("hello")
    if hello.get("msg") == "isdbgrid":
        pytest.skip("requires replica-set failpoint support")

    if t.source.admin.command("buildInfo")["versionArray"][0] < 7:
        pytest.skip("createIndexes returnOnStart requires MongoDB 7.0+ on the source")

    primary_host = hello.get("primary")
    assert primary_host, f"source hello did not identify a primary: {hello}"

    primary = MongoClient(f"mongodb://{primary_host}", directConnection=True)
    runner = t.run(Runner.Phase.MANUAL, wait_timeout=120)
    index_spec = {"key": {"guard": 1}, "name": INDEX_NAME}
    create_indexes = {
        "createIndexes": COLL_NAME,
        "indexes": [index_spec],
        "commitQuorum": "votingMembers",
    }
    failpoint_armed = False
    build_started = False
    runner_started = False
    runner_finalized = False

    try:
        primary_hello = primary.admin.command("hello")
        assert primary_hello.get("isWritablePrimary") is True, primary_hello

        collection = primary[DB_NAME][COLL_NAME].with_options(
            write_concern=WriteConcern("majority")
        )
        collection.insert_many([{"guard": value} for value in range(100)])

        failpoint_result = primary.admin.command(
            {"configureFailPoint": FAILPOINT, "mode": "alwaysOn"}
        )
        failpoint_count = failpoint_result["count"]
        failpoint_armed = True

        primary[DB_NAME].command(
            {
                **create_indexes,
                "returnOnStart": True,
                "comment": INDEX_COMMENT,
            }
        )
        build_started = True
        primary.admin.command(
            {
                "waitForFailPoint": FAILPOINT,
                "timesEntered": failpoint_count + 1,
                "maxTimeMS": 30_000,
            }
        )

        visible_without_idle = _matching_index_builds(primary, idle_connections=False)
        visible_with_idle = _matching_index_builds(primary, idle_connections=True)
        assert visible_without_idle == [], (
            "index coordinator unexpectedly visible without idleConnections; "
            f"{visible_without_idle=}, {visible_with_idle=}, {primary_hello=}"
        )
        assert visible_with_idle == [(INDEX_NAME, False)], (
            "expected one inactive index coordinator with idleConnections; "
            f"{visible_without_idle=}, {visible_with_idle=}, {primary_hello=}"
        )

        runner.start()
        runner_started = True
        runner.wait_for_initial_sync()

        target_indexes_after_clone = t.target[DB_NAME][COLL_NAME].index_information()
        assert INDEX_NAME not in target_indexes_after_clone, target_indexes_after_clone

        visible_without_idle = _matching_index_builds(primary, idle_connections=False)
        visible_with_idle = _matching_index_builds(primary, idle_connections=True)
        assert visible_without_idle == [], (
            "index coordinator stopped being idle before finalization; "
            f"{visible_without_idle=}, {visible_with_idle=}, {primary_hello=}"
        )
        assert visible_with_idle == [(INDEX_NAME, False)], (
            "idle index coordinator missing before finalization; "
            f"{visible_without_idle=}, {visible_with_idle=}, {primary_hello=}"
        )
        runner.finalize()
        runner_finalized = True

        target_indexes_after_finalize = t.target[DB_NAME][COLL_NAME].index_information()
        assert INDEX_NAME not in target_indexes_after_finalize, target_indexes_after_finalize

        status = t.pcsm.status()
        assert status["state"] == PCSM.State.FINALIZED, status
        finalization = status.get("finalization")
        assert finalization is not None, status
        assert finalization["completed"] is True, finalization

        unsuccessful = finalization.get("unsuccessfulIndexes") or []
        matching = [
            entry
            for entry in unsuccessful
            if entry["namespace"] == f"{DB_NAME}.{COLL_NAME}" and entry["indexName"] == INDEX_NAME
        ]
        expected = {
            "namespace": f"{DB_NAME}.{COLL_NAME}",
            "indexName": INDEX_NAME,
            "type": "incomplete",
            "reason": "index is still building on one or more source shards",
        }
        reports = [{field: entry[field] for field in expected} for entry in matching]
        assert reports == [expected], (
            "expected exactly one incomplete idle index build; "
            f"{reports=}, {matching=}, {unsuccessful=}, {status=}"
        )
    finally:
        try:
            if failpoint_armed:
                primary.admin.command({"configureFailPoint": FAILPOINT, "mode": "off"})
        finally:
            try:
                if build_started:
                    primary[DB_NAME].command({**create_indexes, "maxTimeMS": 30_000})
            finally:
                try:
                    if runner_started and not runner_finalized:
                        runner.finalize(fast=True)
                finally:
                    try:
                        t.source.drop_database(DB_NAME)
                    finally:
                        primary.close()
