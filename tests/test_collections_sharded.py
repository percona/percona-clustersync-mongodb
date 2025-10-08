# pylint: disable=missing-docstring,redefined-outer-name
from datetime import datetime

import pytest
from plm import Runner
from testing import Testing


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_shard_collection(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
        t.source.admin.command("shardCollection", "db_1.coll_2", key={"_id": "hashed"})
        t.source.admin.command("shardCollection", "db_1.coll_3", key={"a": 1, "b": 1})
        t.source.admin.command("shardCollection", "db_1.coll_4", key={"a": "hashed", "b": 1})

    t.compare_all_sharded()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_sharded(t: Testing, phase: Runner.Phase):
    t.source["db_1"].drop_collection("coll_1")
    t.source["db_1"].create_collection("coll_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})

    with t.run(phase):
        t.source["db_1"].drop_collection("coll_1")

    assert "coll_1" not in t.target["db_1"].list_collection_names()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_rename_sharded(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
        t.source["db_1"]["coll_1"].rename("coll_2")

    t.compare_all()
