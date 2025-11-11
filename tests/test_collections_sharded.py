# pylint: disable=missing-docstring,redefined-outer-name
import threading
import time
from datetime import datetime

import pytest
from pcsm import Runner
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
def test_shard_unique_collection(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source.admin.command("shardCollection", "db_1.coll_1", key={"a": 1}, unique=True)
        t.source.admin.command("shardCollection", "db_1.coll_2", key={"a": 1, "b": 1}, unique=True)

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


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_collection_with_collation(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1", collation={"locale": "en", "strength": 2})
        t.source.admin.command(
            "shardCollection", "db_1.coll_1", key={"name": 1}, collation={"locale": "simple"}
        )
        t.source["db_1"]["coll_1"].insert_many([{"name": "n3"}, {"name": "n2"}, {"name": "n3"}])

        t.source["db_1"].create_collection("coll_2", collation={"locale": "en", "strength": 2})
        t.source.admin.command(
            "shardCollection", "db_1.coll_2", key={"_id": "hashed"}, collation={"locale": "simple"}
        )
        t.source["db_1"]["coll_2"].insert_many([{"_id": 11}, {"_id": 22}, {"_id": 33}])

    t.compare_all_sharded()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_collection_with_collation_with_shard_key_index_prefix(
    t: Testing, phase: Runner.Phase
):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_2", collation={"locale": "en", "strength": 2})
        t.source["db_1"]["coll_2"].create_index(
            [("name", 1), ("date", 1), ("age", 1)], collation={"locale": "simple"}
        )
        t.source.admin.command(
            "shardCollection",
            "db_1.coll_1",
            key={"name": 1, "date": 1},
            collation={"locale": "simple"},
        )

    t.compare_all_sharded()

@pytest.mark.parametrize("phase", [Runner.Phase.APPLY])
def test_shard_key_update_duplicate_key_error(t: Testing, phase: Runner.Phase):
    """
    Test to reproduce pcsm duplicate key error when handling shard key updates
    """
    db_name = "test_db"
    collection_name = "test_collection"
    coll = t.source[db_name][collection_name]
    t.source.admin.command("shardCollection", f"{db_name}.{collection_name}", key={"key_id": 1})
    coll.insert_one({"key_id": 0, "name": "item_0", "value": "value_0"})
    for i in range(1, 10):
        key_id = 100 + i
        coll.insert_one({"key_id": key_id, "name": f"pre_sync_doc_{i}", "value": f"value_{key_id}"})
    stop_event = threading.Event()
    def perform_shard_key_updates():
        num_updates = 500
        for i in range(1, num_updates + 1):
            if stop_event.is_set():
                break
            key_id = 200 + i
            new_key_id = 5000 + i
            coll.insert_one({"key_id": key_id, "name": f"test_doc_{i}", "value": f"value_{key_id}"})
            coll.update_one({"key_id": key_id}, {"$set": {"key_id": new_key_id, "shard_key_updated": True}})
            time.sleep(0.05)
    update_thread = threading.Thread(target=perform_shard_key_updates)
    update_thread.start()
    time.sleep(3)
    with t.run(phase):
        stop_event.set()
        update_thread.join(timeout=5)
    t.compare_all_sharded()
