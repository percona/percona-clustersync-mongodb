# pylint: disable=missing-docstring,redefined-outer-name
import time

import pytest
from pymongo import MongoClient
from pymongo.errors import OperationFailure
from testing import Testing

from pcsm import PCSM, Runner


@pytest.mark.timeout(20)
@pytest.mark.parametrize("phase", [Runner.Phase.CLONE])
def test_inconsistent_index(t: Testing, phase: Runner.Phase):
    t.source.admin.command({"enableSharding": "init_test_db"})
    t.source.admin.command("shardCollection", "init_test_db.text_collection", key={"_id": "hashed"})
    t.source["init_test_db"].text_collection.insert_many(
        [{"a": {"b": i}, "words": f"text_{i}"} for i in range(50)]
    )
    t.source["init_test_db"].text_collection.insert_one({"a": {"b": []}, "words": "omnibus"})

    # Create inconsistent index
    try:
        t.source["init_test_db"].text_collection.create_index([("a.b", 1), ("words", "text")])
        raise AssertionError("Index build should fail due array in doc for text index")
    except Exception:
        pass

    time.sleep(15)

    # Check that the index is inconsistent on source (exists on fewer shards than _id_)
    pipeline = [{"$indexStats": {}}, {"$group": {"_id": "$name", "count": {"$sum": 1}}}]
    index_stats = list(t.source["init_test_db"].text_collection.aggregate(pipeline))
    print(f"Source index stats: {index_stats}")

    with t.run(phase):
        pass

    target_indexes = t.target["init_test_db"].text_collection.index_information()

    # Target should NOT have the inconsistent index
    assert "a.b_1_words_text" not in target_indexes, "Target should NOT have the inconsistent index"

    # Target should have the other indexes (_id_ and _id_hashed)
    assert "_id_" in target_indexes, "Target should have _id_ index"
    assert "_id_hashed" in target_indexes, "Target should have _id_hashed index"


def _direct_shard_client(mongos: MongoClient, shard_id: str) -> MongoClient:
    """Open a `directConnection=true` client to the given shard's host."""
    shard_doc = mongos["config"]["shards"].find_one({"_id": shard_id})
    assert shard_doc, f"missing config.shards entry for {shard_id}"
    # `host` is `rs0/src-rs00:30000`; strip the replica-set name prefix.
    host = shard_doc["host"].split("/", 1)[1]
    return MongoClient(f"mongodb://{host}", directConnection=True)


def _hide_index_from_mongos(
    mongos: MongoClient,
    db_name: str,
    coll_name: str,
    index_name: str,
    index_keys: list,
) -> str:
    """Drop `index_name` on exactly the shard mongos routes `listIndexes` to.

    For a sharded collection mongos forwards `listIndexes` to a single shard
    determined by routing metadata. The exact rule depends on MongoDB version
    and whether the config server acts as a data shard, so this helper
    discovers it empirically: drop the index on each data shard in turn and
    check mongos `listIndexes` after each drop. The first shard whose drop
    makes the index disappear from mongos is the routing target. The index
    is restored on shards that did not turn out to be the target.

    Returns the shard id where the index was left dropped.
    """
    data_shards = [s["_id"] for s in mongos["config"]["shards"].find({}) if s["_id"] != "config"]
    assert len(data_shards) >= 2, f"need at least 2 data shards, got: {data_shards}"

    for candidate in data_shards:
        direct = _direct_shard_client(mongos, candidate)
        try:
            try:
                direct[db_name][coll_name].drop_index(index_name)
            except OperationFailure:
                # Index may already be absent on this shard; that is fine.
                pass

            mongos_idx = mongos[db_name][coll_name].index_information()
            if index_name not in mongos_idx:
                return candidate

            # mongos still surfaces the index, so this was not the routing
            # target. Restore the index on this shard before trying the next.
            direct[db_name][coll_name].create_index(index_keys, name=index_name)
        finally:
            direct.close()

    raise AssertionError(
        f"could not find a shard whose drop hides {index_name} from mongos; tried: {data_shards}"
    )


@pytest.mark.timeout(30)
@pytest.mark.parametrize("phase", [Runner.Phase.CLONE])
def test_inconsistent_index_hidden_from_mongos(t: Testing, phase: Runner.Phase):
    """Regression guard for 9e9b8cb.

    mongos `listIndexes` for a sharded collection forwards to a single shard,
    not a fan-out + union. Pre-9e9b8cb the clone path discovered inconsistent
    indexes by intersecting that mongos response with names from `$indexStats`,
    so any index missing from the routing-target shard never reached the
    catalog. 9e9b8cb switched discovery to `$indexStats` specs directly.

    This test reproduces the hidden-from-mongos scenario deterministically:
    create a normal index via mongos so every data shard has it, then drop it
    on the specific shard mongos routes `listIndexes` to. mongos `listIndexes`
    no longer shows the index, but `$indexStats` still reports it as
    inconsistent. The post-fix code must surface it in
    `finalization.unsuccessfulIndexes`.
    """
    db_name = "hidden_inconsistent_db"
    coll_name = "c"
    index_name = "a_idx"
    ns = f"{db_name}.{coll_name}"

    t.source.admin.command({"enableSharding": db_name})
    t.source.admin.command("shardCollection", ns, key={"_id": "hashed"})
    t.source[db_name][coll_name].insert_many([{"a": i} for i in range(200)])
    t.source[db_name][coll_name].create_index([("a", 1)], name=index_name)

    # Drop the index on the shard mongos routes listIndexes to. The helper
    # discovers it empirically rather than guessing from chunk metadata.
    _hide_index_from_mongos(t.source, db_name, coll_name, index_name, [("a", 1)])

    # Preconditions: ensure the hidden-from-mongos state actually holds before
    # running PCSM. Without these the test could pass for the wrong reason if a
    # future MongoDB version changed the routing behavior.
    mongos_indexes = t.source[db_name][coll_name].index_information()
    assert index_name not in mongos_indexes, (
        f"precondition: mongos listIndexes still shows {index_name}: {list(mongos_indexes)}"
    )

    pipeline = [{"$indexStats": {}}, {"$group": {"_id": "$name", "count": {"$sum": 1}}}]
    counts = {row["_id"]: row["count"] for row in t.source[db_name][coll_name].aggregate(pipeline)}
    assert counts.get(index_name, 0) < counts["_id_"], (
        f"precondition: $indexStats does not show {index_name} as inconsistent: {counts}"
    )

    with t.run(phase):
        pass

    # Target must not have the inconsistent index, while still receiving the
    # baseline ones.
    target_indexes = t.target[db_name][coll_name].index_information()
    assert index_name not in target_indexes, target_indexes
    assert "_id_" in target_indexes, target_indexes
    assert "_id_hashed" in target_indexes, target_indexes

    # The bug from 9e9b8cb manifested here: pre-fix, unsuccessfulIndexes was
    # empty because the mongos-hidden index never reached the catalog.
    status = t.pcsm.status()
    assert status["state"] == PCSM.State.FINALIZED, status

    fin = status.get("finalization")
    assert fin is not None, status
    assert fin["completed"] is True, fin

    unsuccessful = fin.get("unsuccessfulIndexes") or []
    matching = [
        idx for idx in unsuccessful if idx["namespace"] == ns and idx["indexName"] == index_name
    ]
    assert matching, f"expected inconsistent index entry for {ns}.{index_name}, got: {unsuccessful}"
    assert matching[0].get("type") == "inconsistent", matching[0]
    assert matching[0].get("reason"), matching[0]
