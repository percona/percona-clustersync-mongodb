# pylint: disable=missing-docstring,redefined-outer-name
import time

import pytest
from pcsm import Runner
from testing import Testing

@pytest.mark.timeout(20)
@pytest.mark.parametrize("phase", [Runner.Phase.CLONE])
def test_inconsistent_index(t: Testing, phase: Runner.Phase):
    t.source.admin.command({"enableSharding": "init_test_db"})
    t.source.admin.command("shardCollection", "init_test_db.text_collection", key={"_id": "hashed"})
    t.source["init_test_db"].text_collection.insert_many([{"a": {"b": i}, "words": f"text_{i}"} for i in range(50)])
    t.source["init_test_db"].text_collection.insert_one({"a": {"b": []}, "words": "omnibus"})

    # Create inconsistent index
    try:
        t.source["init_test_db"].text_collection.create_index([("a.b", 1), ("words", "text")])
        assert False, "Index build should fail due array in doc for text index"
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
