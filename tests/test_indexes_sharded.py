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

    with t.run(phase):
        pass

    t.compare_all_sharded()
