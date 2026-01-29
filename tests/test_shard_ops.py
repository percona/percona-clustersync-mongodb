# pylint: disable=missing-docstring,redefined-outer-name
import pytest
from pymongo.errors import OperationFailure
from pcsm import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY])
def test_move_primary(t, phase: Runner.Phase):
    """
    Test to verify movePrimary command
    """
    db_name = "move_primary_test_db"
    coll_name = "test_coll"
    t.source.admin.command("enableSharding", db_name)
    collection = t.source[db_name][coll_name]
    collection.insert_many([{"_id": i} for i in range(10)])
    config_db = t.source.get_database("config")
    shards = list(config_db.shards.find())
    shard_names = [shard["_id"] for shard in shards]
    db_info = config_db.databases.find_one({"_id": db_name})
    current_primary = db_info.get("primary")
    target_shard = next((s for s in shard_names if s != current_primary), None)
    with t.run(phase):
        try:
            t.source.admin.command("movePrimary", db_name, to=target_shard)
        except OperationFailure as e:
            if "movePrimary" in str(e) or "not supported" in str(e).lower() or "command not found" in str(e).lower():
                pytest.skip(f"movePrimary not supported: {e}")
            raise
    db_info_after = config_db.databases.find_one({"_id": db_name})
    assert db_info_after is not None, f"Database {db_name} not found after movePrimary"
    assert db_info_after.get("primary") == target_shard, f"Primary shard should be {target_shard}, got {db_info_after.get('primary')}"
    t.compare_all()
