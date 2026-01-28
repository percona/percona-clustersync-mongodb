# pylint: disable=missing-docstring,redefined-outer-name

import pytest
from testing import Testing

from pcsm import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY])
def test_insert_without_shardkey_in_doc(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source.admin.command("shardCollection", "db_1.coll_1", key={"key_id_1": 1})
        t.source["db_1"].create_collection("coll_2")
        t.source.admin.command("shardCollection", "db_1.coll_2", key={"key_id_1": 1, "key_id_2": 1})
        t.source["db_1"].create_collection("coll_3")
        t.source.admin.command(
            "shardCollection", "db_1.coll_3", key={"key_id_1": 1, "key_id_2": "hashed"}
        )

        t.source["db_1"]["coll_1"].insert_many(
            [
                {"_id": 1, "data": "some data 1"},
                {"_id": 2, "data": "some data 2", "key_id_1": 10},
            ]
        )

        t.source["db_1"]["coll_2"].insert_many(
            [
                {"_id": 1, "data": "some data 1"},
                {"_id": 2, "data": "some data 2", "key_id_1": 20},
                {"_id": 3, "data": "some data 2", "key_id_2": 20},
                {"_id": 4, "data": "some data 3", "key_id_1": 20, "key_id_2": 30},
            ]
        )

        t.source["db_1"]["coll_3"].insert_many(
            [
                {"_id": 1, "data": "some data 1"},
                {"_id": 2, "data": "some data 2", "key_id_1": 30},
                {"_id": 3, "data": "some data 3", "key_id_2": 40},
                {"_id": 4, "data": "some data 4", "key_id_1": 30, "key_id_2": 40},
            ]
        )

    t.compare_all_sharded()
