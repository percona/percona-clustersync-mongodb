# pylint: disable=missing-docstring,redefined-outer-name
import random
from datetime import datetime

import time
import pytest
import testing
from plm import PLM, Runner
from pymongo import MongoClient
from testing import Testing
from bson.decimal128 import Decimal128


# def ensure_collection(source: MongoClient, db: str, coll: str, **kwargs):
#     """Create a collection in the source MongoDB."""
#     source[db].drop_collection(coll)
#     source[db].create_collection(coll, **kwargs)

@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")

    t.compare_all_sharded()

@pytest.mark.parametrize("phase", [Runner.Phase.APPLY])
def test_create_implicitly(t: Testing, phase: Runner.Phase):

    with t.run(phase):
        t.source.admin.command("enableSharding", "db_1")
        t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
        # t.source.admin.command("shardCollection", "db_1.coll_2", key={"_id": "hashed"})
        t.source["db_1"]["coll_1"].insert_one({})
        # t.source["db_1"]["coll_2"].insert_one({})

    t.compare_all_sharded()

@pytest.mark.parametrize("phase", [Runner.Phase.CLONE, Runner.Phase.APPLY])
def test_create_implicitly_already_sharded(t: Testing, phase: Runner.Phase):
    t.source.admin.command("enableSharding", "db_1")
    t.source.admin.command("shardCollection", "db_1.coll_1", key={"_id": 1})
    t.source.admin.command("shardCollection", "db_1.coll_2", key={"_id": "hashed"})

    with t.run(phase):
        t.source["db_1"]["coll_1"].insert_one({})
        t.source["db_1"]["coll_2"].insert_one({})

    t.compare_all_sharded()
