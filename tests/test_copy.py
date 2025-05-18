# pylint: disable=missing-docstring,redefined-outer-name
import random
from datetime import datetime, timedelta

import bson
import pymongo
import pytest
import testing
from mlink import Runner
from testing import Testing


def vary_id_gen():
    for i in range(1_000_000):
        for j in range(1000):
            yield bson.ObjectId()
            yield f"{i}:{j+1}"
            yield i * 1000 + (j + 2)
            yield bson.Timestamp(i, j + 3)
            yield {"i": i, "j": j + 4}
            yield datetime.now() + timedelta(seconds=i * 1000 + (j + 5))


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_clone_2gb(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"s": random.randbytes(1024)} for _ in range(420000)),
            )

    try:
        t.compare_all(sort=[("_id", 1)])
    finally:
        # clean up after to avoid other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_clone_2gb_two_namespace(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"s": random.randbytes(1024)} for _ in range(210000)),
            )
            t.source["db_0"]["coll_1"].insert_many(
                ({"s": random.randbytes(1024)} for _ in range(210000)),
            )

    try:
        t.compare_all(sort=[("_id", pymongo.ASCENDING)])
    finally:
        # clean up after to avoid other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_clone_2gb_vary_id(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        id_gen = vary_id_gen()
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"_id": next(id_gen), "s": random.randbytes(1024)} for _ in range(420000)),
            )

    try:
        t.compare_all(sort=[("_id", pymongo.ASCENDING)])
    finally:
        # clean up after to avoid other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_clone_2gb_capped(t: Testing):
    t.source["db_0"].create_collection("coll_0", capped=True, size=2_147_483_647)

    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"s": random.randbytes(1024)} for _ in range(420000))
            )

    try:
        t.compare_all(sort=[("$natural", pymongo.ASCENDING)])  # must keep source order
    finally:
        # clean up after to avoid other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_clone_2gb_capped_vary_id(t: Testing):
    t.source["db_0"].create_collection("coll_0", capped=True, size=2_147_483_647)

    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        id_gen = vary_id_gen()
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"_id": next(id_gen), "s": random.randbytes(1024)} for _ in range(420000)),
            )

    try:
        t.compare_all(sort=[("$natural", pymongo.ASCENDING)])  # must keep source order
    finally:
        # clean up after to avoid other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)
