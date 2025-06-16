# pylint: disable=missing-docstring,redefined-outer-name
import concurrent.futures
import random
from datetime import datetime, timedelta

import bson
import pymongo
import pytest
import testing
from plm import Runner
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


def cleanup_numerous_databases(source: pymongo.MongoClient, target: pymongo.MongoClient):
    with concurrent.futures.ThreadPoolExecutor() as pool:
        for db in testing.list_databases(source):
            pool.submit(source.drop_database, db)

        for db in testing.list_databases(target):
            pool.submit(target.drop_database, db)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_slow_clone_2gb(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"s": random.randbytes(1024)} for _ in range(420000)),
            )

    try:
        t.compare_all(sort=[("_id", 1)])
    finally:
        # clean up after to avoid consuming other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_slow_clone_2gb_two_namespace(t: Testing):
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
        # clean up after to avoid consuming other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_slow_clone_2gb_vary_id(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        id_gen = vary_id_gen()
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"_id": next(id_gen), "s": random.randbytes(1024)} for _ in range(420000)),
            )

    try:
        t.compare_all(sort=[("_id", pymongo.ASCENDING)])
    finally:
        # clean up after to avoid consuming other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_slow_clone_2gb_capped(t: Testing):
    t.source["db_0"].create_collection("coll_0", capped=True, size=2_147_483_647)

    with t.run(phase=Runner.Phase.CLONE, wait_timeout=300):
        for _ in range(5):
            t.source["db_0"]["coll_0"].insert_many(
                ({"s": random.randbytes(1024)} for _ in range(420000))
            )

    try:
        t.compare_all(sort=[("$natural", pymongo.ASCENDING)])  # must keep source order
    finally:
        # clean up after to avoid consuming other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(300)
def test_slow_clone_2gb_capped_vary_id(t: Testing):
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
        # clean up after to avoid consuming other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.slow
@pytest.mark.timeout(240)
def test_slow_plm_119_clone_numerous_collections_deadlock(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=180):
        for i in range(1000):
            for j in range(10):
                t.source[f"db_{i:03d}"][f"coll_{j:02d}"].insert_one({})

    try:
        t.compare_all()
    finally:
        # clean up after to avoid consuming other tests running time
        cleanup_numerous_databases(t.source, t.target)


@pytest.mark.slow
@pytest.mark.timeout(90)
def test_slow_plm_135_clone_numerous_indexes_deadlock(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=90):
        for i in range(200):
            for j in range(50):
                t.source["db_1"][f"coll_{i:03d}"].create_index([(f"prop_{j:02d}", 1)])

    try:
        t.compare_all()
    finally:
        # clean up after to avoid consuming other tests running time
        cleanup_numerous_databases(t.source, t.target)
