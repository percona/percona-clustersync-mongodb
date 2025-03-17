# pylint: disable=missing-docstring,redefined-outer-name
import pymongo
import pytest
from mlink import Runner
from testing import Testing


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_insert_one(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].insert_one({"i": i})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_insert_many(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_update_one(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].update_one(
                {"i": i},
                {
                    "$inc": {"i": (i * 100) - i},
                    "$set": {f"field_{i}": f"value_{i}"},
                },
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_update_many(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        t.source["db_1"]["coll_1"].update_many({}, {"$inc": {"i": 100}})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_replace_one(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].replace_one(
                {"i": i},
                {"i": (i * 100) - i, f"field_{i}": f"value_{i}"},
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_delete_one(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        t.source["db_1"]["coll_1"].delete_one({"i": 4})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_delete_many(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    assert t.source["db_1"]["coll_1"].count_documents({}) == 5

    with t.run(phase):
        t.source["db_1"]["coll_1"].delete_many({"i": {"$gt": 2}})

    assert t.source["db_1"]["coll_1"].count_documents({}) == 3

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_find_one_and_update(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].find_one_and_update(
                {"i": i},
                {
                    "$inc": {"i": (i * 100) - i},
                    "$set": {f"field_{i}": f"value_{i}"},
                },
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_find_one_and_replace(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].find_one_and_replace(
                {"i": i},
                {
                    "i": (i * 100) - i,
                    f"field_{i}": f"value_{i}",
                },
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_find_one_and_delete(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

    with t.run(phase):
        t.source["db_1"]["coll_1"].find_one_and_delete({"i": 4})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_upsert_by_update_one(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].update_one(
                {"i": i},
                {
                    "$inc": {"i": (i * 100) - i},
                    "$set": {f"field_{i}": f"value_{i}"},
                },
                upsert=True,
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_upsert_by_update_many(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].update_many({}, {"$inc": {"i": 100}}, upsert=True)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_upsert_by_replace_one(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].replace_one(
                {"i": i},
                {
                    "i": (i * 100) - i,
                    f"field_{i}": f"value_{i}",
                },
                upsert=True,
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_upsert_by_find_one_and_update(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].find_one_and_update(
                {"i": i},
                {
                    "$inc": {"i": (i * 100) - i},
                    "$set": {f"field_{i}": f"value_{i}"},
                },
                upsert=True,
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_upsert_by_find_one_and_replace(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].find_one_and_replace(
                {"i": i},
                {
                    "i": (i * 100) - i,
                    f"field_{i}": f"value_{i}",
                },
                upsert=True,
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_bulk_write(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        ops = [
            pymongo.InsertOne({"i": 1}),
            pymongo.InsertOne({"i": 2}),
            pymongo.InsertOne({"i": 3}),
            pymongo.InsertOne({"i": 4}),
            pymongo.UpdateOne({"i": 1}, {"$set": {"i": "1"}}),
            pymongo.DeleteOne({"i": 2}),
            pymongo.ReplaceOne({"i": 4}, {"k": "4"}),
        ]
        t.source["db_1"]["coll_1"].bulk_write(ops, ordered=True)

    coll_1 = []
    for doc in t.source["db_1"]["coll_1"].find():
        del doc["_id"]
        coll_1.append(doc)

    assert coll_1 == [{"i": "1"}, {"i": 3}, {"k": "4"}]

    t.compare_all()
