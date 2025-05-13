# pylint: disable=missing-docstring,redefined-outer-name
from datetime import datetime

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
def test_update_one_complex(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many(
        [
            {
                "i": i,
                "j": i,
                "a1": ["A", "B", "C", "D", "E"],
                "a2": [1, 2, 3, 4, 5],
                "f2": {"0": [{"i": i, "0": i} for i in range(5)], "1": "val"},
            }
            for i in range(5)
        ]
    )
    t.target["db_1"]["coll_1"].insert_many(
        [
            {
                "j": i,
                "a1": ["A", "B", "C", "D", "E"],
                "a2": [1, 2, 3, 4, 5],
                "f2": {"0": [{"i": i, "0": i} for i in range(5)], "1": "val"},
            }
            for i in range(5)
        ]
    )

    with t.run(phase):
        for i in range(5):
            t.source["db_1"]["coll_1"].update_one(
                {"i": i},
                {
                    "$inc": {"i": (i * 100) - i},
                    "$set": {f"field_{i}": f"value_{i}"},
                    "$unset": {f"j": 1},
                },
            )

            t.source["db_1"]["coll_1"].update_one(
                {"i": i},
                {"$set": {"f2.1": "new-val"}},
            )

            # Update the second element of the a1 array
            t.source["db_1"]["coll_1"].update_one(
                {"i": i},
                {"$set": {"a1.1": "X"}},
            )

            # Update `0` field of the first element of the f2.0 array
            t.source["db_1"]["coll_1"].update_one(
                {"i": i},
                {"$set": {"f2.0.3.0": 99}},
            )

            # Add new element to the f2.0 array
            t.source["db_1"]["coll_1"].update_one(
                {"i": i}, [{"$set": {"f2.0": {"$concatArrays": ["$f2.0", [{"i": 5, "0": 5}]]}}}]
            )

            # Reverse the a2 array
            t.source["db_1"]["coll_1"].update_one(
                {"i": i}, [{"$set": {"a2": {"$reverseArray": "$a2"}}}]
            )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_update_one_to_slice_array(t: Testing, phase: Runner.Phase):
    docs = [{"_id": i, "arr": ["A", "B", "C", "D", "E", "F", "G"]} for i in range(4)]
    t.source["db_1"]["coll_1"].insert_many(docs)
    t.target["db_1"]["coll_1"].insert_many(docs)

    with t.run(phase):
        t.source["db_1"]["coll_1"].update_one(
            {"_id": 0},
            {"$push": {"arr": {"$each": [], "$slice": 3}}},
        )
        t.source["db_1"]["coll_1"].update_one(
            {"_id": 1},
            [{"$set": {"arr": {"$slice": ["$arr", 3]}}}],
        )
        t.source["db_1"]["coll_1"].update_one(
            {"_id": 2},
            [{"$set": {"arr": {"$slice": ["$arr", 1, 3]}}}],
        )
        t.source["db_1"]["coll_1"].update_one(
            {"_id": 3},
            {"$pull": {"arr": {"$in": ["C"]}}},
        )

        assert list(t.source["db_1"]["coll_1"].find(sort=[("_id", pymongo.ASCENDING)])) == [
            {"_id": 0, "arr": ["A", "B", "C"]},
            {"_id": 1, "arr": ["A", "B", "C"]},
            {"_id": 2, "arr": ["B", "C", "D"]},
            {"_id": 3, "arr": ["A", "B", "D", "E", "F", "G"]},
        ]

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_update_one_with_nested_path(t: Testing, phase: Runner.Phase):
    docs = [
        {"_id": 0, "a": {"0": "val"}},
        {"_id": 1, "a": {"0": "val"}},
        {"_id": 2, "a": ["val"]},
        {"_id": 3, "a": {"0": ["val"]}},
        {"_id": 4, "a": [{"0": "val"}]},
        {"_id": 5, "a": {"0": ["val"]}},
        {"_id": 6},
    ]
    t.source["db_1"]["coll_1"].insert_many(docs)
    t.target["db_1"]["coll_1"].insert_many(docs)

    with t.run(phase):
        t.source["db_1"]["coll_1"].update_one({"_id": 0}, {"$set": {"a": {"0": "changed"}}})
        t.source["db_1"]["coll_1"].update_one({"_id": 1}, {"$set": {"a.0": "changed"}})
        t.source["db_1"]["coll_1"].update_one({"_id": 2}, {"$set": {"a.0": "changed"}})
        t.source["db_1"]["coll_1"].update_one({"_id": 3}, {"$set": {"a.0.0": "changed"}})
        t.source["db_1"]["coll_1"].update_one({"_id": 4}, {"$set": {"a.0.0": "changed"}})  # fails
        t.source["db_1"]["coll_1"].update_one({"_id": 5}, {"$set": {"a": {"0.0": "changed"}}})
        t.source["db_1"]["coll_1"].update_one({"_id": 6}, {"$set": {"a.0": {"0.0": "changed"}}})

        assert list(t.source["db_1"]["coll_1"].find(sort=[("_id", pymongo.ASCENDING)])) == [
            {"_id": 0, "a": {"0": "changed"}},
            {"_id": 1, "a": {"0": "changed"}},
            {"_id": 2, "a": ["changed"]},
            {"_id": 3, "a": {"0": ["changed"]}},
            {"_id": 4, "a": [{"0": "changed"}]},
            {"_id": 5, "a": {"0.0": "changed"}},
            {"_id": 6, "a": {"0": {"0.0": "changed"}}},
        ]

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_update_one_multiple_arrays(t: Testing, phase: Runner.Phase):
    doc = {
        "_id": "",
        "a": ["A", "B", "C", "D", "E", "F", "G", "H", "I"],
        "b": {"0": ["val1", "val2", "val3"]},
    }
    t.source["db_1"]["coll_1"].insert_one(doc)
    t.target["db_1"]["coll_1"].insert_one(doc)

    with t.run(phase):
        t.source["db_1"]["coll_1"].update_one(
            {"_id": ""},
            [
                {
                    "$set": {
                        "a": {
                            "$filter": {
                                "input": "$a",
                                "as": "arr",
                                "cond": {"$ne": ["$$arr", "C"]},
                            }
                        }
                    }
                },
                {
                    "$set": {
                        "b.0": {
                            "$concatArrays": [
                                {"$slice": ["$b.0", 1]},
                                ["changed2"],
                                {"$slice": ["$b.0", 2, {"$size": "$b.0"}]},
                            ],
                        }
                    }
                },
            ],
        )

        assert t.source["db_1"]["coll_1"].find_one({"_id": ""}) == {
            "_id": "",
            "a": ["A", "B", "D", "E", "F", "G", "H", "I"],
            "b": {"0": ["val1", "changed2", "val3"]},
        }

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY])
def test_update_one_with_trucated_arrays(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_one(
        {
            "i": "f1",
            "a1": ["A", "B", "C", "D", "B", "B", "E", "F", "G"],
            "j": "val",
            "f2": {"0": [{"i": i, "0": i} for i in range(5)], "1": "val"},
        }
    )
    t.target["db_1"]["coll_1"].insert_one(
        {
            "i": "f1",
            "a1": ["A", "B", "C", "D", "B", "B", "E", "F", "G"],
            "j": "val",
            "f2": {"0": [{"i": i, "0": i} for i in range(5)], "1": "val"},
        }
    )

    with t.run(phase):
        t.source["db_1"]["coll_1"].update_one(
            {"i": "f1"},
            [
                {  # Remove the second element of a1
                    "$set": {
                        "a1": {
                            "$filter": {
                                "input": "$a1",
                                "as": "arr",
                                "cond": {"$ne": ["$$arr", "C"]},
                            }
                        }
                    }
                },
                {  # Remove the third element of nested f2.0 array
                    "$set": {
                        "f2.0": {
                            "$filter": {
                                "input": "$f2.0",
                                "as": "arr",
                                "cond": {"$ne": ["$$arr", {"i": 2}]},
                            }
                        }
                    }
                },
                # Remove first elements from the f2.0 array
                {"$set": {"f2.0": {"$slice": ["$f2.0", -7]}}},
                {"$set": {"f2.1": "new-val"}},
                {"$unset": ["j"]},
            ],
        )

        # Remove all occurrences of "B" from the a1 array
        t.source["db_1"]["coll_1"].update_one(
            {"i": "f1"},
            [
                {
                    "$set": {
                        "a1": {
                            "$filter": {"input": "$a1", "as": "a", "cond": {"$ne": ["$$a", "B"]}}
                        }
                    }
                }
            ],
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


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_id_type(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].insert_many(
            [
                {},  # bson object id
                {"_id": "1"},
                {"_id": 2},
                {"_id": datetime.now()},
                {"_id": 3.4},
                {"_id": {"a": 1, "b": "c"}},
                {"_id": True},
                {"_id": None},
            ]
        )

    t.compare_all()


@pytest.mark.timeout(180)
def test_compare_all(t: Testing):
    t.compare_all()
