# pylint: disable=missing-docstring,redefined-outer-name
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import pymongo
import pytest
from testing import Testing

from pcsm import Runner


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
                    "$unset": {"j": 1},
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


@pytest.mark.timeout(180)
@pytest.mark.parametrize("phase", [Runner.Phase.APPLY])
def test_pcsm_305_bufbuilder_overflow(t: Testing, phase: Runner.Phase):
    """Regression test for PCSM-305 (BufBuilder 125MB target-side crash).

    Each updated document emits a change-stream event combining:
      - truncation of an array nested under an indexed parent
      - indexed writes within that truncated array
      - sibling count and signature updates

    With the bug, PCSM bundles many such events into a single target bulkWrite
    that exceeds MongoDB's 125MB BufBuilder limit and change replication fails
    ('BufBuilder attempted to grow() to 131072001 bytes, past the 125MB limit').
    The harness surfaces this as either an AssertionError on the state==RUNNING
    check in wait_for_current_optime, a WaitTimeoutError, or a compare_all()
    content mismatch. With the fix, source and target converge.
    """
    parent_index = 4
    base_array_size = 1500
    new_size = 1000
    indexed_updates = 15
    iterations = 10
    parallel_workers = 25
    total_docs = iterations * parallel_workers  # 250

    def build_seed_doc(doc_id):
        groups = []
        for i in range(parent_index + 1):
            if i == parent_index:
                groups.append(
                    {
                        "name": f"group_{i}",
                        "count": base_array_size,
                        "items": [f"item_{j}" for j in range(base_array_size)],
                    }
                )
            else:
                groups.append({"name": f"group_{i}", "count": 0, "items": []})
        return {
            "_id": doc_id,
            "groups": groups,
            "signature": "initial",
            "updated_at": "initial",
        }

    def build_pipeline_computed_diff_update():
        # Truncates groups.<idx>.items to new_size, rewrites the trailing
        # `indexed_updates` elements via $concatArrays/$slice, and updates the
        # sibling count/signature fields. Avoids classic-modifier path
        # conflicts so the resulting change-stream event combines truncation +
        # indexed writes + count update on the same array path.
        start_idx = max(0, new_size - indexed_updates)
        touched_indexes = list(range(start_idx, new_size))

        target_group_expr = {"$arrayElemAt": ["$groups", parent_index]}
        source_items_expr = {
            "$ifNull": [
                {"$getField": {"field": "items", "input": target_group_expr}},
                [],
            ]
        }
        mutated_items_expr = {"$slice": [source_items_expr, new_size]}
        for idx in touched_indexes:
            mutated_items_expr = {
                "$concatArrays": [
                    {"$slice": [mutated_items_expr, idx]},
                    [f"updated_{idx}"],
                    {"$slice": [mutated_items_expr, idx + 1, new_size]},
                ]
            }

        updated_group_expr = {
            "$mergeObjects": [
                target_group_expr,
                {"count": new_size, "items": mutated_items_expr},
            ]
        }

        trailing_count_expr = {
            "$max": [0, {"$subtract": [{"$size": "$groups"}, parent_index + 1]}]
        }
        trailing_slice_expr = {
            "$let": {
                "vars": {"tailCount": trailing_count_expr},
                "in": {
                    "$cond": [
                        {"$gt": ["$$tailCount", 0]},
                        {"$slice": ["$groups", parent_index + 1, "$$tailCount"]},
                        [],
                    ]
                },
            }
        }
        updated_groups_expr = {
            "$concatArrays": [
                {"$slice": ["$groups", parent_index]},
                [updated_group_expr],
                trailing_slice_expr,
            ]
        }

        return [
            {
                "$set": {
                    "groups": updated_groups_expr,
                    "signature": "updated",
                    "updated_at": "updated",
                }
            }
        ]

    seed_docs = [build_seed_doc(i) for i in range(total_docs)]
    t.source["db_1"]["coll_1"].insert_many(seed_docs)
    t.target["db_1"]["coll_1"].insert_many(seed_docs)

    update_pipeline = build_pipeline_computed_diff_update()

    def apply_one(doc_id):
        t.source["db_1"]["coll_1"].update_one({"_id": doc_id}, update_pipeline)

    with t.run(phase, wait_timeout=10):
        with ThreadPoolExecutor(max_workers=parallel_workers) as ex:
            futures = [ex.submit(apply_one, doc_id) for doc_id in range(total_docs)]
            for fut in as_completed(futures):
                fut.result()  # surface any source-side write error

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
