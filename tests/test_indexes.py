# pylint: disable=missing-docstring,redefined-outer-name
import threading
from datetime import datetime

import pymongo
import pytest
import testing
from mlink import Runner
from testing import Testing


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1}, name="idx+1")

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_with_collation(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1}, collation={"locale": "en_US"})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_unique(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1}, unique=True)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_prepare_unique(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1}, prepareUnique=True)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_sparse(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1}, sparse=True)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_partial(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index(
            {"i": 1},
            partialFilterExpression={"j": {"$gt": 5}},
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_hidden(t: Testing, phase: Runner.Phase):
    with t.run(phase) as mlink:
        name = t.source["db_1"]["coll_1"].create_index({"i": 1}, hidden=True)
        assert t.source["db_1"]["coll_1"].index_information()[name]["hidden"]

        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            assert "hidden" not in t.target["db_1"]["coll_1"].index_information()[name]

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_hashed(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": pymongo.HASHED})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_compound(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1, "j": -1})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_multikey(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i.j": 1})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_wildcard(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"$**": 1})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_wildcard_projection(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"$**": 1}, wildcardProjection={"a.*": 1})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_geospatial(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index(
            {"loc1": pymongo.GEO2D}, bits=30, min=-179.0, max=178.0
        )
        t.source["db_1"]["coll_1"].create_index(
            {"loc2": pymongo.GEOSPHERE}, **{"2dsphereIndexVersion": 2}
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_text(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index(
            [("title", pymongo.TEXT), ("description", pymongo.TEXT)],
            name="ArticlesTextIndex",
            default_language="english",
            language_override="language",
            weights={"title": 10, "description": 5},
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_text_wildcard(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"$**": "text"})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_ttl(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1}, expireAfterSeconds=1)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_cloned(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].create_index([("i", 1)])
    t.target["db_1"]["coll_1"].create_index([("i", 1)])

    with t.run(phase):
        t.source["db_1"]["coll_1"].drop_index([("i", 1)])

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_created(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        index_name = t.source["db_1"]["coll_1"].create_index({"i": 1})
        t.source["db_1"]["coll_1"].drop_index(index_name)

    t.compare_all()


def test_drop_non_existing_index(t: Testing):
    t.source["db_0"]["coll_0"].create_index([("i", 1)])

    with t.run(phase=Runner.Phase.APPLY):
        t.target["db_0"]["coll_0"].drop_index([("i", 1)])
        t.source["db_0"]["coll_0"].drop_index([("i", 1)])

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_hide(t: Testing, phase: Runner.Phase):
    index_name = t.source["db_1"]["coll_1"].create_index({"i": 1})

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert "hidden" not in indexes[index_name]

    with t.run(phase):
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {
                    "name": index_name,
                    "hidden": True,
                },
            }
        )

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert indexes[index_name]["hidden"]

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_unhide(t: Testing, phase: Runner.Phase):
    index_name = t.source["db_1"]["coll_1"].create_index({"i": 1}, hidden=True)

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert "hidden" in indexes[index_name]

    with t.run(phase):
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {
                    "keyPattern": {"i": 1},
                    "hidden": False,
                },
            }
        )

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert "hidden" not in indexes[index_name]

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_ttl(t: Testing, phase: Runner.Phase):
    index_name = t.source["db_1"]["coll_1"].create_index({"i": 1}, expireAfterSeconds=123)

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert indexes[index_name]["expireAfterSeconds"] == 123

    with t.run(phase):
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {
                    "keyPattern": {"i": 1},
                    "expireAfterSeconds": 432,
                },
            }
        )

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert indexes[index_name]["expireAfterSeconds"] == 432

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_unique(t: Testing, phase: Runner.Phase):
    index_name = t.source["db_1"]["coll_1"].create_index({"i": 1})

    indexes = t.source["db_1"]["coll_1"].index_information()
    assert "prepareUnique" not in indexes[index_name]
    assert "unique" not in indexes[index_name]

    with t.run(phase):
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {"keyPattern": {"i": 1}, "prepareUnique": True},
            }
        )

        indexes = t.source["db_1"]["coll_1"].index_information()
        assert indexes[index_name]["prepareUnique"]
        assert "unique" not in indexes[index_name]

        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {
                    "keyPattern": {"i": 1},
                    "unique": True,
                },
            }
        )

        indexes = t.source["db_1"]["coll_1"].index_information()
        assert "prepareUnique" not in indexes[index_name]
        assert indexes[index_name]["unique"]

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_internal_create_many_props(t: Testing, phase: Runner.Phase):
    with t.run(phase) as mlink:
        options = {
            "unique": True,
            "hidden": True,
            "expireAfterSeconds": 3600,
        }
        index_name = t.source["db_1"]["coll_1"].create_index({"i": 1}, **options)

        source_index = t.source["db_1"]["coll_1"].index_information()[index_name]
        for prop, val in options.items():
            assert source_index.get(prop) == val

        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            target_index = t.target["db_1"]["coll_1"].index_information()[index_name]
            for prop, val in options.items():
                if prop == "expireAfterSeconds":
                    assert target_index["expireAfterSeconds"] == (2**31) - 1
                else:
                    assert not target_index.get(prop)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_internal_modify_many_props(t: Testing, phase: Runner.Phase):
    index_name = t.source["db_1"]["coll_1"].create_index({"i": 1})

    with t.run(phase) as mlink:
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {"name": index_name, "prepareUnique": True},
            }
        )

        source_index = t.source["db_1"]["coll_1"].index_information()[index_name]
        assert source_index["prepareUnique"]

        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            target_index = t.target["db_1"]["coll_1"].index_information()[index_name]
            assert "prepareUnique" not in target_index

        modify_options = {
            "unique": True,
            "hidden": True,
            "expireAfterSeconds": 3600,
        }
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {"name": index_name, **modify_options},
            },
        )

        source_index = t.source["db_1"]["coll_1"].index_information()[index_name]
        for prop, val in modify_options.items():
            assert source_index.get(prop) == val

        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            target_index = t.target["db_1"]["coll_1"].index_information()[index_name]
            for prop, val in modify_options.items():
                if prop == "expireAfterSeconds":
                    assert target_index["expireAfterSeconds"] == (2**31) - 1
                else:
                    assert not target_index.get(prop)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_internal_modify_index_props_complex(t: Testing, phase: Runner.Phase):
    index_key = {"i": 1}
    index_name = t.source["db_1"]["coll_1"].create_index(index_key, prepareUnique=True)

    source_index1 = t.source["db_1"]["coll_1"].index_information()[index_name]
    assert source_index1["prepareUnique"]
    assert "unique" not in source_index1
    assert "hidden" not in source_index1
    assert "expireAfterSeconds" not in source_index1

    with t.run(phase) as mlink:
        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            target_index = t.target["db_1"]["coll_1"].index_information()[index_name]
            assert "prepareUnique" not in target_index
            assert "unique" not in target_index
            assert "hidden" not in target_index
            assert "expireAfterSeconds" not in target_index

        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {
                    "keyPattern": index_key,
                    "prepareUnique": True,
                    "unique": True,
                    "hidden": True,
                    "expireAfterSeconds": 132,
                },
            }
        )

        source_index2 = t.source["db_1"]["coll_1"].index_information()[index_name]
        assert "prepareUnique" not in source_index2
        assert source_index2["unique"]
        assert source_index2["hidden"]
        assert source_index2["expireAfterSeconds"] == 132

        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            target_index = t.target["db_1"]["coll_1"].index_information()[index_name]
            assert "prepareUnique" not in target_index
            assert "unique" not in target_index
            assert "hidden" not in target_index
            assert target_index["expireAfterSeconds"] == (2**31) - 1

        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "index": {
                    "keyPattern": index_key,
                    "prepareUnique": True,  # do nothing
                    "expireAfterSeconds": 133,
                },
            }
        )

        source_index3 = t.source["db_1"]["coll_1"].index_information()[index_name]
        assert "prepareUnique" not in source_index2
        assert source_index3["unique"]
        assert source_index3["hidden"]
        assert source_index3["expireAfterSeconds"] == 133

        if phase == Runner.Phase.APPLY:
            mlink.wait_for_current_optime()
            target_index = t.target["db_1"]["coll_1"].index_information()[index_name]
            assert "prepareUnique" not in target_index
            assert "unique" not in target_index
            assert "hidden" not in target_index
            assert target_index["expireAfterSeconds"] == (2**31) - 1

    target_index = t.source["db_1"]["coll_1"].index_information()[index_name]
    assert target_index["unique"]
    assert target_index["hidden"]
    assert target_index["expireAfterSeconds"] == 133

    t.compare_all()


def test_manual_create_ttl(t: Testing):
    mlink = t.run(Runner.Phase.MANUAL)
    try:
        t.source["db_1"]["coll_1"].create_index({"a": 1}, expireAfterSeconds=1)
        mlink.start()
        t.source["db_1"]["coll_1"].create_index({"b": 1}, expireAfterSeconds=1)
        mlink.wait_for_initial_sync()
        t.source["db_1"]["coll_1"].create_index({"c": 1}, expireAfterSeconds=1)
        mlink.finalize()
    except:
        mlink.finalize(fast=True)
        raise

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_pml_56_ttl_mismatch(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].drop_collection("coll_1")
        t.source["db_1"]["coll_1"].insert_many(
            [
                {"created_at": datetime.now(), "short_lived": True},
                {"created_at": datetime.now(), "long_lived": True},
            ]
        )
        t.source["db_1"]["coll_1"].create_index(
            {"created_at": 1},
            name="short_ttl_index",
            expireAfterSeconds=1260,
        )

        source_indexes = t.source["db_1"]["coll_1"].index_information()
        assert source_indexes["short_ttl_index"]["expireAfterSeconds"] == 1260

    target_indexes = t.target["db_1"]["coll_1"].index_information()
    assert target_indexes["short_ttl_index"]["expireAfterSeconds"] == 1260

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_continue_creating_indexes_if_some_fail(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].create_index({"i": 1, "j": 1}, name="idx_1")
        t.source["db_1"]["coll_1"].create_index({"i": 1, "j": 1}, unique=True, name="idx_2")
        t.source["db_1"]["coll_1"].create_index(
            {"i": 1, "j": 1},
            collation={"locale": "en_US"},
            name="idx_3",
        )

    target_idx_count = len(t.target["db_1"]["coll_1"].index_information())
    source_idx_count = len(t.source["db_1"]["coll_1"].index_information())

    assert source_idx_count - 1 == target_idx_count


def test_pml_95_drop_index_for_non_existing_namespace(t: Testing):
    t.source["db_0"]["coll_0"].create_index([("i", 1)])

    with t.run(phase=Runner.Phase.APPLY):
        t.target["db_0"]["coll_0"].drop()
        t.source["db_0"]["coll_0"].drop_index([("i", 1)])


@pytest.mark.slow
@pytest.mark.timeout(90)
def test_pml_135_clone_numerous_indexes_deadlock(t: Testing):
    with t.run(phase=Runner.Phase.CLONE, wait_timeout=90):
        for i in range(200):
            for j in range(50):
                t.source["db_1"][f"coll_{i:03d}"].create_index([(f"prop_{j:02d}", 1)])

    try:
        t.compare_all()
    finally:
        # clean up after to avoid other tests running time
        testing.drop_all_database(t.source)
        testing.drop_all_database(t.target)


@pytest.mark.timeout(300)
@pytest.mark.parametrize("index_status", ["succeed", "fail"])
def test_pml_118_ignore_incomplete_index(t: Testing, index_status: str):
    def build_index():
        try:
            t.source["db_1"]["coll_1"].create_index([("a", 1), ("i", "text")])
        except:  # pylint: disable=bare-except
            pass

    for i in range(1000):
        t.source["db_1"]["coll_1"].insert_many({"a": str(i), "i": str(j)} for j in range(1000))

    if index_status == "fail":
        t.source["db_1"]["coll_1"].insert_one({"a": [], "i": []})

    runner = t.run(Runner.Phase.MANUAL, wait_timeout=300)
    runner.start()

    thread = threading.Thread(target=build_index)
    thread.start()
    thread.join()

    runner.finalize()

    t.compare_all()
