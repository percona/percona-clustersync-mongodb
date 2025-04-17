# pylint: disable=missing-docstring,redefined-outer-name
from datetime import datetime

import pytest
from mlink import Runner
from pymongo import MongoClient
from testing import Testing


def ensure_collection(source: MongoClient, target: MongoClient, db: str, coll: str, **kwargs):
    """Create a collection in the source and target MongoDB."""
    source[db].drop_collection(coll)
    target[db].drop_collection(coll)
    source[db].create_collection(coll, **kwargs)
    target[db].create_collection(coll, **kwargs)


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_implicitly(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"]["coll_1"].insert_one({})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_with_collation(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1", collation={"locale": "en_US"})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_diff_uuid(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")

    t.compare_all()

    source_info = next(t.source["db_1"].list_collections(filter={"name": "coll_1"}))
    target_info = next(t.target["db_1"].list_collections(filter={"name": "coll_1"}))
    assert source_info["name"] == "coll_1" == target_info["name"]

    if source_info["info"]["uuid"] == target_info["info"]["uuid"]:
        # mongolink does not use applyOps. no possible to preserveUUID
        pytest.xfail("colllection UUID should not be equal")


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_clustered(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection(
            "coll_1",
            clusteredIndex={"key": {"_id": 1}, "unique": True},
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_clustered_ttl_ignored(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection(
            "coll_1",
            clusteredIndex={"key": {"_id": 1}, "unique": True},
            expireAfterSeconds=1,
        )

    source_options = t.source["db_1"]["coll_1"].options()
    target_options = t.target["db_1"]["coll_1"].options()

    assert source_options["clusteredIndex"] == target_options["clusteredIndex"]
    assert source_options["expireAfterSeconds"] == 1
    assert "expireAfterSeconds" not in target_options


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_capped(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1", capped=True, size=54321, max=12345)
        t.source["db_1"]["coll_1"].insert_many({"i": i} for i in range(10))

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_view(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(-3, 3)])
    t.target["db_1"]["coll_1"].insert_many([{"i": i} for i in range(-3, 3)])

    with t.run(phase):
        t.source["db_1"].create_collection(
            "view_1",
            viewOn="coll_1",
            pipeline=[{"$match": {"i": {"$gte": 0}}}],
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_view_with_collation(t: Testing, phase: Runner.Phase):
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(-3, 3)])
    t.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(-3, 3)])

    with t.run(phase):
        t.source["db_1"].create_collection(
            "view_1",
            viewOn="coll_1",
            pipeline=[{"$match": {"i": {"$gte": 0}}}],
            collation={"locale": "en_US"},
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_timeseries_ignored(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection(
            "coll_1",
            timeseries={"timeField": "ts", "metaField": "meta"},
        )
        t.source["db_1"]["coll_1"].insert_many(
            {"ts": datetime.now(), "meta": {"i": i}} for i in range(10)
        )

    assert t.target["db_1"].list_collection_names() == []


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_with_storage_options(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        options = {
            "storageEngine": {"wiredTiger": {"configString": "block_compressor=snappy"}},
            "indexOptionDefaults": {
                "storageEngine": {"wiredTiger": {"configString": "block_compressor=zlib"}},
            },
        }
        t.source["db_1"].create_collection("coll_1", **options)
        assert t.source["db_1"]["coll_1"].options() == options

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_with_pre_post_images_ignored(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        options = {
            "changeStreamPreAndPostImages": {"enabled": True},
        }
        t.source["db_1"].create_collection("coll_1", **options)
        assert t.source["db_1"]["coll_1"].options() == options

    assert "changeStreamPreAndPostImages" not in t.target["db_1"]["coll_1"].options()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_create_with_validation(t: Testing, phase: Runner.Phase):
    create_options = {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
            }
        },
        "validationLevel": "moderate",
        "validationAction": "warn",
    }

    with t.run(phase):
        t.source["db_1"].create_collection("coll_1", **create_options)
        assert t.source["db_1"]["coll_1"].options() == create_options

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_collection(t: Testing, phase: Runner.Phase):
    ensure_collection(t.source, t.target, "db_1", "coll_1")

    with t.run(phase):
        t.source["db_1"].drop_collection("coll_1")

    assert "coll_1" not in t.target["db_1"].list_collection_names()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_capped_collection(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection("coll_1", capped=True, size=54321, max=12345)
    t.source["db_1"]["coll_1"].insert_many({"i": i} for i in range(10))

    with t.run(phase):
        t.source["db_1"].drop_collection("coll_1")

    assert "coll_1" not in t.target["db_1"].list_collection_names()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_view(t: Testing, phase: Runner.Phase):
    ensure_collection(t.source, t.target, "db_1", "coll_1")
    ensure_collection(
        t.source,
        t.target,
        "db_1",
        "view_1",
        viewOn="coll_1",
        pipeline=[{"$match": {"i": {"$gt": 3}}}],
    )

    with t.run(phase):
        t.source["db_1"].drop_collection("view_1")

    assert "view_1" not in t.target["db_1"].list_collection_names()
    assert "coll_1" in t.target["db_1"].list_collection_names()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_view_source_collection(t: Testing, phase: Runner.Phase):
    ensure_collection(t.source, t.target, "db_1", "coll_1")
    ensure_collection(
        t.source,
        t.target,
        "db_1",
        "view_1",
        viewOn="coll_1",
        pipeline=[{"$match": {"i": {"$gt": 3}}}],
    )

    with t.run(phase):
        t.source["db_1"].drop_collection("coll_1")

    assert "view_1" in t.target["db_1"].list_collection_names()
    assert "coll_1" not in t.target["db_1"].list_collection_names()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_drop_database(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source["db_1"].create_collection(
            "view_1",
            viewOn="coll_1",
            pipeline=[{"$match": {"i": {"$gte": 0}}}],
        )
        t.source.drop_database("db_1")

    if phase == Runner.Phase.CLONE:
        # clone started after view has been dropped
        assert t.target["db_1"].list_collection_names() == []
    else:
        # view was dropped after the clone had started
        assert t.target["db_1"].list_collection_names() == ["system.views"]


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_clustered_ttl_ignored(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection(
        "coll_1",
        clusteredIndex={"key": {"_id": 1}, "unique": True},
        expireAfterSeconds=123,
    )

    expected_index_options = {"name": "_id_", "key": {"_id": 1}, "unique": True, "v": 2}
    assert t.source["db_1"]["coll_1"].options() == {
        "clusteredIndex": expected_index_options,
        "expireAfterSeconds": 123,
    }

    with t.run(phase):
        t.source["db_1"].command({"collMod": "coll_1", "expireAfterSeconds": 444})

    assert t.source["db_1"]["coll_1"].options() == {
        "clusteredIndex": expected_index_options,
        "expireAfterSeconds": 444,
    }
    assert t.target["db_1"]["coll_1"].options() == {"clusteredIndex": expected_index_options}


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_capped_size(t: Testing, phase: Runner.Phase):
    ensure_collection(t.source, t.target, "db_1", "coll_1", capped=True, size=1111, max=222)
    ensure_collection(t.source, t.target, "db_1", "coll_2", capped=True, size=1111, max=222)
    ensure_collection(t.source, t.target, "db_1", "coll_3", capped=True, size=1111, max=222)

    with t.run(phase):
        t.source["db_1"].command({"collMod": "coll_1", "cappedSize": 3333, "cappedMax": 444})
        t.source["db_1"].command({"collMod": "coll_2", "cappedSize": 3333})
        t.source["db_1"].command({"collMod": "coll_3", "cappedMax": 444})

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_view(t: Testing, phase: Runner.Phase):
    create_options = {
        "viewOn": "coll_1",
        "pipeline": [{"$match": {"i": {"$gte": 0}}}],
    }
    t.source["db_1"].create_collection("view_1", **create_options)

    options = t.source["db_1"]["view_1"].options()
    assert options == create_options

    with t.run(phase):
        modify_options = {
            "viewOn": "coll_2",
            "pipeline": [{"$match": {"j": {"$gte": 0}}}],
        }

        t.source["db_1"].command({"collMod": "view_1", **modify_options})
        assert t.source["db_1"]["view_1"].options() == modify_options

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_timeseries_options_ignored(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection(
        "coll_1",
        timeseries={"timeField": "ts", "metaField": "meta", "granularity": "seconds"},
    )

    with t.run(phase):
        t.source["db_1"].command({"collMod": "coll_1", "expireAfterSeconds": 123})

    assert "db_1" not in t.target.list_database_names()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_pre_post_images_ignored(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection("coll_1")

    with t.run(phase):
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "changeStreamPreAndPostImages": {"enabled": True},
            }
        )

        options = t.source["db_1"]["coll_1"].options()
        assert options["changeStreamPreAndPostImages"] == {"enabled": True}

    assert "changeStreamPreAndPostImages" not in t.target["db_1"]["coll_1"].options()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_validation_set(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection("coll_1")

    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name"],
            "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
        }
    }

    with t.run(phase):
        t.source["db_1"].command({"collMod": "coll_1", "validator": validator})
        assert t.source["db_1"]["coll_1"].options() == {
            "validator": validator,
            "validationLevel": "strict",  # default
            "validationAction": "error",  # default
        }

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_validation_unset(t: Testing, phase: Runner.Phase):
    create_options = {
        "validator": {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
            }
        },
        "validationLevel": "strict",
        "validationAction": "error",
    }

    t.source["db_1"].create_collection("coll_1", **create_options)
    assert t.source["db_1"]["coll_1"].options() == create_options

    with t.run(phase):
        t.source["db_1"].command({"collMod": "coll_1", "validator": {}})

        modified_options = create_options.copy()
        del modified_options["validator"]
        assert t.source["db_1"]["coll_1"].options() == modified_options

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_capped_size_with_validation(t: Testing, phase: Runner.Phase):
    ensure_collection(t.source, t.target, "db_1", "coll_1", capped=True, size=1111)

    with t.run(phase):
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "cappedSize": 3333,
                "validator": {
                    "$jsonSchema": {
                        "bsonType": "object",
                        "required": ["name"],
                        "properties": {
                            "name": {"bsonType": "string", "description": "must be a string"}
                        },
                    }
                },
            }
        )

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_clustered_ttl_with_validation(t: Testing, phase: Runner.Phase):
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name"],
            "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
        }
    }

    expected_index_options = {"name": "_id_", "key": {"_id": 1}, "unique": True, "v": 2}

    with t.run(phase):
        t.source["db_1"].create_collection(
            "coll_1",
            clusteredIndex={"key": {"_id": 1}, "unique": True},
            expireAfterSeconds=123,
        )
        assert t.source["db_1"]["coll_1"].options() == {
            "clusteredIndex": expected_index_options,
            "expireAfterSeconds": 123,
        }

        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "expireAfterSeconds": 444,
                "validator": validator,
            }
        )

    assert t.source["db_1"]["coll_1"].options() == {
        "clusteredIndex": expected_index_options,
        "expireAfterSeconds": 444,
        "validator": validator,
        "validationLevel": "strict",
        "validationAction": "error",
    }
    assert t.target["db_1"]["coll_1"].options() == {
        "clusteredIndex": expected_index_options,
        "validator": validator,
        "validationLevel": "strict",
        "validationAction": "error",
    }


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_modify_pre_post_images_with_validation(t: Testing, phase: Runner.Phase):
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["name"],
            "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
        }
    }

    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source["db_1"].command(
            {
                "collMod": "coll_1",
                "changeStreamPreAndPostImages": {"enabled": True},
                "validator": validator,
            }
        )

        assert t.source["db_1"]["coll_1"].options() == {
            "changeStreamPreAndPostImages": {"enabled": True},
            "validator": validator,
            "validationLevel": "strict",
            "validationAction": "error",
        }

    assert t.target["db_1"]["coll_1"].options() == {
        "validator": validator,
        "validationLevel": "strict",
        "validationAction": "error",
    }


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_rename(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1")
        t.source["db_1"]["coll_1"].rename("coll_2")

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_rename_created(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection("coll_1")

    with t.run(phase):
        t.source["db_1"]["coll_1"].rename("coll_2")

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_rename_with_drop_target(t: Testing, phase: Runner.Phase):
    t.source["db_1"].create_collection("coll_1")
    t.source["db_1"].create_collection("coll_2")
    t.source["db_1"].create_collection("target_coll_1")

    with t.run(phase):
        t.source["db_1"]["coll_1"].rename("target_coll_1", dropTarget=True)
        t.source["db_1"]["coll_2"].rename("target_coll_2", dropTarget=True)

    t.compare_all()


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
def test_pml_120_capped_size_overflow(t: Testing, phase: Runner.Phase):
    with t.run(phase):
        t.source["db_1"].create_collection("coll_1", capped=True, size=2147483648, max=2147483647)

    t.compare_all()


@pytest.mark.slow
@pytest.mark.timeout(30)
def test_pml_119_clone_collect_size_map_deadlock(t: Testing):
    with t.run(phase=Runner.Phase.CLONE):
        for i in range(50):
            t.source[f"db_{i}"]["coll"].insert_one({})

    t.compare_all()
