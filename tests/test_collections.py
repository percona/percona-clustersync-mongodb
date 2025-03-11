# pylint: disable=missing-docstring,redefined-outer-name
from datetime import datetime

import pytest
from _base import BaseTesting
from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
class TestCollection(BaseTesting):
    def test_create_implicitly(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"]["coll_1"].insert_one({})

        self.compare_all()

    def test_create(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1")

        self.compare_all()

    def test_create_with_collation(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1", collation={"locale": "en_US"})

        self.compare_all()

    def test_create_diff_uuid(self, phase):
        self.drop_all_database()

        # mongolink does not use applyOps. no possible to preserveUUID
        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1")

        self.compare_all()

        source_info = next(self.source["db_1"].list_collections(filter={"name": "coll_1"}))
        target_info = next(self.target["db_1"].list_collections(filter={"name": "coll_1"}))
        assert source_info["name"] == "coll_1" == target_info["name"]

        if source_info["info"]["uuid"] == target_info["info"]["uuid"]:
            pytest.xfail("colllection UUID should not be equal")

    def test_create_clustered(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "coll_1",
                clusteredIndex={"key": {"_id": 1}, "unique": True},
            )

        self.compare_all()

    def test_create_clustered_ttl_ignored(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "coll_1",
                clusteredIndex={"key": {"_id": 1}, "unique": True},
                expireAfterSeconds=1,
            )

        source_options = self.source["db_1"]["coll_1"].options()
        target_options = self.target["db_1"]["coll_1"].options()

        assert source_options["clusteredIndex"] == target_options["clusteredIndex"]
        assert source_options["expireAfterSeconds"] == 1
        assert "expireAfterSeconds" not in target_options

    def test_create_capped(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1", capped=True, size=54321, max=12345)
            self.source["db_1"]["coll_1"].insert_many({"i": i} for i in range(10))

        self.compare_all()

    def test_create_view(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(-3, 3)])

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "view_1",
                viewOn="coll_1",
                pipeline=[{"$match": {"i": {"$gte": 0}}}],
            )

        self.compare_all()

    def test_create_view_with_collation(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(-3, 3)])

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "view_1",
                viewOn="coll_1",
                pipeline=[{"$match": {"i": {"$gte": 0}}}],
                collation={"locale": "en_US"},
            )

        self.compare_all()

    def test_create_timeseries_ignored(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "coll_1",
                timeseries={"timeField": "ts", "metaField": "meta"},
            )
            self.source["db_1"]["coll_1"].insert_many(
                {"ts": datetime.now(), "meta": {"i": i}} for i in range(10)
            )

        assert self.target["db_1"].list_collection_names() == []

    def test_create_with_storage_options(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            options = {
                "storageEngine": {"wiredTiger": {"configString": "block_compressor=snappy"}},
                "indexOptionDefaults": {
                    "storageEngine": {"wiredTiger": {"configString": "block_compressor=zlib"}},
                },
            }
            self.source["db_1"].create_collection("coll_1", **options)
            assert self.source["db_1"]["coll_1"].options() == options

        self.compare_all()

    def test_create_with_pre_post_images_ignored(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            options = {
                "changeStreamPreAndPostImages": {"enabled": True},
            }
            self.source["db_1"].create_collection("coll_1", **options)
            assert self.source["db_1"]["coll_1"].options() == options

        assert "changeStreamPreAndPostImages" not in self.target["db_1"]["coll_1"].options()

    def test_create_with_validator_ignored(self, phase):
        self.drop_all_database()

        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
            }
        }
        create_options = {
            "validator": validator,
            "validationLevel": "strict",
            "validationAction": "error",
        }

        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1", **create_options)
            assert self.source["db_1"]["coll_1"].options() == create_options

        assert self.target["db_1"]["coll_1"].options() == {}

    def test_drop_collection(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"].drop_collection("coll_1")

        assert "coll_1" not in self.target["db_1"].list_collection_names()

    def test_drop_capped_collection(self, phase):
        self.drop_all_database()
        self.source["db_1"].create_collection("coll_1", capped=True, size=54321, max=12345)
        self.source["db_1"]["coll_1"].insert_many({"i": i} for i in range(10))

        with self.perform(phase):
            self.source["db_1"].drop_collection("coll_1")

        assert "coll_1" not in self.target["db_1"].list_collection_names()

    def test_drop_view(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")
        self.create_view("db_1", "view_1", "coll_1", [{"$match": {"i": {"$gt": 3}}}])

        with self.perform(phase):
            self.source["db_1"].drop_collection("view_1")

        assert "view_1" not in self.target["db_1"].list_collection_names()
        assert "coll_1" in self.target["db_1"].list_collection_names()

    def test_drop_view_source_collection(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")
        self.create_view("db_1", "view_1", "coll_1", [{"$match": {"i": {"$gt": 3}}}])

        with self.perform(phase):
            self.source["db_1"].drop_collection("coll_1")

        assert "view_1" in self.target["db_1"].list_collection_names()
        assert "coll_1" not in self.target["db_1"].list_collection_names()

    def test_drop_database(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1")
            self.source["db_1"].create_collection(
                "view_1",
                viewOn="coll_1",
                pipeline=[{"$match": {"i": {"$gte": 0}}}],
            )
            self.source.drop_database("db_1")

        if phase == Runner.Phase.CLONE:
            # clone started after view has been dropped
            assert self.target["db_1"].list_collection_names() == []
        else:
            # view was dropped after the clone had started
            assert self.target["db_1"].list_collection_names() == ["system.views"]

    def test_modify_clustered_ttl_ignored(self, phase):
        self.drop_all_database()

        self.source["db_1"].create_collection(
            "coll_1",
            clusteredIndex={"key": {"_id": 1}, "unique": True},
            expireAfterSeconds=123,
        )

        expected_index_options = {"name": "_id_", "key": {"_id": 1}, "unique": True, "v": 2}
        assert self.source["db_1"]["coll_1"].options() == {
            "clusteredIndex": expected_index_options,
            "expireAfterSeconds": 123,
        }

        with self.perform(phase):
            self.source["db_1"].command({"collMod": "coll_1", "expireAfterSeconds": 444})

        assert self.source["db_1"]["coll_1"].options() == {
            "clusteredIndex": expected_index_options,
            "expireAfterSeconds": 444,
        }
        assert self.target["db_1"]["coll_1"].options() == {
            "clusteredIndex": expected_index_options,
        }

    def test_modify_capped_size(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1", capped=True, size=1111, max=222)
        self.create_collection("db_1", "coll_2", capped=True, size=1111, max=222)
        self.create_collection("db_1", "coll_3", capped=True, size=1111, max=222)

        for coll in self.source["db_1"].list_collections():
            assert coll["options"] == dict(capped=True, size=1111, max=222)

        with self.perform(phase):
            self.source["db_1"].command({"collMod": "coll_1", "cappedSize": 3333, "cappedMax": 444})
            self.source["db_1"].command({"collMod": "coll_2", "cappedSize": 3333})
            self.source["db_1"].command({"collMod": "coll_3", "cappedMax": 444})

        assert self.source["db_1"]["coll_1"].options() == dict(capped=True, size=3333, max=444)
        assert self.source["db_1"]["coll_2"].options() == dict(capped=True, size=3333, max=222)
        assert self.source["db_1"]["coll_3"].options() == dict(capped=True, size=1111, max=444)

        self.compare_all()

    def test_modify_view(self, phase):
        self.drop_all_database()
        create_options = {
            "viewOn": "coll_1",
            "pipeline": [{"$match": {"i": {"$gte": 0}}}],
        }
        self.source["db_1"].create_collection("view_1", **create_options)

        options = self.source["db_1"]["view_1"].options()
        assert options == create_options

        with self.perform(phase):
            modify_options = {
                "viewOn": "coll_2",
                "pipeline": [{"$match": {"j": {"$gte": 0}}}],
            }

            self.source["db_1"].command({"collMod": "view_1", **modify_options})
            assert self.source["db_1"]["view_1"].options() == modify_options

        self.compare_all()

    def test_modify_timeseries_options_ignored(self, phase):
        self.drop_all_database()
        self.source["db_1"].create_collection(
            "coll_1",
            timeseries={"timeField": "ts", "metaField": "meta", "granularity": "seconds"},
        )

        with self.perform(phase):
            self.source["db_1"].command({"collMod": "coll_1", "expireAfterSeconds": 123})

        assert "db_1" not in self.target.list_database_names()

    def test_modify_pre_post_images_ignored(self, phase):
        self.drop_all_database()
        self.source["db_1"].create_collection("coll_1")

        with self.perform(phase):
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "changeStreamPreAndPostImages": {"enabled": True},
                }
            )

            options = self.source["db_1"]["coll_1"].options()
            assert options["changeStreamPreAndPostImages"] == {"enabled": True}

        assert "changeStreamPreAndPostImages" not in self.target["db_1"]["coll_1"].options()

    def test_modify_validator_ignored(self, phase):
        self.drop_all_database()
        self.source["db_1"].create_collection("coll_1")

        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
            }
        }
        modify_options = {
            "validator": validator,
            "validationLevel": "strict",
            "validationAction": "error",
        }

        with self.perform(phase):
            self.source["db_1"].command({"collMod": "coll_1", **modify_options})
            assert self.source["db_1"]["coll_1"].options() == modify_options

        assert self.target["db_1"]["coll_1"].options() == {}

    def test_modify_validator_unset_ignored(self, phase):
        self.drop_all_database()

        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["name"],
                "properties": {"name": {"bsonType": "string", "description": "must be a string"}},
            }
        }
        create_options = {
            "validator": validator,
            "validationLevel": "strict",
            "validationAction": "error",
        }

        self.source["db_1"].create_collection("coll_1", **create_options)
        assert self.source["db_1"]["coll_1"].options() == create_options

        with self.perform(phase):
            self.source["db_1"].command({"collMod": "coll_1", "validator": {}})

            modified_options = create_options
            del modified_options["validator"]
            assert self.source["db_1"]["coll_1"].options() == modified_options

        assert self.target["db_1"]["coll_1"].options() == {}

    def test_rename(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].rename("coll_2")

        self.compare_all()

    def test_rename_with_drop_target(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")
        self.create_collection("db_1", "coll_2")
        self.create_collection("db_1", "target_coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].rename("target_coll_1", dropTarget=True)
            self.source["db_1"]["coll_2"].rename("target_coll_2", dropTarget=True)

        self.compare_all()
