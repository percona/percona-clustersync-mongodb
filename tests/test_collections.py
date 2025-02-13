# pylint: disable=missing-docstring,redefined-outer-name
from datetime import datetime

import pytest
from _base import BaseTesting

from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.CLONE, Runner.Phase.APPLY])
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

    @pytest.mark.xfail
    def test_create_equal_uuid(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection("coll_1")

        self.compare_all()

        source_info = next(self.source["db_1"].list_collections(filter={"name": "coll_1"}))
        target_info = next(self.target["db_1"].list_collections(filter={"name": "coll_1"}))
        assert source_info["name"] == "coll_1" == target_info["name"]

        if source_info["info"]["uuid"] != target_info["info"]["uuid"]:
            pytest.xfail("colllection UUID may vary")

    def test_create_clustered(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "coll_1",
                clusteredIndex={"key": {"_id": 1}, "unique": True},
            )

        self.compare_all()

    def test_create_clustered_ttl(self, phase):
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

    def test_timeseries_is_not_replicated(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_2")

        with self.perform(phase):
            self.source["db_1"].create_collection(
                "coll_1",
                timeseries={"timeField": "ts", "metaField": "meta"},
            )
            self.source["db_1"]["coll_1"].insert_many(
                {"ts": datetime.now(), "meta": {"i": i}} for i in range(10)
            )

        assert "test" not in self.target.list_database_names()

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
        self.create_collection("db_1", "coll_1")
        self.create_view("db_1", "view_1", "coll_1", [{"$match": {"i": {"$gt": 3}}}])

        with self.perform(phase):
            self.source.drop_database("test")

        assert "test" not in self.target.list_database_names()
