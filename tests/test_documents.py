# pylint: disable=missing-docstring,redefined-outer-name
import pymongo
import pytest
from _base import BaseTesting

from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.CLONE, Runner.Phase.APPLY])
class TestCRUDOperation(BaseTesting):
    def test_insert_one(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].insert_one({"i": i})

        self.compare_all()

    def test_insert_many(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].insert_many([{"i": i} for i in range(5)])

        self.compare_all()

    def test_update_one(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].update_one(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )

        self.compare_all()

    def test_update_many(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].update_many({}, {"$inc": {"i": 100}})

        self.compare_all()

    def test_replace_one(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].replace_one(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )

        self.compare_all()

    def test_delete_one(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].delete_one({"i": 4})

        self.compare_all()

    def test_delete_many(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        assert self.source["db_1"]["coll_1"].count_documents({}) == 5

        with self.perform(phase):
            self.source["db_1"]["coll_1"].delete_many({"i": {"$gt": 2}})

        assert self.source["db_1"]["coll_1"].count_documents({}) == 3

        self.compare_all()

    def test_find_one_and_update(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                )

        self.compare_all()

    def test_find_one_and_replace(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                )

        self.compare_all()

    def test_find_one_and_delete(self, phase):
        self.drop_all_database()
        self.insert_documents("db_1", "coll_1", [{"i": i} for i in range(5)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].find_one_and_delete({"i": 4})

        self.compare_all()

    def test_upsert_by_update_one(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].update_one(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_upsert_by_update_many(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].update_many({}, {"$inc": {"i": 100}}, upsert=True)

        self.compare_all()

    def test_upsert_by_replace_one(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].replace_one(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_upsert_by_find_one_and_update(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_update(
                    {"i": i},
                    {
                        "$inc": {"i": (i * 100) - i},
                        "$set": {f"field_{i}": f"value_{i}"},
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_upsert_by_find_one_and_replace(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            for i in range(5):
                self.source["db_1"]["coll_1"].find_one_and_replace(
                    {"i": i},
                    {
                        "i": (i * 100) - i,
                        f"field_{i}": f"value_{i}",
                    },
                    upsert=True,
                )

        self.compare_all()

    def test_bulk_write(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            ops = [
                pymongo.InsertOne({"i": 1}),
                pymongo.InsertOne({"i": 2}),
                pymongo.InsertOne({"i": 3}),
                pymongo.InsertOne({"i": 4}),
                pymongo.UpdateOne({"i": 1}, {"$set": {"i": "1"}}),
                pymongo.DeleteOne({"i": 2}),
                pymongo.ReplaceOne({"i": 4}, {"k": "4"}),
            ]
            self.source["db_1"]["coll_1"].bulk_write(ops, ordered=True)

        coll_1 = []
        for doc in self.source["db_1"]["coll_1"].find():
            del doc["_id"]
            coll_1.append(doc)

        assert coll_1 == [{"i": "1"}, {"i": 3}, {"k": "4"}]
        self.compare_all()
