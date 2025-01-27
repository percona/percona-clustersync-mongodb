# pylint: disable=missing-docstring,redefined-outer-name
import pytest
from _base import BaseTesting

from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.CLONE, Runner.Phase.APPLY])
class TestIndexes(BaseTesting):
    def test_create(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1})

        self.compare_all()

    def test_create_with_collation(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, collation={"locale": "en_US"})

        self.compare_all()

    def test_create_unique(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, unique=True)

        self.compare_all()

    def test_create_sparse(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, sparse=True)

        self.compare_all()

    def test_create_partial(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index(
                {"i": 1},
                partialFilterExpression={"j": {"$gt": 5}},
            )

        self.compare_all()

    def test_create_hidden(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, hidden=True)

        self.compare_all()

    def test_create_hashed(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": "hashed"})

        self.compare_all()

    def test_create_compound(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1, "j": -1})

        self.compare_all()

    def test_create_wildcard(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"$**": 1})

        self.compare_all()

    def test_create_geospatial(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"loc1": "2d"})
            self.source["db_1"]["coll_1"].create_index({"loc2": "2dsphere"})

        self.compare_all()

    def test_drop_cloned(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")
        self.create_index("db_1", "coll_1", [("i", 1)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].drop_index([("i", 1)])

        self.compare_all()

    def test_drop_created(self, phase):
        self.drop_database("db_1")
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            index_name = self.source["db_1"]["coll_1"].create_index({"i": 1})
            self.source["db_1"]["coll_1"].drop_index(index_name)

        self.compare_all()
