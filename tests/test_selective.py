# pylint: disable=missing-docstring,redefined-outer-name
import pytest
from _base import BaseTesting
from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
class TestSelective(BaseTesting):
    def test_create_collection_with_include_only(self, phase):
        self.drop_all_database()

        with self.perform_with_options(
            phase,
            include_ns=[
                "db_0.*",
                "db_1.coll_0",
                "db_1.coll_1",
                "db_2.coll_0",
                "db_2.coll_1",
            ],
        ):
            for db in range(3):
                for coll in range(3):
                    self.source["db_1"]["coll_1"].create_index({"i": 1})
                    self.source[f"db_{db}"][f"coll_{coll}"].insert_one({})

        expected = {
            "db_0.coll_0",
            "db_0.coll_1",
            "db_0.coll_2",
            "db_1.coll_0",
            "db_1.coll_1",
            # "db_1.coll_2",
            "db_2.coll_0",
            "db_2.coll_1",
            # "db_2.coll_2",
        }

        assert expected == self.all_target_namespaces()
        self.check_if_target_is_subset()

    def test_create_collection_with_exclude_only(self, phase):
        self.drop_all_database()

        with self.perform_with_options(
            phase,
            exclude_ns=[
                "db_0.*",
                "db_1.coll_0",
                "db_1.coll_1",
            ],
        ):
            for db in range(3):
                for coll in range(3):
                    self.source["db_1"]["coll_1"].create_index({"i": 1})
                    self.source[f"db_{db}"][f"coll_{coll}"].insert_one({})

        expected = {
            # "db_0.coll_0",
            # "db_0.coll_1",
            # "db_0.coll_2",
            # "db_1.coll_0",
            # "db_1.coll_1",
            "db_1.coll_2",
            "db_2.coll_0",
            "db_2.coll_1",
            "db_2.coll_2",
        }

        assert expected == self.all_target_namespaces()
        self.check_if_target_is_subset()

    def test_create_collection(self, phase):
        self.drop_all_database()

        with self.perform_with_options(
            phase,
            include_ns=[
                "db_0.*",
                "db_1.coll_0",
                "db_1.coll_1",
                "db_2.coll_0",
                "db_2.coll_1",
            ],
            exclude_ns=[
                "db_0.*",
                "db_1.coll_0",
            ],
        ):
            for db in range(3):
                for coll in range(3):
                    self.source["db_1"]["coll_1"].create_index({"i": 1})
                    self.source[f"db_{db}"][f"coll_{coll}"].insert_one({})

        expected = {
            # "db_0.coll_0",
            # "db_0.coll_1",
            # "db_0.coll_2",
            # "db_1.coll_0",
            "db_1.coll_1",
            # "db_1.coll_2",
            "db_2.coll_0",
            "db_2.coll_1",
            # "db_2.coll_2",
        }

        assert expected == self.all_target_namespaces()
        self.check_if_target_is_subset()
