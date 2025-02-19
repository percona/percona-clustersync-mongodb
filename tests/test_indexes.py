# pylint: disable=missing-docstring,redefined-outer-name
from datetime import datetime

import pymongo
import pytest
from _base import BaseTesting

from mlink import Runner


@pytest.mark.parametrize("phase", [Runner.Phase.APPLY, Runner.Phase.CLONE])
class TestIndexes(BaseTesting):
    def test_create(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1})

        self.compare_all()

    def test_create_with_collation(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, collation={"locale": "en_US"})

        self.compare_all()

    def test_create_unique(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, unique=True)

        self.compare_all()

    def test_create_prepare_unique(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, prepareUnique=True)

        self.compare_all()

    def test_create_sparse(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, sparse=True)

        self.compare_all()

    def test_create_partial(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index(
                {"i": 1},
                partialFilterExpression={"j": {"$gt": 5}},
            )

        self.compare_all()

    def test_create_hidden(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase) as mlink:
            name = "i_1"
            self.source["db_1"]["coll_1"].create_index({"i": 1}, name=name, hidden=True)
            assert self.source["db_1"]["coll_1"].index_information()[name]["hidden"]

            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                assert "hidden" not in self.target["db_1"]["coll_1"].index_information()[name]

        self.compare_all()

    def test_create_hashed(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": pymongo.HASHED})

        self.compare_all()

    def test_create_compound(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1, "j": -1})

        self.compare_all()

    def test_create_multikey(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i.j": 1})

        self.compare_all()

    def test_create_wildcard(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"$**": 1})

        self.compare_all()

    def test_create_wildcard_projection(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"$**": 1}, wildcardProjection={"a.*": 1})

        self.compare_all()

    @pytest.mark.xfail(reason="IndexOptionsConflict")
    def test_create_geospatial(self, phase):
        # FIXME(phase:clone): create indexes error
        #   (IndexOptionsConflict) An equivalent index already exists with the same name but different options.
        #       Requested index: { v: 2, key: { loc2: \"2dsphere\" }, name: \"loc2_2dsphere\",
        #                          bits: 30, min: -179.0, max: 178.0, 2dsphereIndexVersion: 2 },
        #       existing index: { v: 2, key: { loc2: \"2dsphere\" }, name: \"loc2_2dsphere\",
        #                          2dsphereIndexVersion: 2 }
        # op=createIndexes
        # s=repl:apply
        #
        # reason:
        #  [clone] (1) create with 2dsphere index.
        #  [repl]  (1) create 2dsphere with empty bits, min, max fields.
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            options = {"bits": 30, "min": -179.0, "max": 178.0, "2dsphereIndexVersion": 2}
            self.source["db_1"]["coll_1"].create_index({"loc1": pymongo.GEO2D}, **options)
            self.source["db_1"]["coll_1"].create_index({"loc2": pymongo.GEOSPHERE}, **options)

        self.compare_all()

    def test_create_text(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index(
                [("title", pymongo.TEXT), ("description", pymongo.TEXT)],
                name="ArticlesTextIndex",
                default_language="english",
                language_override="language",
                weights={"title": 10, "description": 5},
            )

        self.compare_all()

    def test_create_text_wildcard(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"$**": "text"})

        self.compare_all()

    def test_create_ttl(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            self.source["db_1"]["coll_1"].create_index({"i": 1}, expireAfterSeconds=1)

        self.compare_all()

    def test_drop_cloned(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")
        self.create_index("db_1", "coll_1", [("i", 1)])

        with self.perform(phase):
            self.source["db_1"]["coll_1"].drop_index([("i", 1)])

        self.compare_all()

    def test_drop_created(self, phase):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        with self.perform(phase):
            index_name = self.source["db_1"]["coll_1"].create_index({"i": 1})
            self.source["db_1"]["coll_1"].drop_index(index_name)

        self.compare_all()

    def test_modify_hide(self, phase):
        self.drop_all_database()
        index_name = self.source["db_1"]["coll_1"].create_index({"i": 1})

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert "hidden" not in indexes[index_name]

        with self.perform(phase):
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {
                        "name": index_name,
                        "hidden": True,
                    },
                }
            )

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert indexes[index_name]["hidden"]

        self.compare_all()

    def test_modify_unhide(self, phase):
        self.drop_all_database()
        index_name = self.source["db_1"]["coll_1"].create_index({"i": 1}, hidden=True)

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert "hidden" in indexes[index_name]

        with self.perform(phase):
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {
                        "keyPattern": {"i": 1},
                        "hidden": False,
                    },
                }
            )

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert "hidden" not in indexes[index_name]

        self.compare_all()

    def test_modify_ttl(self, phase):
        self.drop_all_database()
        index_name = self.source["db_1"]["coll_1"].create_index({"i": 1}, expireAfterSeconds=123)

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert indexes[index_name]["expireAfterSeconds"] == 123

        with self.perform(phase):
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {
                        "keyPattern": {"i": 1},
                        "expireAfterSeconds": 432,
                    },
                }
            )

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert indexes[index_name]["expireAfterSeconds"] == 432

        self.compare_all()

    def test_modify_unique(self, phase):
        self.drop_all_database()
        index_name = self.source["db_1"]["coll_1"].create_index({"i": 1})

        indexes = self.source["db_1"]["coll_1"].index_information()
        assert "prepareUnique" not in indexes[index_name]
        assert "unique" not in indexes[index_name]

        with self.perform(phase):
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {"keyPattern": {"i": 1}, "prepareUnique": True},
                }
            )

            indexes = self.source["db_1"]["coll_1"].index_information()
            assert indexes[index_name]["prepareUnique"]
            assert "unique" not in indexes[index_name]

            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {
                        "keyPattern": {"i": 1},
                        "unique": True,
                    },
                }
            )

            indexes = self.source["db_1"]["coll_1"].index_information()
            assert "prepareUnique" not in indexes[index_name]
            assert indexes[index_name]["unique"]

        self.compare_all()

    def test_internal_create_many_props(self, phase):
        self.drop_all_database()

        with self.perform(phase) as mlink:
            options = {
                "unique": True,
                "hidden": True,
                "expireAfterSeconds": 3600,
            }
            index_name = self.source["db_1"]["coll_1"].create_index({"i": 1}, **options)

            source_index = self.source["db_1"]["coll_1"].index_information()[index_name]
            for prop, val in options.items():
                assert source_index.get(prop) == val

            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                target_index = self.target["db_1"]["coll_1"].index_information()[index_name]
                for prop, val in options.items():
                    if prop == "expireAfterSeconds":
                        assert target_index["expireAfterSeconds"] == (2**31) - 1
                    else:
                        assert not target_index.get(prop)

        self.compare_all()

    def test_internal_modify_many_props(self, phase):
        self.drop_all_database()
        index_name = self.source["db_1"]["coll_1"].create_index({"i": 1})

        with self.perform(phase) as mlink:
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {"name": index_name, "prepareUnique": True},
                }
            )

            source_index = self.source["db_1"]["coll_1"].index_information()[index_name]
            assert source_index["prepareUnique"]

            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                target_index = self.target["db_1"]["coll_1"].index_information()[index_name]
                assert "prepareUnique" not in target_index

            modify_options = {
                "unique": True,
                "hidden": True,
                "expireAfterSeconds": 3600,
            }
            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {"name": index_name, **modify_options},
                },
            )

            source_index = self.source["db_1"]["coll_1"].index_information()[index_name]
            for prop, val in modify_options.items():
                assert source_index.get(prop) == val

            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                target_index = self.target["db_1"]["coll_1"].index_information()[index_name]
                for prop, val in modify_options.items():
                    if prop == "expireAfterSeconds":
                        assert target_index["expireAfterSeconds"] == (2**31) - 1
                    else:
                        assert not target_index.get(prop)

        self.compare_all()

    def test_internal_modify_index_props_complex(self, phase):
        self.drop_all_database()
        index_key = {"i": 1}
        index_name = self.source["db_1"]["coll_1"].create_index(index_key, prepareUnique=True)

        source_index1 = self.source["db_1"]["coll_1"].index_information()[index_name]
        assert source_index1["prepareUnique"]
        assert "unique" not in source_index1
        assert "hidden" not in source_index1
        assert "expireAfterSeconds" not in source_index1

        with self.perform(phase) as mlink:
            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                target_index = self.target["db_1"]["coll_1"].index_information()[index_name]
                assert "prepareUnique" not in target_index
                assert "unique" not in target_index
                assert "hidden" not in target_index
                assert "expireAfterSeconds" not in target_index

            self.source["db_1"].command(
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

            source_index2 = self.source["db_1"]["coll_1"].index_information()[index_name]
            assert "prepareUnique" not in source_index2
            assert source_index2["unique"]
            assert source_index2["hidden"]
            assert source_index2["expireAfterSeconds"] == 132

            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                target_index = self.target["db_1"]["coll_1"].index_information()[index_name]
                assert "prepareUnique" not in target_index
                assert "unique" not in target_index
                assert "hidden" not in target_index
                assert target_index["expireAfterSeconds"] == (2**31) - 1

            self.source["db_1"].command(
                {
                    "collMod": "coll_1",
                    "index": {
                        "keyPattern": index_key,
                        "prepareUnique": True,  # do nothing
                        "expireAfterSeconds": 133,
                    },
                }
            )

            source_index3 = self.source["db_1"]["coll_1"].index_information()[index_name]
            assert "prepareUnique" not in source_index2
            assert source_index3["unique"]
            assert source_index3["hidden"]
            assert source_index3["expireAfterSeconds"] == 133

            if phase is Runner.Phase.APPLY:
                mlink.wait_for_current_optime()
                target_index = self.target["db_1"]["coll_1"].index_information()[index_name]
                assert "prepareUnique" not in target_index
                assert "unique" not in target_index
                assert "hidden" not in target_index
                assert target_index["expireAfterSeconds"] == (2**31) - 1

        target_index = self.source["db_1"]["coll_1"].index_information()[index_name]
        assert target_index["unique"]
        assert target_index["hidden"]
        assert target_index["expireAfterSeconds"] == 133

        self.compare_all()


class TestIndexesManually(BaseTesting):
    def test_create_ttl_manual(self):
        self.drop_all_database()
        self.create_collection("db_1", "coll_1")

        mlink = self.perform(None)
        try:
            self.source["db_1"]["coll_1"].create_index({"a": 1}, expireAfterSeconds=1)
            mlink.start()
            self.source["db_1"]["coll_1"].create_index({"b": 1}, expireAfterSeconds=1)
            mlink.wait_for_finalizable()
            self.source["db_1"]["coll_1"].create_index({"c": 1}, expireAfterSeconds=1)
            mlink.finalize()
        except:
            mlink.finalize_fast()
            raise

        self.compare_all()


class TestIndexFixes(BaseTesting):
    @pytest.mark.parametrize("phase", [Runner.Phase.CLONE, Runner.Phase.APPLY])
    def test_pml_56_ttl_mismatch(self, phase):
        self.drop_all_database()

        with self.perform(phase):
            self.source["db_1"].drop_collection("coll_1")
            self.source["db_1"]["coll_1"].insert_many(
                [
                    {"created_at": datetime.now(), "short_lived": True},
                    {"created_at": datetime.now(), "long_lived": True},
                ]
            )
            self.source["db_1"]["coll_1"].create_index(
                {"created_at": 1},
                name="short_ttl_index",
                expireAfterSeconds=1260,
            )

            source_indexes = self.source["db_1"]["coll_1"].index_information()
            assert source_indexes["short_ttl_index"]["expireAfterSeconds"] == 1260

        target_indexes = self.target["db_1"]["coll_1"].index_information()
        assert target_indexes["short_ttl_index"]["expireAfterSeconds"] == 1260

        self.compare_all()
