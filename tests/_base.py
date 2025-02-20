# pylint: disable=missing-docstring,redefined-outer-name
import hashlib
from typing import List

import bson
import pytest
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from mlink import MongoLink, Runner


@pytest.mark.usefixtures("cls_source", "cls_target", "cls_mlink")
class BaseTesting:
    """BaseTesting provides common setup and utility methods for MongoLink tests."""

    source: MongoClient
    target: MongoClient
    mlink: MongoLink

    def perform(self, phase: Runner.Phase):
        """Perform the MongoLink operation for the given phase."""
        return self.perform_with_options(phase, {})

    def perform_with_options(
        self,
        phase: Runner.Phase,
        include_ns=None,
        exclude_ns=None,
    ):
        """Perform the MongoLink operation with the given options."""
        mlink_options = {}
        if include_ns:
            mlink_options["includeNamespaces"] = include_ns
        if exclude_ns:
            mlink_options["excludeNamespaces"] = exclude_ns
        return Runner(self.source, self.mlink, phase, mlink_options)

    @classmethod
    def all_target_namespaces(cls):
        """Return all namespaces in the target MongoDB."""
        rv = set()
        for db in cls.target.list_database_names():
            if db in ("admin", "config", "local"):
                continue
            for coll in cls.target[db].list_collection_names():
                rv.add(f"{db}.{coll}")
        return rv

    @classmethod
    def compare_all(cls):
        """Compare all databases and collections between source and target MongoDB."""
        source_dbs = cls.list_databases(cls.source)
        target_dbs = cls.list_databases(cls.target)
        assert source_dbs == target_dbs, f"{source_dbs} != {target_dbs}"

        for db in source_dbs:
            source_colls = cls.list_namespaces(cls.source, db)
            target_colls = cls.list_namespaces(cls.target, db)
            assert source_colls == target_colls, f"{source_colls} != {target_colls}"

            for coll in source_colls:
                cls.compare_namespace(db, coll)

    @classmethod
    def check_if_target_is_subset(cls):
        """Check if the target MongoDB is a subset of the source MongoDB."""
        source_dbs = cls.list_databases(cls.source)
        target_dbs = cls.list_databases(cls.target)
        assert set(target_dbs).issubset(source_dbs)

        for db in target_dbs:
            source_colls = cls.list_namespaces(cls.source, db)
            target_colls = cls.list_namespaces(cls.target, db)
            assert set(target_colls).issubset(source_colls)

            for coll in target_colls:
                cls.compare_namespace(db, coll)

    @staticmethod
    def list_databases(client: MongoClient):
        """List all databases in the given MongoClient."""
        rv = set()
        for name in client.list_database_names():
            if name not in ("admin", "config", "local"):
                rv.add(name)
        return rv

    @staticmethod
    def list_namespaces(client: MongoClient, db: str):
        """List all namespaces in the given database."""
        rv = set()
        for name in client[db].list_collection_names():
            if not name.startswith("system."):
                rv.add(name)
        return rv

    @classmethod
    def compare_namespace(cls, db: str, coll: str):
        """Compare the given namespace between source and target MongoDB."""
        source_options = cls.source[db][coll].options()
        target_options = cls.target[db][coll].options()
        assert source_options == target_options, f"{source_options=} != {target_options=}"

        if "viewOn" not in source_options:
            source_indexes = cls.source[db][coll].index_information()
            target_indexes = cls.target[db][coll].index_information()
            assert source_indexes == target_indexes, f"{source_indexes=} != {target_indexes=}"

        source_docs, source_hash = cls._coll_content(cls.source[db][coll])
        target_docs, target_hash = cls._coll_content(cls.target[db][coll])
        assert len(source_docs) == len(target_docs), f"{source_docs=} != {target_docs=}"
        assert source_docs == target_docs, f"{source_docs=} != {target_docs=}"
        assert source_hash == target_hash, f"{source_hash=} != {target_hash=}"

    @staticmethod
    def _coll_content(coll: Collection):
        """Get the content and hash of the given collection."""
        docs, md5 = [], hashlib.md5()
        for data in coll.find_raw_batches(sort=[("_id", ASCENDING)]):
            md5.update(data)
            docs.extend(bson.decode_all(data))
        return docs, md5.hexdigest()

    def drop_all_database(self):
        """Drop all databases in the source and target MongoDB."""
        for db in self.source.list_database_names():
            if db not in ("admin", "config", "local"):
                self.source.drop_database(db)
        for db in self.target.list_database_names():
            if db not in ("admin", "config", "local"):
                self.target.drop_database(db)

    def drop_database(self, db: str):
        """Drop the given database in the source and target MongoDB."""
        self.source.drop_database(db)
        self.target.drop_database(db)

    def create_collection(self, db: str, coll: str, **kwargs):
        """Create a collection in the source and target MongoDB."""
        self.source[db].create_collection(coll, **kwargs)
        self.target[db].create_collection(coll, **kwargs)

    def create_index(self, db: str, coll: str, keys, **kwargs):
        """Create an index in the source and target MongoDB."""
        self.source[db][coll].create_index(keys, **kwargs)
        self.source[db][coll].create_index(keys, **kwargs)

    def create_view(self, db: str, view_name: str, source: str, pipeline: List[dict]):
        """Create a view in the source and target MongoDB."""
        self.source[db].create_collection(view_name, viewOn=source, pipeline=pipeline)
        self.target[db].create_collection(view_name, viewOn=source, pipeline=pipeline)

    def insert_documents(self, db: str, coll: str, documents: List[dict]):
        """Insert documents into the source and target MongoDB."""
        self.source[db][coll].insert_many(documents)
        self.target[db][coll].insert_many(documents)
