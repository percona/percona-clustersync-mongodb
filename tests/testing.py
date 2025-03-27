# pylint: disable=missing-docstring,redefined-outer-name
import hashlib

import bson
from mlink import MongoLink, Runner
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection


class Testing:
    __test__ = False

    def __init__(self, source: MongoClient, target: MongoClient, mlink: MongoLink):
        self.source: MongoClient = source
        self.target: MongoClient = target
        self.mlink: MongoLink = mlink

    def run(self, phase: Runner.Phase, wait_timeout=None):
        """Perform the MongoLink operation for the given phase."""
        return Runner(self.source, self.mlink, phase, {}, wait_timeout=wait_timeout)

    def compare_all(self, sort=None):
        """Compare all databases and collections between source and target MongoDB."""
        source_dbs = set(list_databases(self.source))
        target_dbs = set(list_databases(self.target))
        assert source_dbs == target_dbs, f"{source_dbs} != {target_dbs}"

        for db in source_dbs:
            source_colls = set(list_collections(self.source, db))
            target_colls = set(list_collections(self.target, db))
            assert source_colls == target_colls, f"{db} :: {source_colls} != {target_colls}"

            for coll in source_colls:
                compare_namespace(self.source, self.target, db, coll, sort)


def list_databases(client: MongoClient):
    """List all databases in the given MongoClient."""
    for name in client.list_database_names():
        if name not in ("admin", "config", "local", "percona_mongolink"):
            yield name


def list_collections(client: MongoClient, db: str):
    """List all namespaces in the given database."""
    for name in client[db].list_collection_names():
        if not name.startswith("system."):
            yield name


def list_all_namespaces(client: MongoClient):
    """Return all namespaces in the target MongoDB."""
    for db in list_databases(client):
        for coll in list_collections(client, db):
            yield f"{db}.{coll}"


def compare_namespace(source: MongoClient, target: MongoClient, db: str, coll: str, sort=None):
    """Compare the given namespace between source and target MongoDB."""
    ns = f"{db}.{coll}"

    source_options = source[db][coll].options()
    target_options = target[db][coll].options()
    assert source_options == target_options, f"{ns}: {source_options=} != {target_options=}"

    if "viewOn" not in source_options:
        source_indexes = source[db][coll].index_information()
        target_indexes = target[db][coll].index_information()
        assert source_indexes == target_indexes, f"{ns}: {source_indexes=} != {target_indexes=}"

    source_count, source_hash = _coll_content(source[db][coll], sort)
    target_count, target_hash = _coll_content(target[db][coll], sort)
    assert source_count == target_count, f"{ns}: {source_count=} != {target_count=}"
    assert source_hash == target_hash, f"{ns}: {source_hash=} != {target_hash=}"


def _coll_content(coll: Collection, sort=None):
    """Get the content and hash of the given collection."""
    if not sort:
        sort = [("_id", ASCENDING)]

    count, md5 = 0, hashlib.md5()
    for data in coll.find_raw_batches(sort=sort):
        md5.update(data)
        count += len(bson.decode_all(data))
    return count, md5.hexdigest()
