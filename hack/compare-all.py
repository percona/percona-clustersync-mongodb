#!/usr/bin/env python3
"""
Compare all databases and collections between source and target MongoDB clusters.

Compares databases, collections, indexes, collection options, document counts,
and content hashes to verify data consistency between clusters.

Reports every mismatch it finds and exits non-zero, instead of stopping at the
first difference.

Usage:
    hack/compare-all.py
    SKIP_DBS=db_live hack/compare-all.py

Environment variables:
    SRC_URI   MongoDB connection string for source (default: mongodb://src-mongos:27017)
    TGT_URI   MongoDB connection string for target (default: mongodb://tgt-mongos:29017)
    SKIP_DBS  Comma-separated database names to exclude from the comparison
"""

import argparse
import hashlib
import os
import sys

import bson
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

INTERNAL_DBS = frozenset(("admin", "config", "local", "percona_clustersync_mongodb"))


def list_databases(client: MongoClient, skip_dbs: set[str] | frozenset[str]):
    """List all databases in the given MongoClient."""
    for name in client.list_database_names():
        if name not in skip_dbs:
            yield name


def list_collections(client: MongoClient, db: str):
    """List all namespaces in the given database."""
    for name in client[db].list_collection_names():
        if not name.startswith("system."):
            yield name


def list_all_namespaces(client: MongoClient, skip_dbs: set[str] | frozenset[str]):
    """Return all namespaces in the target MongoDB."""
    for db in list_databases(client, skip_dbs):
        for coll in list_collections(client, db):
            yield f"{db}.{coll}"


def _coll_content(coll: Collection, sort=None):
    """Get the content and hash of the given collection."""
    if not sort:
        sort = [("_id", ASCENDING)]

    count, md5 = 0, hashlib.md5()
    for data in coll.find_raw_batches(sort=sort):
        md5.update(data)
        count += len(bson.decode_all(data))
    return count, md5.hexdigest()


def compare_namespace(
    source: MongoClient, target: MongoClient, db: str, coll: str, sort=None
) -> list[str]:
    """Compare the given namespace between source and target MongoDB.

    Returns one line per mismatch. An empty list means the namespace matches.
    """
    ns = f"{db}.{coll}"
    mismatches = []

    source_options = source[db][coll].options()
    target_options = target[db][coll].options()
    if source_options != target_options:
        mismatches.append(f"MISMATCH {ns}: options {source_options} vs {target_options}")

    if "viewOn" not in source_options:
        source_indexes = source[db][coll].index_information()
        target_indexes = target[db][coll].index_information()
        if source_indexes != target_indexes:
            mismatches.append(f"MISMATCH {ns}: indexes {source_indexes} vs {target_indexes}")

    source_count, source_hash = _coll_content(source[db][coll], sort)
    target_count, target_hash = _coll_content(target[db][coll], sort)
    if source_count != target_count:
        mismatches.append(f"MISMATCH {ns}: count {source_count} vs {target_count}")
    if source_hash != target_hash:
        mismatches.append(f"MISMATCH {ns}: content hash {source_hash} vs {target_hash}")

    return mismatches


def compare_clusters(
    source: MongoClient,
    target: MongoClient,
    skip_dbs: set[str] | frozenset[str],
) -> list[str]:
    """Compare source and target clusters and return every mismatch."""
    failures: list[str] = []
    source_dbs = set(list_databases(source, skip_dbs))
    target_dbs = set(list_databases(target, skip_dbs))

    failures += [
        f"MISMATCH {db}: database missing on target" for db in sorted(source_dbs - target_dbs)
    ]
    failures += [
        f"MISMATCH {db}: database missing on source" for db in sorted(target_dbs - source_dbs)
    ]

    for db in sorted(source_dbs & target_dbs):
        source_colls = set(list_collections(source, db))
        target_colls = set(list_collections(target, db))

        failures += [
            f"MISMATCH {db}.{coll}: collection missing on target"
            for coll in sorted(source_colls - target_colls)
        ]
        failures += [
            f"MISMATCH {db}.{coll}: collection missing on source"
            for coll in sorted(target_colls - source_colls)
        ]

        for coll in sorted(source_colls & target_colls):
            print(f"Comparing {db}.{coll}...")
            failures += compare_namespace(source, target, db, coll)

    return failures


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.parse_args()

    excluded_dbs = {
        name.strip() for name in os.environ.get("SKIP_DBS", "").split(",") if name.strip()
    }
    skip_dbs = INTERNAL_DBS | excluded_dbs

    if excluded_dbs:
        print(f"Skipping databases: {', '.join(sorted(excluded_dbs))}\n")

    source = MongoClient(os.environ.get("SRC_URI", "mongodb://src-mongos:27017"))
    target = MongoClient(os.environ.get("TGT_URI", "mongodb://tgt-mongos:29017"))
    failures = compare_clusters(source, target, skip_dbs)

    if failures:
        print()
        for line in failures:
            print(line)
        print(f"\nFAILED: {len(failures)} mismatch(es)")
        return 1

    print("\nOK: source and target match")
    return 0


if __name__ == "__main__":
    sys.exit(main())
