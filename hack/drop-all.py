#!/usr/bin/env python3
"""
Drop all user databases from MongoDB clusters.

Drops all databases except system databases (admin, config, local, percona_mongolink).

Usage:
    python hack/drop-all.py           # Drop from both source and target
    python hack/drop-all.py -s        # Drop from source only
    python hack/drop-all.py -t        # Drop from target only
    python hack/drop-all.py -s -t     # Drop from both source and target

Environment variables:
    SRC_URI  MongoDB connection string for source (default: mongodb://src-mongos:27017)
    TGT_URI  MongoDB connection string for target (default: mongodb://tgt-mongos:29017)
"""

import argparse
import os

import pymongo

SRC_URI = os.environ.get("SRC_URI", "mongodb://src-mongos:27017")
TGT_URI = os.environ.get("TGT_URI", "mongodb://tgt-mongos:29017")

src = pymongo.MongoClient(SRC_URI)
tgt = pymongo.MongoClient(TGT_URI)


def drop_from_source():
    """Drop all databases from source MongoDB."""
    for db in src.list_database_names():
        if db not in ("admin", "config", "local", "percona_mongolink"):
            src.drop_database(db)


def drop_from_target():
    """Drop all databases from target MongoDB."""
    for db in tgt.list_database_names():
        if db not in ("admin", "config", "local", "percona_mongolink"):
            tgt.drop_database(db)


# Main ===========================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Drop databases from MongoDB clusters")
    parser.add_argument("-s", "--source", action="store_true", help="Drop from source cluster")
    parser.add_argument("-t", "--target", action="store_true", help="Drop from target cluster")
    args = parser.parse_args()

    # If neither flag set OR both flags set → drop from both
    drop_source = args.source or not args.target
    drop_target = args.target or not args.source

    if drop_source:
        drop_from_source()
    if drop_target:
        drop_from_target()

    # Build output message
    if drop_source and drop_target:
        print("Dropped all databases from source and target MongoDB.")
    elif drop_source:
        print("Dropped all databases from source MongoDB.")
    else:
        print("Dropped all databases from target MongoDB.")
