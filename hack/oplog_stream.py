#!/usr/bin/env python3
"""
Stream entries from a MongoDB replica set oplog.

Usage:
    export MONGO_URI="mongodb://rs10:30100,rs11:30101,rs12:30102"
    hack/oplog_stream.py
"""

import argparse

import bson.json_util as json
import pymongo
from mongo_uri import resolve_uri


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream the MongoDB oplog")
    parser.add_argument(
        "-u",
        "--uri",
        help="MongoDB connection string (default: $MONGO_URI)",
    )
    args = parser.parse_args()

    m = pymongo.MongoClient(resolve_uri(args.uri), readPreference="primary")
    for change in m.local["oplog.rs"].find():
        del change["_id"]
        del change["wallTime"]
        print(json.dumps(change))


if __name__ == "__main__":
    main()
