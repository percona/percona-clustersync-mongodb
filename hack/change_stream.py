#!/usr/bin/env python3
"""
PCSM Change Stream Watcher

Watches MongoDB change stream and prints events in a compact JSON format.
Useful for debugging and monitoring replication.

Usage:
    export MONGO_URI="mongodb://localhost:27017"
    hack/change_stream.py
    MONGO_URI="mongodb://rs00:30000" hack/change_stream.py --show-checkpoints

Options:
    -u, --uri               MongoDB connection string (default: $MONGO_URI)
    --show-checkpoints      Include PCSM checkpoint events (default: hidden)
"""

import argparse
from signal import SIG_DFL, SIGINT, signal

import bson.json_util as json
import pymongo
from mongo_uri import redact_uri, resolve_uri


def parse_args():
    parser = argparse.ArgumentParser(
        description="PCSM Change Stream Watcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    MONGO_URI="mongodb://localhost:27017" hack/change_stream.py
    MONGO_URI="mongodb://rs00:30000" hack/change_stream.py --show-checkpoints
        """,
    )
    parser.add_argument(
        "-u",
        "--uri",
        type=str,
        default=None,
        help="MongoDB connection string (default: $MONGO_URI)",
    )
    parser.add_argument(
        "--show-checkpoints",
        action="store_true",
        help="Include PCSM checkpoint events (default: hidden)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    signal(SIGINT, SIG_DFL)

    args = parse_args()
    uri = resolve_uri(args.uri)

    print()
    print("PCSM Change Stream Watcher")
    print("=" * 45)
    print(f"URI:                {redact_uri(uri)}")
    print(f"Show checkpoints:   {args.show_checkpoints}")
    print()

    m = pymongo.MongoClient(uri)
    for change in m.watch(show_expanded_events=True):
        del change["_id"]
        del change["wallTime"]

        ns = change["ns"]["db"]

        if not args.show_checkpoints and ns == "percona_clustersync_mongodb":
            continue

        if coll := change["ns"].get("coll"):
            ns += "." + coll
        ts = change["clusterTime"]
        doc = {
            "ts": f"{ts.time}.{ts.inc:03}",
            "ns": ns,
            "op": change["operationType"][0],
        }
        if val := change.get("updateDescription"):
            doc["updateDesc"] = val
        if val := change.get("operationDescription"):
            doc["desc"] = val
        if val := change.get("fullDocument"):
            doc["fullDoc"] = val
        if val := change.get("txnNumber"):
            doc["txn"] = val

        print(json.dumps(doc))
