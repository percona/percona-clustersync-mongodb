#!/usr/bin/env python3
"""
PCSM Change Stream Watcher

Watches MongoDB change stream and prints events in a compact JSON format.
Useful for debugging and monitoring replication.

Usage:
    python hack/change_stream.py -u "mongodb://localhost:27017"
    python hack/change_stream.py --uri "mongodb://rs00:30000" --show-checkpoints

Options:
    -u, --uri               MongoDB connection string (required)
    --show-checkpoints      Include PCSM checkpoint events (default: hidden)
"""

import argparse
from signal import SIG_DFL, SIGINT, signal

import bson.json_util as json
import pymongo


def parse_args():
    parser = argparse.ArgumentParser(
        description="PCSM Change Stream Watcher",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python hack/change_stream.py -u "mongodb://localhost:27017"
    python hack/change_stream.py --uri "mongodb://rs00:30000" --show-checkpoints
        """,
    )
    parser.add_argument("-u", "--uri", type=str, required=True, help="MongoDB connection string")
    parser.add_argument(
        "--show-checkpoints",
        action="store_true",
        help="Include PCSM checkpoint events (default: hidden)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    signal(SIGINT, SIG_DFL)

    args = parse_args()

    print()
    print("PCSM Change Stream Watcher")
    print("=" * 45)
    print(f"URI:                {args.uri}")
    print(f"Show checkpoints:   {args.show_checkpoints}")
    print()

    m = pymongo.MongoClient(args.uri)
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
