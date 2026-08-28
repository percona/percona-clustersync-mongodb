#!/usr/bin/env python3
"""
MongoDB Write Operations Monitor

Monitors write operations per second on MongoDB cluster.
Works with both replica sets and sharded clusters (mongos).

Usage:
    export MONGO_URI="mongodb://localhost:27017"
    hack/monitor_writes.py
    hack/monitor_writes.py --interval 5

Options:
    -u, --uri       MongoDB connection string (default: $MONGO_URI)
    --interval      Sampling interval in seconds (default: 1)
"""

import argparse
import sys
import time

import pymongo
from mongo_uri import redact_uri, resolve_uri


def parse_args():
    parser = argparse.ArgumentParser(
        description="MongoDB Write Operations Monitor",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    MONGO_URI="mongodb://localhost:27017" hack/monitor_writes.py
    MONGO_URI="mongodb://mongos:27017" hack/monitor_writes.py --interval 5
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
        "--interval",
        type=int,
        default=1,
        help="Sampling interval in seconds (default: 1)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    uri = resolve_uri(args.uri)

    try:
        client = pymongo.MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        print(f"ERROR: Failed to connect to MongoDB: {e}")
        sys.exit(1)

    print(f"Monitoring writes on {redact_uri(uri)} (Ctrl+C to stop)")
    print()

    prev = client.admin.command("serverStatus")["opcounters"]

    try:
        while True:
            time.sleep(args.interval)
            curr = client.admin.command("serverStatus")["opcounters"]

            inserts = (curr["insert"] - prev["insert"]) // args.interval
            updates = (curr["update"] - prev["update"]) // args.interval
            deletes = (curr["delete"] - prev["delete"]) // args.interval
            total = inserts + updates + deletes

            print(f"Writes/sec: {total} (ins: {inserts}, upd: {updates}, del: {deletes})")

            prev = curr
    except KeyboardInterrupt:
        print("\n\nStopped.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
