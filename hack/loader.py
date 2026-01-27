#!/usr/bin/env python3
"""
PCSM Source Loader

Loads a specified amount of data into MongoDB with deterministic,
reproducible document generation. Docker-friendly with throttled inserts.

Usage:
    python hack/loader.py --size 10 --uri "mongodb://localhost:27017"
    python hack/loader.py -s 5 -u "mongodb://localhost:27017" --databases 3 --collections-per-db 5 --drop
    python hack/loader.py -s 5 -u "mongodb://mongos:27017" --databases 2 --collections-per-db 3 --sharded

Options:
    -s, --size              Size in GB to load (required)
    -u, --uri               MongoDB connection string (required)
    --databases             Number of databases (default: 1)
    --collections-per-db    Collections per database (default: 1)
    --workers               Number of parallel workers (default: 4)
    --batch-size            Documents per insert batch (default: 200)
    --drop                  Drop collections before loading (default: false)
    --sharded               Create sharded collections (default: false)
"""

import argparse
import random
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pymongo
import pymongo.errors
from bson import ObjectId

# Constants for deterministic generation
SEED = 42
DOC_SIZE_BYTES = 5120  # 5KB per document
FIXED_TIMESTAMP = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
STATUSES = ["active", "pending", "archived"]
PADDING_SIZE = 4600  # Adjusted to reach ~5KB total doc size


def parse_args():
    parser = argparse.ArgumentParser(
        description="PCSM Source Loader",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python hack/loader.py --size 10 --uri "mongodb://src-mongos:27017"
    python hack/loader.py -s 5 -u "mongodb://src-mongos:27017" --databases 3 --collections-per-db 5 --drop
    python hack/loader.py -s 5 -u "mongodb://mongos:27017" --sharded --databases 2 --collections-per-db 3
        """,
    )
    parser.add_argument("-s", "--size", type=float, required=True, help="Size in GB to load")
    parser.add_argument("-u", "--uri", type=str, required=True, help="MongoDB connection string")
    parser.add_argument(
        "--databases",
        type=int,
        default=1,
        help="Number of databases to create (default: 1)",
    )
    parser.add_argument(
        "--collections-per-db",
        type=int,
        default=1,
        help="Number of collections per database (default: 1)",
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Documents per insert batch (default: 200)",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop existing collections before loading",
    )
    parser.add_argument(
        "--sharded",
        action="store_true",
        help="Create sharded collections (requires connection to mongos)",
    )
    return parser.parse_args()


def calculate_parameters(size_gb: float, num_databases: int, collections_per_db: int) -> dict:
    """Calculate deterministic parameters based on target size."""
    total_bytes = int(size_gb * 1024 * 1024 * 1024)
    total_docs = total_bytes // DOC_SIZE_BYTES
    total_collections = num_databases * collections_per_db

    # Distribute docs evenly, remainder goes to last collection
    base_docs_per_collection = total_docs // total_collections
    remainder = total_docs % total_collections

    # Build work items: list of (db_name, collection_name, num_docs)
    work_items = []
    idx = 0
    for db_idx in range(num_databases):
        db_name = f"db_{db_idx}"
        for coll_idx in range(collections_per_db):
            collection_name = f"collection_{coll_idx}"
            count = base_docs_per_collection
            # Add remainder to the last collection
            if idx == total_collections - 1:
                count += remainder
            work_items.append(
                {
                    "db_name": db_name,
                    "collection_name": collection_name,
                    "num_docs": count,
                }
            )
            idx += 1

    return {
        "total_bytes": total_bytes,
        "total_docs": total_docs,
        "num_databases": num_databases,
        "collections_per_db": collections_per_db,
        "total_collections": total_collections,
        "work_items": work_items,
    }


def generate_tags(idx: int, rng: random.Random) -> list:
    """Generate deterministic tags based on index."""
    # Use a seeded selection based on idx
    rng_local = random.Random(SEED + idx)
    num_tags = (idx % 5) + 1  # 1-5 tags
    all_tags = [f"tag_{i}" for i in range(20)]
    return rng_local.sample(all_tags, num_tags)


def generate_document(idx: int, batch_num: int, worker_id: int, rng: random.Random) -> dict:
    """Generate a single deterministic document."""
    return {
        "_id": ObjectId(),
        "idx": idx,
        "created_at": FIXED_TIMESTAMP,
        "category": f"cat_{idx % 100}",
        "status": STATUSES[idx % 3],
        "score": (idx * 17) % 1000,
        "priority": (idx * 13) % 5,
        "tags": generate_tags(idx, rng),
        "metadata": {
            "source": "loader",
            "version": 1,
            "batch": batch_num,
            "worker": worker_id,
        },
        "payload": "x" * PADDING_SIZE,
    }


def worker_load_collection(
    uri: str,
    db_name: str,
    collection_name: str,
    start_idx: int,
    num_docs: int,
    batch_size: int,
    worker_id: int,
) -> dict:
    """Worker function to load a single collection."""
    full_name = f"{db_name}.{collection_name}"
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    rng = random.Random(SEED)
    inserted = 0
    batch_num = 0
    last_report = 0
    report_interval = 10000

    try:
        while inserted < num_docs:
            current_batch_size = min(batch_size, num_docs - inserted)
            docs = []

            for i in range(current_batch_size):
                doc_idx = start_idx + inserted + i
                docs.append(generate_document(doc_idx, batch_num, worker_id, rng))

            collection.insert_many(docs, ordered=False)
            inserted += current_batch_size
            batch_num += 1

            # Progress reporting
            if inserted - last_report >= report_interval or inserted == num_docs:
                pct = (inserted / num_docs) * 100
                print(f"  [{full_name}] {inserted:,} / {num_docs:,} ({pct:.1f}%)")
                last_report = inserted

    except Exception as e:
        return {
            "db_name": db_name,
            "collection": collection_name,
            "full_name": full_name,
            "success": False,
            "inserted": inserted,
            "error": str(e),
        }
    finally:
        client.close()

    return {
        "db_name": db_name,
        "collection": collection_name,
        "full_name": full_name,
        "success": True,
        "inserted": inserted,
        "error": None,
    }


def drop_collections(client: pymongo.MongoClient, num_databases: int, collections_per_db: int):
    """Drop existing collections across all databases."""
    for db_idx in range(num_databases):
        db_name = f"db_{db_idx}"
        db = client[db_name]
        for coll_idx in range(collections_per_db):
            collection_name = f"collection_{coll_idx}"
            if collection_name in db.list_collection_names():
                db.drop_collection(collection_name)
                print(f"  Dropped: {db_name}.{collection_name}")


def setup_sharded_collections(
    client: pymongo.MongoClient, num_databases: int, collections_per_db: int
):
    """Enable sharding and create sharded collections."""
    admin = client.admin
    for db_idx in range(num_databases):
        db_name = f"db_{db_idx}"
        # Enable sharding on the database
        try:
            admin.command("enableSharding", db_name)
            print(f"  Enabled sharding: {db_name}")
        except pymongo.errors.OperationFailure as e:
            # Database may already be sharded
            if "already enabled" not in str(e).lower():
                raise

        for coll_idx in range(collections_per_db):
            collection_name = f"collection_{coll_idx}"
            namespace = f"{db_name}.{collection_name}"
            try:
                admin.command("shardCollection", namespace, key={"_id": "hashed"})
                print(f"  Sharded: {namespace}")
            except pymongo.errors.OperationFailure as e:
                # Collection may already be sharded
                if "already sharded" not in str(e).lower():
                    raise


def get_db_stats(client: pymongo.MongoClient, db_name: str) -> dict:
    """Get database statistics for verification."""
    db = client[db_name]
    stats = db.command("dbStats")
    return {
        "db_name": db_name,
        "collections": stats.get("collections", 0),
        "documents": stats.get("objects", 0),
        "data_size": stats.get("dataSize", 0),
        "storage_size": stats.get("storageSize", 0),
        "indexes": stats.get("indexes", 0),
    }


def get_all_db_stats(client: pymongo.MongoClient, num_databases: int) -> list:
    """Get statistics for all databases."""
    stats = []
    for db_idx in range(num_databases):
        db_name = f"db_{db_idx}"
        stats.append(get_db_stats(client, db_name))
    return stats


def format_bytes(size_bytes: float) -> str:
    """Format bytes to human readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def main():
    args = parse_args()

    # Set global seed for reproducibility
    random.seed(SEED)

    # Calculate parameters
    params = calculate_parameters(args.size, args.databases, args.collections_per_db)

    # Print configuration
    print()
    print("PCSM Source Loader")
    print("=" * 45)
    print(f"Target size:        {args.size} GB")
    print(f"URI:                {args.uri}")
    print(f"Databases:          {params['num_databases']}")
    print(f"Collections per DB: {params['collections_per_db']}")
    print(f"Total collections:  {params['total_collections']}")
    print(f"Total docs:         {params['total_docs']:,}")
    print(f"Doc size:           {DOC_SIZE_BYTES:,} bytes")
    print(f"Workers:            {args.workers}")
    print(f"Batch size:         {args.batch_size}")
    print(f"Sharded:            {args.sharded}")
    print()

    # Connect to MongoDB
    try:
        client = pymongo.MongoClient(args.uri, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        print(f"ERROR: Failed to connect to MongoDB: {e}")
        sys.exit(1)

    # Drop collections if requested
    if args.drop:
        print("Dropping existing collections...")
        drop_collections(client, params["num_databases"], params["collections_per_db"])
        print()

    # Setup sharded collections if requested
    if args.sharded:
        print("Setting up sharded collections...")
        setup_sharded_collections(client, params["num_databases"], params["collections_per_db"])
        print()

    # Prepare work items with cumulative index for determinism
    work_items = params["work_items"]
    cumulative_idx = 0
    for work in work_items:
        work["start_idx"] = cumulative_idx
        cumulative_idx += work["num_docs"]

    # Load data with thread pool
    print("Loading data...")
    start_time = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {}
        for i, work in enumerate(work_items):
            future = executor.submit(
                worker_load_collection,
                args.uri,
                work["db_name"],
                work["collection_name"],
                work["start_idx"],
                work["num_docs"],
                args.batch_size,
                i,
            )
            futures[future] = f"{work['db_name']}.{work['collection_name']}"

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            if result["success"]:
                print(f"  [{result['full_name']}] Complete: {result['inserted']:,} docs")
            else:
                print(f"  [{result['full_name']}] FAILED: {result['error']}")

    elapsed = time.time() - start_time

    # Check for failures
    failures = [r for r in results if not r["success"]]
    if failures:
        print()
        print("ERRORS:")
        for f in failures:
            print(f"  {f['full_name']}: {f['error']}")
        sys.exit(1)

    # Verification
    print()
    print("Verification")
    print("=" * 45)

    all_stats = get_all_db_stats(client, params["num_databases"])
    total_collections = 0
    total_documents = 0
    total_data_size = 0
    total_storage_size = 0

    for stats in all_stats:
        print(f"  {stats['db_name']}:")
        print(f"    Collections:   {stats['collections']}")
        print(f"    Documents:     {stats['documents']:,}")
        print(f"    Data size:     {format_bytes(stats['data_size'])}")
        total_collections += stats["collections"]
        total_documents += stats["documents"]
        total_data_size += stats["data_size"]
        total_storage_size += stats["storage_size"]

    print("  " + "-" * 43)
    print("  Total:")
    print(f"    Databases:     {params['num_databases']}")
    print(f"    Collections:   {total_collections}")
    print(f"    Documents:     {total_documents:,}")
    print(f"    Data size:     {format_bytes(total_data_size)}")
    print(f"    Storage size:  {format_bytes(total_storage_size)} (with compression)")
    print()

    # Summary
    total_inserted = sum(r["inserted"] for r in results)
    throughput = (total_inserted * DOC_SIZE_BYTES) / elapsed / (1024 * 1024)
    print(f"Done in {elapsed:.1f} seconds ({throughput:.1f} MB/s)")
    print()

    client.close()


if __name__ == "__main__":
    main()
