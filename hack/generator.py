#!/usr/bin/env python3
"""
PCSM Write Generator

Generates a continuous stream of inserts to MongoDB at a configurable rate.
Deterministic and reproducible - same config produces identical operation sequence.
Useful for measuring PCSM replication performance.

Usage:
    python hack/generator.py -r 100 -u "mongodb://localhost:27017"
    python hack/generator.py -r 500 -u "mongodb://localhost:27017" -d 120 --databases 2 --collections-per-db 3
    python hack/generator.py -r 1000 -u "mongodb://mongos:27017" --databases 2 --collections-per-db 3 --sharded --drop

Options:
    -r, --rate              Target inserts per second (required)
    -u, --uri               MongoDB connection string (required)
    -d, --duration          Duration in seconds (default: 60)
    --databases             Number of databases (default: 1)
    --collections-per-db    Collections per database (default: 1)
    --seed                  Random seed for determinism (default: 42)
    --drop                  Drop collections before starting (default: false)
    --sharded               Create sharded collections (default: false)
"""

import argparse
import random
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pymongo
import pymongo.errors
from bson import ObjectId

# Constants for deterministic generation
DEFAULT_SEED = 42
DOC_SIZE_BYTES = 5120  # 5KB per document
FIXED_TIMESTAMP = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
STATUSES = ["active", "pending", "archived"]
PADDING_SIZE = 4600  # Adjusted to reach ~5KB total doc size
STATS_INTERVAL = 5  # Print stats every N seconds
NUM_WORKERS = 4  # Fixed number of parallel write workers


# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle interrupt signals for graceful shutdown."""
    global shutdown_requested
    shutdown_requested = True
    print("\nShutdown requested, finishing current batch...")


def parse_args():
    parser = argparse.ArgumentParser(
        description="PCSM Writer - Continuous insert stream generator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python hack/writer.py --rate 100 --uri "mongodb://localhost:27017"
    python hack/writer.py -r 500 -u "mongodb://localhost:27017" -d 60 --databases 2 --collections-per-db 3
    python hack/writer.py -r 1000 -u "mongodb://mongos:27017" --databases 2 --collections-per-db 3 --sharded --drop
        """,
    )
    parser.add_argument(
        "-r", "--rate", type=int, required=True, help="Target inserts per second"
    )
    parser.add_argument(
        "-u", "--uri", type=str, required=True, help="MongoDB connection string"
    )
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)",
    )
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
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for determinism (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop existing collections before starting",
    )
    parser.add_argument(
        "--sharded",
        action="store_true",
        help="Create sharded collections (requires connection to mongos)",
    )
    return parser.parse_args()


def generate_tags(idx: int, seed: int) -> list:
    """Generate deterministic tags based on index."""
    rng_local = random.Random(seed + idx)
    num_tags = (idx % 5) + 1  # 1-5 tags
    all_tags = [f"tag_{i}" for i in range(20)]
    return rng_local.sample(all_tags, num_tags)


def generate_document(idx: int, seed: int) -> dict:
    """Generate a single deterministic document."""
    return {
        "_id": ObjectId(),
        "idx": idx,
        "created_at": FIXED_TIMESTAMP,
        "category": f"cat_{idx % 100}",
        "status": STATUSES[idx % 3],
        "score": (idx * 17) % 1000,
        "priority": (idx * 13) % 5,
        "tags": generate_tags(idx, seed),
        "metadata": {
            "source": "writer",
            "version": 1,
            "seed": seed,
        },
        "payload": "x" * PADDING_SIZE,
    }


def drop_collections(
    client: pymongo.MongoClient, num_databases: int, collections_per_db: int
):
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


def get_collection_names(num_databases: int, collections_per_db: int) -> list:
    """Get list of (db_name, collection_name) tuples."""
    collections = []
    for db_idx in range(num_databases):
        db_name = f"db_{db_idx}"
        for coll_idx in range(collections_per_db):
            collection_name = f"collection_{coll_idx}"
            collections.append((db_name, collection_name))
    return collections


def worker_write(
    uri: str,
    collection_names: list,
    worker_rate: int,
    duration: int,
    seed: int,
    worker_id: int,
    shared_counter: dict,
    counter_lock: threading.Lock,
) -> dict:
    """Worker function to write documents at a target rate."""
    global shutdown_requested

    # Each worker creates its own MongoDB client
    client = pymongo.MongoClient(uri)

    # Build collection handles
    collections = []
    for db_name, coll_name in collection_names:
        db = client[db_name]
        collections.append((db_name, coll_name, db[coll_name]))

    total_collections = len(collections)
    local_inserted = 0
    # Use worker_id to offset document indices for determinism
    doc_idx_offset = worker_id * 1_000_000_000  # Large offset per worker
    doc_idx = 0

    start_time = time.monotonic()
    end_time = start_time + duration

    # Calculate batch size for this worker's rate
    # Use larger batches at higher rates for better throughput
    if worker_rate <= 100:
        batch_size = max(1, worker_rate // 10)
    else:
        batch_size = min(1000, worker_rate // 10)

    # Target time per document for drift compensation
    time_per_doc = 1.0 / worker_rate if worker_rate > 0 else 1.0

    try:
        while time.monotonic() < end_time and not shutdown_requested:
            # Calculate expected docs based on elapsed time (drift compensation)
            elapsed = time.monotonic() - start_time
            expected_docs = int(elapsed * worker_rate)
            drift = expected_docs - local_inserted

            # Round-robin collection selection within this worker
            coll_idx = local_inserted % total_collections
            _, _, collection = collections[coll_idx]

            # Generate and insert batch
            docs = []
            for _ in range(batch_size):
                docs.append(generate_document(doc_idx_offset + doc_idx, seed))
                doc_idx += 1

            collection.insert_many(docs, ordered=False)
            local_inserted += len(docs)

            # Update shared counter for stats
            with counter_lock:
                shared_counter["total"] += len(docs)

            # Rate limiting with drift compensation
            # Only sleep if we're ahead of schedule (negative drift means we're ahead)
            if drift < 0:
                # We're ahead, sleep to match target rate
                sleep_time = batch_size * time_per_doc
                time.sleep(sleep_time)
            # If drift > 0, we're behind - skip sleep to catch up

    except Exception as e:
        return {"inserted": local_inserted, "error": str(e)}
    finally:
        client.close()

    return {"inserted": local_inserted, "error": None}


def run_writer(
    uri: str,
    collection_names: list,
    rate: int,
    duration: int,
    seed: int,
) -> dict:
    """Run the continuous insert stream with parallel workers."""
    global shutdown_requested

    # Shared counter for stats reporting
    shared_counter = {"total": 0}
    counter_lock = threading.Lock()

    # Calculate per-worker rate
    worker_rate = rate // NUM_WORKERS

    print(f"  Workers: {NUM_WORKERS}, rate per worker: {worker_rate}/s")
    print()

    start_time = time.monotonic()

    # Launch workers
    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        futures = []
        for worker_id in range(NUM_WORKERS):
            future = executor.submit(
                worker_write,
                uri,
                collection_names,
                worker_rate,
                duration,
                seed,
                worker_id,
                shared_counter,
                counter_lock,
            )
            futures.append(future)

        # Main thread: print stats while workers are running
        last_stats_time = start_time
        last_stats_count = 0

        while True:
            # Check if all futures are done
            all_done = all(f.done() for f in futures)
            if all_done or shutdown_requested:
                break

            time.sleep(0.5)  # Check every 0.5s

            current_time = time.monotonic()
            if current_time - last_stats_time >= STATS_INTERVAL:
                with counter_lock:
                    current_total = shared_counter["total"]

                elapsed_since_start = current_time - start_time
                interval_count = current_total - last_stats_count
                interval_rate = interval_count / (current_time - last_stats_time)
                overall_rate = current_total / elapsed_since_start

                print(
                    f"  [{int(elapsed_since_start)}s] "
                    f"inserted: {current_total:,}  "
                    f"rate: {interval_rate:.0f}/s (avg: {overall_rate:.0f}/s)"
                )
                last_stats_time = current_time
                last_stats_count = current_total

        # Collect results
        total_inserted = 0
        errors = []
        for future in as_completed(futures):
            result = future.result()
            total_inserted += result["inserted"]
            if result["error"]:
                errors.append(result["error"])

        if errors:
            for err in errors:
                print(f"  Worker error: {err}")

    actual_duration = time.monotonic() - start_time
    return {
        "total_inserted": total_inserted,
        "duration": actual_duration,
        "actual_rate": total_inserted / actual_duration if actual_duration > 0 else 0,
    }


def main():
    args = parse_args()

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Set global seed for reproducibility
    random.seed(args.seed)

    total_collections = args.databases * args.collections_per_db

    # Print configuration
    print()
    print("PCSM Writer")
    print("=" * 45)
    print(f"Target rate:        {args.rate} ops/sec")
    print(f"URI:                {args.uri}")
    print(f"Duration:           {args.duration} seconds")
    print(f"Databases:          {args.databases}")
    print(f"Collections per DB: {args.collections_per_db}")
    print(f"Total collections:  {total_collections}")
    print(f"Workers:            {NUM_WORKERS}")
    print(f"Seed:               {args.seed}")
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
        drop_collections(client, args.databases, args.collections_per_db)
        print()

    # Setup sharded collections if requested
    if args.sharded:
        print("Setting up sharded collections...")
        setup_sharded_collections(client, args.databases, args.collections_per_db)
        print()

    # Get collection names (workers create their own handles)
    collection_names = get_collection_names(args.databases, args.collections_per_db)

    # Run the writer
    print("Writing...")
    result = run_writer(args.uri, collection_names, args.rate, args.duration, args.seed)

    # Print summary
    print()
    print("Summary")
    print("=" * 45)
    print(f"Duration:           {result['duration']:.1f} seconds")
    print(f"Total inserted:     {result['total_inserted']:,}")
    print(f"Actual rate:        {result['actual_rate']:.1f} ops/sec")
    print(f"Target rate:        {args.rate} ops/sec")
    print()

    client.close()


if __name__ == "__main__":
    main()
