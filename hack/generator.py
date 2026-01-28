#!/usr/bin/env python3
"""
PCSM Write Generator

Generates a continuous stream of inserts to MongoDB at a configurable rate.
Deterministic and reproducible - same config produces identical operation sequence.
Useful for measuring PCSM replication performance.

Usage:
    python hack/generator.py -r 100 -u "mongodb://localhost:27017"
    python hack/generator.py -r 500 -u "mongodb://localhost:27017" -d 120 --doc-size 10240
    python hack/generator.py -r 1000 -u "mongodb://localhost:27017" --txn-percent 20 --txn-size 5
    python hack/generator.py -r 1000 -u "mongodb://mongos:27017" --sharded --drop

Options:
    -r, --rate              Target inserts per second (required)
    -u, --uri               MongoDB connection string (required)
    -d, --duration          Duration in seconds (default: 60)
    --doc-size              Target document size in bytes (default: 5120)
    --txn-percent           Percentage of operations within transactions (0-100, default: 0)
    --txn-size              Number of operations per transaction (default: 5)
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
from datetime import UTC, datetime

import pymongo
import pymongo.errors
from bson import ObjectId

# Constants for deterministic generation
DEFAULT_SEED = 42
DEFAULT_DOC_SIZE = 5120  # 5KB per document
BASE_DOC_SIZE = 520  # Approximate size of document without padding
FIXED_TIMESTAMP = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
STATUSES = ["active", "pending", "archived"]
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
    python hack/generator.py --rate 100 --uri "mongodb://localhost:27017"
    python hack/generator.py -r 500 -u "mongodb://localhost:27017" -d 60
    python hack/generator.py -r 1000 -u "mongodb://mongos:27017" --sharded --drop
        """,
    )
    parser.add_argument("-r", "--rate", type=int, required=True, help="Target inserts per second")
    parser.add_argument("-u", "--uri", type=str, required=True, help="MongoDB connection string")
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        default=60,
        help="Duration in seconds (default: 60)",
    )
    parser.add_argument(
        "--doc-size",
        type=int,
        default=DEFAULT_DOC_SIZE,
        help=f"Target document size in bytes (default: {DEFAULT_DOC_SIZE})",
    )
    parser.add_argument(
        "--txn-percent",
        type=int,
        default=0,
        help="Percentage of operations within transactions (0-100, default: 0)",
    )
    parser.add_argument(
        "--txn-size",
        type=int,
        default=5,
        help="Number of operations per transaction (default: 5)",
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


def generate_document(idx: int, seed: int, padding_size: int) -> dict:
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
        "payload": "x" * padding_size,
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
    padding_size: int,
    txn_percent: int,
    txn_size: int,
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
    txn_count = 0
    batch_count = 0
    ops_in_cycle = 0  # tracks position in 100-op cycle for txn_percent
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

            # Determine if this batch should be transactional
            use_txn = txn_percent > 0 and (ops_in_cycle % 100) < txn_percent

            if use_txn:
                # Transaction: insert txn_size docs individually within transaction
                with client.start_session() as session:
                    with session.start_transaction():
                        for _ in range(txn_size):
                            doc = generate_document(doc_idx_offset + doc_idx, seed, padding_size)
                            doc_idx += 1
                            collection.insert_one(doc, session=session)

                local_inserted += txn_size
                ops_in_cycle = (ops_in_cycle + txn_size) % 100
                txn_count += 1

                with counter_lock:
                    shared_counter["total"] += txn_size
                    shared_counter["txns"] += 1

                # Rate limiting for transaction
                if drift < 0:
                    sleep_time = txn_size * time_per_doc
                    time.sleep(sleep_time)
            else:
                # Regular batch insert
                docs = []
                for _ in range(batch_size):
                    docs.append(generate_document(doc_idx_offset + doc_idx, seed, padding_size))
                    doc_idx += 1

                collection.insert_many(docs, ordered=False)
                local_inserted += len(docs)
                ops_in_cycle = (ops_in_cycle + len(docs)) % 100
                batch_count += 1

                with counter_lock:
                    shared_counter["total"] += len(docs)
                    shared_counter["batches"] += 1

                # Rate limiting with drift compensation
                if drift < 0:
                    sleep_time = batch_size * time_per_doc
                    time.sleep(sleep_time)

    except Exception as e:
        return {
            "inserted": local_inserted,
            "transactions": txn_count,
            "batches": batch_count,
            "error": str(e),
        }
    finally:
        client.close()

    return {
        "inserted": local_inserted,
        "transactions": txn_count,
        "batches": batch_count,
        "error": None,
    }


def run_writer(
    uri: str,
    collection_names: list,
    rate: int,
    duration: int,
    seed: int,
    padding_size: int,
    txn_percent: int,
    txn_size: int,
) -> dict:
    """Run the continuous insert stream with parallel workers."""
    global shutdown_requested

    # Shared counter for stats reporting
    shared_counter = {"total": 0, "txns": 0, "batches": 0}
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
                padding_size,
                txn_percent,
                txn_size,
                worker_id,
                shared_counter,
                counter_lock,
            )
            futures.append(future)

        # Main thread: print stats while workers are running
        last_stats_time = start_time
        last_stats_count = 0
        last_txns = 0

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
                    current_txns = shared_counter["txns"]
                    current_batches = shared_counter["batches"]

                elapsed_since_start = current_time - start_time
                interval_duration = current_time - last_stats_time
                interval_count = current_total - last_stats_count
                interval_rate = interval_count / interval_duration
                overall_rate = current_total / elapsed_since_start

                # Calculate transaction rate
                interval_txns = current_txns - last_txns
                txn_rate = interval_txns / interval_duration

                if txn_percent > 0:
                    print(
                        f"  [{int(elapsed_since_start)}s] "
                        f"inserted: {current_total:,}  "
                        f"rate: {interval_rate:.0f}/s (avg: {overall_rate:.0f}/s)  "
                        f"txn: {current_txns} ({txn_rate:.0f}/s) batch: {current_batches}"
                    )
                else:
                    print(
                        f"  [{int(elapsed_since_start)}s] "
                        f"inserted: {current_total:,}  "
                        f"rate: {interval_rate:.0f}/s (avg: {overall_rate:.0f}/s)"
                    )

                last_stats_time = current_time
                last_stats_count = current_total
                last_txns = current_txns

        # Collect results
        total_inserted = 0
        total_transactions = 0
        total_batches = 0
        errors = []
        for future in as_completed(futures):
            result = future.result()
            total_inserted += result["inserted"]
            total_transactions += result["transactions"]
            total_batches += result["batches"]
            if result["error"]:
                errors.append(result["error"])

        if errors:
            for err in errors:
                print(f"  Worker error: {err}")

    actual_duration = time.monotonic() - start_time
    return {
        "total_inserted": total_inserted,
        "transactions": total_transactions,
        "batches": total_batches,
        "duration": actual_duration,
        "actual_rate": total_inserted / actual_duration if actual_duration > 0 else 0,
    }


def main():
    args = parse_args()

    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Validate document size
    if args.doc_size < BASE_DOC_SIZE:
        print(f"ERROR: --doc-size must be at least {BASE_DOC_SIZE} bytes")
        sys.exit(1)

    # Validate transaction parameters
    if args.txn_percent < 0 or args.txn_percent > 100:
        print("ERROR: --txn-percent must be between 0 and 100")
        sys.exit(1)

    if args.txn_size < 1:
        print("ERROR: --txn-size must be at least 1")
        sys.exit(1)

    # Calculate padding size to reach target document size
    padding_size = args.doc_size - BASE_DOC_SIZE

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
    print(f"Document size:      {args.doc_size} bytes")
    print(f"Txn percent:        {args.txn_percent}%")
    print(f"Txn size:           {args.txn_size} ops")
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
    result = run_writer(
        args.uri,
        collection_names,
        args.rate,
        args.duration,
        args.seed,
        padding_size,
        args.txn_percent,
        args.txn_size,
    )

    # Print summary
    print()
    print("Summary")
    print("=" * 45)
    print(f"Duration:           {result['duration']:.1f} seconds")
    print(f"Total inserted:     {result['total_inserted']:,}")
    if args.txn_percent > 0:
        txn_rate = result["transactions"] / result["duration"] if result["duration"] > 0 else 0
        print(f"Transactions:       {result['transactions']:,} ({txn_rate:.1f}/sec)")
        print(f"Batches:            {result['batches']:,}")
    print(f"Actual rate:        {result['actual_rate']:.1f} ops/sec")
    print(f"Target rate:        {args.rate} ops/sec")
    print()

    client.close()


if __name__ == "__main__":
    main()
