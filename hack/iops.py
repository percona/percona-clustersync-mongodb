import pymongo
import time
import argparse
from bson import ObjectId, BSON
import signal
import threading

MONGO_URI = "mongodb://src-mongos:27017"
DB_NAME = "testdb"
COLLECTION_PREFIX = "testcol"
BATCH_SIZE = 100

DOC_SIZE_ESTIMATE_REGULAR = 200_000  # bytes (~200 KB)
DOC_SIZE_ESTIMATE_LIGHT = 100_000    # bytes (~100 KB)
DOC_SIZE_ESTIMATE_SMALL = 10_000     # bytes (~10 KB)
MAX_COLLECTION_BYTES = 1_000_000_000  # 1 GB

total_inserted = 0
start_time = time.time()
running = True
lock = threading.Lock()
threads = []

def generate_document(template="regular"):
    """Generate document based on template type: 'regular' (~200KB), 'light' (~100KB), or 'small' (~10KB)"""
    if template == "small":
        padding1_size = 5_000
        padding2_size = 5_000
    elif template == "light":
        padding1_size = 50_000
        padding2_size = 50_000
    else:  # regular
        padding1_size = 100_000
        padding2_size = 100_000

    return {
        "_id": ObjectId(),
        "int": 42,
        "float": 3.14159,
        "string": "x" * 100,
        "padding1": "a" * padding1_size,
        "padding2": "b" * padding2_size,
        "array": [1] * 40
    }

def get_bson_size(doc):
    return len(BSON.encode(doc))

def print_doc_sizes():
    regular_doc = generate_document("regular")
    light_doc = generate_document("light")
    small_doc = generate_document("small")
    regular_size = get_bson_size(regular_doc)
    light_size = get_bson_size(light_doc)
    small_size = get_bson_size(small_doc)
    print(f"[Size Info] Regular template size: {regular_size} bytes (~{regular_size / 1024:.2f} KB)")
    print(f"[Size Info] Light template size:   {light_size} bytes (~{light_size / 1024:.2f} KB)")
    print(f"[Size Info] Small template size:   {small_size} bytes (~{small_size / 1024:.2f} KB)")

def signal_handler(sig, frame):
    global running
    running = False
    print("\n[Interrupt received] Shutting down...")

def enable_sharding(client, db_name):
    """Enable sharding on the database."""
    admin_db = client.admin
    try:
        result = admin_db.command("enableSharding", db_name)
        print(f"[Sharding] Enabled sharding on database '{db_name}'")
    except Exception as e:
        if "already enabled" in str(e).lower() or "already sharded" in str(e).lower():
            print(f"[Sharding] Database '{db_name}' is already sharded")
        else:
            print(f"[Sharding] Warning: Could not enable sharding: {e}")

def shard_collection(client, db_name, collection_name, shard_key=None):
    """Shard a collection with the specified shard key"""
    if shard_key is None:
        shard_key = {"_id": "hashed"}
    admin_db = client.admin
    namespace = f"{db_name}.{collection_name}"
    try:
        result = admin_db.command("shardCollection", namespace, key=shard_key)
        print(f"[Sharding] Sharded collection '{namespace}' with key {shard_key}")
    except Exception as e:
        if "already sharded" in str(e).lower() or "already exists" in str(e).lower():
            print(f"[Sharding] Collection '{namespace}' is already sharded")
        else:
            print(f"[Sharding] Warning: Could not shard collection '{namespace}': {e}")

def writer_thread(client, collection_name, per_coll_ops, template_type):
    global total_inserted, start_time
    db = client[DB_NAME]
    collection = db[collection_name]
    batch_interval = BATCH_SIZE / per_coll_ops
    last_counter = 0
    next_log = time.time() + 5
    next_batch_time = time.time()

    if template_type == "small":
        doc_size = DOC_SIZE_ESTIMATE_SMALL
    elif template_type == "light":
        doc_size = DOC_SIZE_ESTIMATE_LIGHT
    else:
        doc_size = DOC_SIZE_ESTIMATE_REGULAR
    inserted_in_collection = 0

    while running:
        estimated_size = inserted_in_collection * doc_size
        if estimated_size >= MAX_COLLECTION_BYTES:
            print(f"[Info] Dropping '{collection_name}' (estimated size ~{estimated_size / 1024 ** 2:.2f} MB)")
            collection.drop()
            collection = db[collection_name]
            shard_collection(client, DB_NAME, collection_name)
            inserted_in_collection = 0

        batch = [generate_document(template_type) for _ in range(BATCH_SIZE)]
        collection.insert_many(batch)
        inserted_in_collection += BATCH_SIZE

        with lock:
            total_inserted += BATCH_SIZE

        now = time.time()
        if now >= next_log:
            with lock:
                elapsed = now - start_time
                interval_inserts = total_inserted - last_counter
                interval_iops = interval_inserts / 5
                avg_iops = total_inserted / elapsed
                print(f"[{elapsed:.1f}s] Inserts: {total_inserted} | Interval IOPS: {interval_iops:.2f} | Avg IOPS: {avg_iops:.2f}")
                last_counter = total_inserted
                next_log += 5

        next_batch_time += batch_interval
        sleep_time = next_batch_time - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)
        else:
            next_batch_time = time.time()

def main():
    global start_time
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops", type=int, default=100, help="Total target inserts per second")
    parser.add_argument("--collections", type=int, default=1, help="Number of collections")
    parser.add_argument("--light-template", action="store_true", help="Use light document template (~100 KB)")
    parser.add_argument("--small-template", action="store_true", help="Use small document template (~10 KB)")
    args = parser.parse_args()

    # Determine template type
    if args.small_template:
        template_type = "small"
        template_desc = "small template (~10 KB)"
    elif args.light_template:
        template_type = "light"
        template_desc = "light template (~100 KB)"
    else:
        template_type = "regular"
        template_desc = "regular template (~200 KB)"

    print_doc_sizes()
    print(f"Starting load: {args.ops} ops/sec across {args.collections} collections (batch size: {BATCH_SIZE}) on {DB_NAME}.*")
    print(f"[Info] Using {template_desc}")

    signal.signal(signal.SIGINT, signal_handler)
    per_coll_ops = args.ops / args.collections
    client = pymongo.MongoClient(MONGO_URI)
    enable_sharding(client, DB_NAME)
    for i in range(args.collections):
        coll_name = f"{COLLECTION_PREFIX}_{i}"
        shard_collection(client, DB_NAME, coll_name)

    start_time = time.time()
    for i in range(args.collections):
        coll_name = f"{COLLECTION_PREFIX}_{i}"
        t = threading.Thread(target=writer_thread, args=(client, coll_name, per_coll_ops, template_type))
        t.start()
        threads.append(t)

    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        pass

    elapsed = time.time() - start_time
    avg_iops = total_inserted / elapsed if elapsed > 0 else 0
    print(f"\n[Done] Inserted {total_inserted} documents in {elapsed:.2f} seconds.")
    print(f"[Summary] Avg IOPS: {avg_iops:.2f}")

if __name__ == "__main__":
    main()
