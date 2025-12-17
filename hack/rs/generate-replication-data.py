import os
import random
import multiprocessing
import pymongo
import base64
import time
from bson import ObjectId, BSON
from datetime import datetime

uri = "mongodb://rs00:30000"
# uri="mongodb://inel:m0sl1RoYDXkSLEse@cluster0.vrfly.mongodb.net/"


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


def generate_distributed_counts(total, chunks, variation=0.4):
    base = total // chunks
    counts = []
    for _ in range(chunks):
        delta = int(base * variation)
        count = random.randint(base - delta, base + delta)
        counts.append(count)
    diff = total - sum(counts)
    for i in range(abs(diff)):
        counts[i % chunks] += 1 if diff > 0 else -1
    return counts


def generate_entropy_pool(num_blocks=20, block_size=100_000):
    return [base64.b64encode(os.urandom(block_size)).decode("ascii") for _ in range(num_blocks)]


def get_template_doc(args):
    template_type, pool = args
    if template_type == "random" and pool:
        p1 = random.choice(pool)
        p2 = random.choice(pool)
        return {
            "_id": ObjectId(),
            "int": 42,
            "float": 3.14159,
            "string": base64.b64encode(os.urandom(100)).decode("ascii"),
            # "padding1": p1,
            # "padding2": p2,
            "array": list(range(40)),
        }
    else:
        return {
            "_id": ObjectId(),
            "int": 42,
            "float": 3.14159,
            "string": "x" * 100,
            # "padding1": "a" * 100_000,
            # "padding2": "b" * 100_000,
            "array": [1] * 40,
        }


def estimate_doc_size(template):
    return len(BSON.encode(template))


def insert_documents(collection, docs):
    try:
        collection.insert_many(docs, ordered=False, bypass_document_validation=True)
    except Exception as e:
        return str(e)
    return None


def worker(collection_name, count, template_type, pool, error_queue, db_name):
    try:
        client = pymongo.MongoClient(uri)
        db = client[db_name]
        collection = db[collection_name]
        batch_size = random.randint(10, 100)
        inserted = 0
        if template_type == "random":
            # while inserted < count:
            while True:
                # current_batch = min(batch_size, count - inserted)
                current_batch = batch_size
                with multiprocessing.Pool() as p:
                    args = [(template_type, pool)] * current_batch
                    docs = p.map(get_template_doc, args)

                error = insert_documents(collection, docs)
                if error:
                    error_queue.put((collection_name, error))
                    log(f"Error inserting documents into {collection_name}: {error}")
                    return

                log(f"Inserted {current_batch} documents into {collection_name}")

                inserted += current_batch
                batch_size = random.randint(10, 100)  # Randomize batch size for each iteration
                time.sleep(random.uniform(0.1, 1))

        else:
            # while inserted < count:
            while True:
                # current_batch = min(batch_size, count - inserted)
                current_batch = batch_size
                docs = [get_template_doc((template_type, pool)) for _ in range(current_batch)]

                error = insert_documents(collection, docs)
                if error:
                    error_queue.put((collection_name, error))
                    log(f"Error inserting documents into {collection_name}: {error}")
                    return

                inserted += current_batch
                batch_size = random.randint(10, 100)  # Randomize batch size for each iteration
                log(f"Inserted {inserted} documents into {collection_name}")

                time.sleep(random.uniform(0.1, 1))

    except Exception as e:
        error_queue.put((collection_name, str(e)))


def load_data():
    start_time = time.time()
    cpu_count = os.cpu_count() or 1
    calculated_threads = max(1, int(cpu_count * 0.75))
    max_threads = min(4, calculated_threads)
    total_collections = int(os.getenv("COLLECTIONS", 5))
    datasize_mb = int(os.getenv("DATASIZE", 1024))
    distribute = os.getenv("DISTRIBUTE", "false").lower() == "true"
    template_type = os.getenv("DOC_TEMPLATE", "compressible").lower()
    db_name = os.getenv("DBNAME", "test_db")

    pool = generate_entropy_pool() if template_type == "random" else None
    sample_doc = get_template_doc((template_type, pool))
    estimated_doc_size = estimate_doc_size(sample_doc)
    total_bytes = datasize_mb * 1024 * 1024 * 1024 * 1024  # to make it run for veeery long
    total_docs = total_bytes // estimated_doc_size

    log(
        f"Starting data generation: database: {db_name}, {total_collections} collections, ~{datasize_mb} MB"
    )
    log(f"Template: {template_type}, threads: {max_threads}, distribute: {distribute}")
    log(f"Estimated doc size: {estimated_doc_size} bytes")
    log(f"Total documents to insert: {total_docs}")

    if distribute:
        doc_counts = generate_distributed_counts(total_docs, total_collections, variation=0.4)
    else:
        base = total_docs // total_collections
        remainder = total_docs % total_collections
        doc_counts = [base + 1 if i < remainder else base for i in range(total_collections)]
    processes = []
    error_queue = multiprocessing.Queue()
    for i, count in enumerate(doc_counts):
        collection_name = f"collection{i}"
        p = multiprocessing.Process(
            target=worker, args=(collection_name, count, template_type, pool, error_queue, db_name)
        )
        processes.append(p)
        p.start()
        if len(processes) >= max_threads:
            for p in processes:
                p.join()
            processes = []
    for p in processes:
        p.join()
    errors = []
    while not error_queue.empty():
        errors.append(error_queue.get())
    if errors:
        for coll, err in errors:
            log(f"[{coll}] ERROR: {err}")
        raise RuntimeError("One or more insert processes failed")
    elapsed = time.time() - start_time
    log(f"Data generation complete in {elapsed:.2f} seconds")


if __name__ == "__main__":
    multiprocessing.set_start_method("fork")
    load_data()
