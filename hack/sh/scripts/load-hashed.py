import os
import random
import multiprocessing
import pymongo
import base64
import time
from bson import ObjectId, BSON
from datetime import datetime

uri="mongodb://src-mongos:27017"
# uri="mongodb://inel:m0sl1RoYDXkSLEse@cluster0.vrfly.mongodb.net/"

def create_sharded_collection(db, collection_name, shard_key):
    admin_db = client.admin
    # Enable sharding on the database
    admin_db.command("enableSharding", db.name)
    # Shard the collection on the specified key
    admin_db.command(
        "shardCollection",
        f"{db.name}.{collection_name}",
        key={shard_key: "hashed"}
    )

def insert_documents(collection, num_docs, shard_key):
    for _ in range(num_docs):
        doc = {
            shard_key: random.randint(1, 1000000),
            "data": base64.b64encode(os.urandom(32)).decode(),
            "created_at": datetime.utcnow()
        }
        collection.insert_one(doc)

def worker(args):
    db_name, collection_name, shard_key, num_docs = args
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]
    insert_documents(collection, num_docs, shard_key)

if __name__ == "__main__":
    client = pymongo.MongoClient(uri)
    db_name = "testdb"
    collections = [
        ("coll_hash1", "shard_key1"),
        ("coll_hash2", "shard_key2"),
        # ("coll_hash3", "shard_key3"),
    ]
    num_docs_per_collection = 1000

    # Create sharded collections
    db = client[db_name]
    for collection_name, shard_key in collections:
        if collection_name not in db.list_collection_names():
            create_sharded_collection(db, collection_name, shard_key)

    # Insert documents in parallel
    args_list = [
        (db_name, collection_name, shard_key, num_docs_per_collection)
        for collection_name, shard_key in collections
    ]
    with multiprocessing.Pool(processes=len(collections)) as pool:
        pool.map(worker, args_list)
