import datetime
import random
from bson.decimal128 import Decimal128
import pymongo
from pymongo import MongoClient

MONGODB_URI = "mongodb://adm:pass@rs00:30000"
client = MongoClient(MONGODB_URI)
db = client["stress_test_db"]

collections_meta = [
    {"name": "regular_coll_float", "options": {}, "id_type": "float"},
    {"name": "regular_coll_decimal", "options": {}, "id_type": "decimal"},
]

batch_size = 5000
num_batches = 150
for meta in collections_meta:
    collection = db[meta["name"]]
    id_type = meta["id_type"]
    # nan_id = float("nan") if id_type == "float" else Decimal128("NaN")
    nan_id = Decimal128("NaN")

    # collection.insert_one({"_id": nan_id, "payload": "x" * 5000})

    for _ in range(num_batches):
        docs = []
        for _ in range(batch_size):
            if id_type == "float":
                _id = random.uniform(1e5, 1e10)
            else:
                _id = Decimal128(str(random.uniform(1e5, 1e10)))
            doc = {
                "_id": _id,
                "payload": "x" * 5000,
                "timestamp": datetime.datetime.now(datetime.timezone.utc),
                "rand": random.random(),
            }
            docs.append(doc)
        try:
            collection.insert_many(docs, ordered=False, bypass_document_validation=True)
        except pymongo.errors.BulkWriteError as e:
            print(f"Insert failed in {meta['name']}: {e.details}")#
