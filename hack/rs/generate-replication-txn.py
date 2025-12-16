import time
import random
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
import threading

# MongoDB connection
uri = "mongodb://rs00:30000"
client = MongoClient(uri)  # Ensure you have a replica set

# Databases and collections
db1 = client["database1"]
db2 = client["database2"]

collection1 = db1["collection1"]
collection2 = db2["collection2"]
collection3 = db2["collection3"]

def generate_transaction():
    return {
        "amount": round(random.uniform(10, 1000), 2),
        "timestamp": datetime.now(),
        "description": f"Transaction {random.randint(1000, 9999)}"
    }

def transaction_worker():
    print("Transaction worker thread started.")
    while True:
        txn = generate_transaction()
        with client.start_session() as session:
            try:
                with session.start_transaction():
                    # Insert into collection1
                    result1 = collection1.insert_one(txn, session=session)
                    inserted_id = result1.inserted_id

                    # Insert into collection2
                    collection2.insert_one(txn, session=session)

                    # Insert into collection3
                    collection3.insert_one(txn, session=session)

                    # Update the document just inserted in collection1
                    collection1.update_one(
                        {"_id": inserted_id},
                        {"$set": {"status": "updated"}},
                        session=session
                    )
                print(f"Inserted and updated transaction: {txn}")
            except PyMongoError as e:
                print(f"Transaction aborted: {e}")
        time.sleep(1)

threads = []
for _ in range(4):
    print("Starting transaction worker thread...")
    t = threading.Thread(target=transaction_worker, daemon=True)
    t.start()
    threads.append(t)
    print("Transaction worker thread started.")

# Keep the main thread alive
for t in threads:
    t.join()
