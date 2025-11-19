#!/usr/bin/env python3
# poetry run python3 .dev/change_stream.py mongodb://adm:pass@rs00:30000

import sys
from signal import SIG_DFL, SIGINT, signal

import bson.json_util as json
import pymongo

MONGODB_URI = "mongodb://adm:pass@rs00:30000"

ignoreCheckpoints = True  # set to False to see all changes, including checkpoints

if __name__ == "__main__":
    signal(SIGINT, SIG_DFL)

    if len(sys.argv) > 1:
        url = sys.argv[1]
        print(f"Using MongoDB URI: {url}")
    else:
        url = MONGODB_URI

    m = pymongo.MongoClient(url)
    for change in m.watch(show_expanded_events=True):
        del change["_id"]
        del change["wallTime"]

        ns = change["ns"]["db"]
        
        if ignoreCheckpoints and ns == "percona_link_mongodb":
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
