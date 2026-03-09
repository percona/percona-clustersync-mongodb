#!/usr/bin/env python3

import bson.json_util as json
import pymongo

if __name__ == "__main__":
    MONGODB_URI = "mongodb://rs10:50100,rs11:50101,rs12:50102"
    m = pymongo.MongoClient(MONGODB_URI, readPreference="primary")
    for change in m.local["oplog.rs"].find():
        del change["_id"]
        del change["wallTime"]
        print(json.dumps(change))
