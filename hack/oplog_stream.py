#!/usr/bin/env python3

import bson.json_util as json
import pymongo

if __name__ == "__main__":
    MONGODB_URI = "mongodb://adm:pass@rs10:30100,rs11:30101,rs12:30102"
    m = pymongo.MongoClient(MONGODB_URI, readPreference="primary")
    for change in m.local["oplog.rs"].find():
        del change["_id"]
        del change["wallTime"]
        print(json.dumps(change))
