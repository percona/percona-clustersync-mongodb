#!/usr/bin/env python3

import pymongo

SRC_URI = "mongodb://adm:pass@src-mongos:27017"
TGT_URI = "mongodb://adm:pass@tgt-mongos:29017"

src = pymongo.MongoClient(SRC_URI)
tgt = pymongo.MongoClient(TGT_URI)


def drop_all_from_src_and_tgt():
    """Drop all databases and collections from source and target MongoDB."""
    for db in src.list_database_names():
        if db not in ("admin", "config", "local", "percona_mongolink"):
            src.drop_database(db)

    for db in tgt.list_database_names():
        if db not in ("admin", "config", "local", "percona_mongolink"):
            tgt.drop_database(db)


# Main ===========================================================================

drop_all_from_src_and_tgt()
print("Dropped all databases and collections from source and target MongoDB.")
