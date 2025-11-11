
## PMM External Sources
Metrics Endpoint for PMM in a docker and PML on a local host:
```
host.docker.internal:2242/metrics
```


## Poetry

```shell
# Run a specific test suite:
poetry run pytest -v -s tests/test_collections.py

# Run a specific test:
poetry run pytest -v -s tests/test_collections.py::test_rename
```


## MongoLink Server
```shell
curl -H 'Content-Type: application/json' \
      -d '{}' \
      -X POST \
     localhost:2242/finalize

curl -H 'Content-Type: application/json' \
      -d '{}' \
      -X POST \
     localhost:2242/start

curl localhost:2242/status


```
## MongoLink CLI

```shell
go install . && pml --source="mongodb://adm:pass@rs00:30000" --target="mongodb://adm:pass@rs10:30100" --reset-state --log-level="debug"
```


## Basic CRUD
```javascript
show dbs
use {db}
show collections

db.createCollection("inel_col", {});

db.inel_coll.insertOne({})
db.inel_coll.insertOne({"my-field":22});
db.inel_coll.insertOne({"my-field":22, "my-field-oh":"what"});
db.inel_coll.insertOne({"my-field":324, "my-field-oh":"o maan"});

db.inel_col.find()
db.inel_col.find().limit(10)

db.inel_col.createIndex( { "my-field": "hashed" } ); // hashed index
db.inel_col.getIndexes()
```


## oplog operations
```javascript
db.oplog.rs.find({ op: "c" }).sort({ wall: 1 }).limit(10) //find 10 op:c operations logs sorted by wall field
```
