
## PMM External Sources

Metrics Endpoint for PMM in a docker and PCSM on a local host:
```
host.docker.internal:2242/metrics
```


## MongoLink CLI

```shell
go install . && pml --source="mongodb://adm:pass@rs00:30000" --target="mongodb://adm:pass@rs10:30100" --reset-state --log-level="debug"
```


## PCSM Server

```shell
curl -H 'Content-Type: application/json' \
      -d '{}' \
      -X POST \
     localhost:2242/start

curl -H 'Content-Type: application/json' \
      -d '{}' \
      -X POST \
     localhost:2242/finalize

curl localhost:2242/status
```


## Poetry

```shell
# Run a specific test suite:
poetry run pytest -v -s tests/test_collections.py

# Run a specific test:
poetry run pytest -v -s tests/test_collections.py::test_rename
```


## oplog operations

```javascript
db.oplog.rs.find({ op: "c" }).sort({ wall: 1 }).limit(10) //find 10 op:c operations logs sorted by wall field
```
