# TARGET CATALOG KNOWLEDGE

## OVERVIEW

Score: 8; distinct target-schema domain. Owns collection/index metadata, DDL application, checkpoint state, and unsuccessful-index finalization.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change collection metadata | `catalog.go` | Namespaces, UUID map, create/drop/modify operations |
| Change index policy | `index.go` | Unsuccessful-index classification and finalize recovery |
| Test catalog state | `catalog_test.go` | Collection and checkpoint behavior |
| Test UUID handling | `catalog_uuid_test.go` | UUID-to-namespace mapping |
| Test index modification | `modify_index_test.go` | `collMod` and index changes |
| Test live index behavior | `index_integration_test.go` | Integration-tagged source/target cases |

## CONVENTIONS

- `BaseCatalog` is the shared interface consumed by clone and repl.
- `Catalog.Databases` is guarded by one `sync.RWMutex`.
- `LockWrite` deliberately takes the read lock: it freezes writers while checkpoint BSON reads the maps.
- `Checkpoint` assumes its caller already holds `LockWrite`; it must not recursively call `RLock`. Keep that lock until the returned maps are no longer being read, including BSON marshaling.
- MongoDB I/O occurs outside the catalog write lock; successful results are committed to memory under a short lock.
- Index outcomes are classified as `failed`, `incomplete`, or `inconsistent`.
- Finalization rechecks current source index state before recreating incomplete or inconsistent indexes.
- Timeseries collections are explicitly unsupported.

## ANTI-PATTERNS

- Never add another `RLock` inside `Checkpoint`; recursive read locking can deadlock when a writer is pending.
- Never hold the catalog write lock across MongoDB network I/O.
- Never mutate `Databases`, collection entries, UUIDs, or index entries without the catalog lock.
- Never collapse unsuccessful-index categories; API status exposes their distinct meanings.
- Never treat source probes and target recreation as atomic; source DDL is not fenced during finalize.
- Never create timeseries support by passing bucket namespaces through existing collection logic.
