# INITIAL CLONE KNOWLEDGE

## OVERVIEW

Score: 8; distinct initial-copy domain. Owns collection sizing, segmentation, parallel reads/inserts, sharded presplitting, and recovery state.

## WHERE TO LOOK

| Task | Location | Notes |
|------|----------|-------|
| Change clone lifecycle | `clone.go` | Options, status, checkpoints, recovery, collection scheduling |
| Change data copy | `copy.go` | Segment reads, batching, insert workers, copied-byte accounting |
| Change shard presplitting | `presplit.go` | Chunk layout and target shard size balancing |
| Test lifecycle/defaults | `clone_test.go` | Unit behavior and option handling |
| Test copy pipeline | `copy_test.go` | Segments, batches, and write paths |
| Test presplitting | `presplit_test.go` | Shard assignment and boundary cases |
| Test live cloning | `clone_integration_test.go` | Integration-tagged MongoDB behavior |

## CONVENTIONS

- `Options` zero values mean auto/default: collection parallelism, read workers, insert workers, segment size, and read batch size.
- `Clone` reports immutable status snapshots while internal lifecycle fields remain under `lock`.
- `copiedSize` is atomic because concurrently copied collections report inserted-byte progress through their copy callbacks.
- Clone's `Catalog` interface embeds `catalog.BaseCatalog`; repl defines its own richer interface.
- Checkpoints persist estimated/copied bytes plus start/finish timestamps and errors.
- Recovery is valid only before the clone has been used.
- Target shard size estimates live for one clone run and guide presplit placement under a shared mutex.
- Any hashed shard-key field disables ranged presplitting; equal shard counts mirror sorted source/target pairing.
- Unequal shard counts place largest chunks on the lightest target; failed placement reconciles reservations to last-known owners.
- Collection workers run under an error group so one failure cancels the clone operation.

## ANTI-PATTERNS

- Do not reuse a `Clone` after start or recovery has populated its start timestamp.
- Do not update copied-byte progress through a non-atomic path.
- Do not bypass the namespace filter when enumerating collections.
- Do not add presplit behavior for unsharded targets.
- Do not leave failed presplit reservations as phantom shard load; unknown placement releases the reservation.
- Do not persist worker-local state that cannot be reconstructed from the clone checkpoint.
- Do not assume recovered size estimates were recalculated; current recovery restores checkpoint values.
