# Percona ClusterSync for MongoDB

[![Go Report Card](https://goreportcard.com/badge/github.com/percona/percona-clustersync-mongodb)](https://goreportcard.com/report/github.com/percona/percona-clustersync-mongodb) [![CLA assistant](https://cla-assistant.percona.com/readme/badge/percona/percona-clustersync-mongodb)](https://cla-assistant.percona.com/percona/percona-clustersync-mongodb)

Percona ClusterSync for MongoDB (PCSM) is a tool for cloning and replicating data between MongoDB clusters. It supports both replica sets and sharded clusters, handling initial data cloning followed by continuous change replication.

For more information about PCSM and how to use it, see [Percona ClusterSync for MongoDB documentation](https://docs.percona.com/percona-clustersync-for-mongodb/).

Percona ClusterSync for MongoDB includes the following **Features**:

- Clone data from source to target MongoDB cluster
- Real-time change replication via [MongoDB Change Streams](https://www.mongodb.com/docs/manual/changeStreams/)
- Support for both replica sets and sharded clusters
- Namespace filtering (include/exclude databases and collections)
- Automatic index management on target cluster
- CLI tool as well as HTTP API

## Installation

You can install Percona ClusterSync for MongoDB in the following ways:

- from Percona repository (recommended)
- build from source code

Find the installation instructions in the [official documentation](https://docs.percona.com/percona-clustersync-for-mongodb/installation.html).

## API

PCSM is a CLI tool, but also exposes HTTP API as well.

For reference see [PCSM commands](https://docs.percona.com/percona-clustersync-for-mongodb/plm-commands.html) and [HTTP API](https://docs.percona.com/percona-clustersync-for-mongodb/api.html) docs.

For final pre-production confidence workflows (CI gates, old-vs-new reproducibility, staging checklist, and alerting), see [Replication Validation Guide](REPLICATION_VALIDATION.md).

## Clone And Replication Safety Notes

- Clone worker auto-defaults are intentionally bounded for safer live migrations:
  - `NumReadWorkers` auto-default: `max(runtime.NumCPU()/4, 1)`
  - `NumInsertWorkers` auto-default: `min(max(runtime.NumCPU(), 2), 16)`
- These are defaults only. You can still tune worker counts explicitly using CLI flags/config for your environment.
- Replication update batching includes safeguards to split large update payloads (including array-path pipeline updates) into ordered follow-up operations, reducing risk of MongoDB BufBuilder/AST overflow failures on large oplog events.
- Prometheus observability is available for chunking behavior:
  - `percona_clustersync_mongodb_repl_update_chunking_triggered_total`
  - `percona_clustersync_mongodb_repl_update_follow_up_ops_total{type=standard|pipeline}`
  - `percona_clustersync_mongodb_repl_update_chunk_limit_hits_total{target=array_pipeline|non_array_set,reason=bytes|stages}`
  - `percona_clustersync_mongodb_repl_update_follow_up_overflow_total{action=fail|warn}`
  - `percona_clustersync_mongodb_repl_update_follow_up_per_event`
  - `percona_clustersync_mongodb_repl_update_array_chunks_per_event`
  - `percona_clustersync_mongodb_repl_update_array_max_stages_per_chunk`

### Optional hard safety valve for pathological events

You can optionally enforce a maximum number of generated follow-up update operations per event:

- `--repl-max-follow-up-ops-per-event` / `PCSM_REPL_MAX_FOLLOW_UP_OPS_PER_EVENT`
  - `0` (default): disabled
  - `>0`: enable limit
- `--repl-follow-up-overflow-action` / `PCSM_REPL_FOLLOW_UP_OVERFLOW_ACTION`
  - `fail` (default when set/invalid): fail fast if limit is exceeded
  - `warn`: log and continue

Recommended production mode for strict safety is `fail`.

### Suggested alerting

For large live migrations, monitor and alert on:

- sustained growth in `repl_update_chunk_limit_hits_total` (bytes/stages)
- high percentiles of `repl_update_follow_up_per_event`
- sudden spikes in `repl_update_array_chunks_per_event`
- any non-zero rate of `repl_update_follow_up_overflow_total{action=\"fail\"}`

## Submit Bug Report / Feature Request

If you find a bug in Percona ClusterSync for MongoDB, submit a report to the project's [JIRA issue tracker](https://jira.percona.com/projects/PCSM).

As a general rule of thumb, please try to create bug reports that are:

- Reproducible. Include steps to reproduce the problem.
- Specific. Include as much detail as possible: which version, what environment, etc.
- Unique. Do not duplicate existing tickets.
- Scoped to a Single Bug. One bug per report.

When submitting a bug report or a feature, please attach the following information:

- The output of the `pcsm status` command
- The output of the `pcsm logs` command

## Licensing

Percona is dedicated to **keeping open source open**. Whenever possible, we strive to include permissive licensing for both our software and documentation. For this project, we are using the Apache License 2.0 license.

## How to get involved

We encourage contributions and are always looking for new members who are as dedicated to serving the community as we are.

The [Contributing Guide](CONTRIBUTING.md) contains the guidelines for contributing.

## Contact

You can reach us:

- on [Percona ClusterSync for MongoDB Community Forum](https://forums.percona.com/c/mongodb/percona-clustersync-mongodb-pcsm/87)
- or [Contact Form](https://www.percona.com/about/contact)
