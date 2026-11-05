# Contributing guide

Welcome to Percona ClusterSync for MongoDB!

1. [Prerequisites](#prerequisites)
2. [Submitting a pull request](#submitting-a-pull-request)
3. [Building Percona ClusterSync for MongoDB](#building-percona-clustersync-for-mongodb)
4. [Running PCSM locally](#running-pcsm-locally)
5. [Running tests locally](#running-tests-locally)
6. [Contributing to documentation](#contributing-to-documentation)

We're glad that you would like to become a Percona community member and participate in keeping open source open.

Percona ClusterSync for MongoDB (PCSM) is a tool for cloning and replicating data between MongoDB clusters. It supports both replica sets and sharded clusters, handling initial data cloning followed by continuous change replication.

You can contribute in one of the following ways:

1. Reach us on our [Forums](https://forums.percona.com/c/mongodb/percona-clustersync-for-mongodb).
2. [Submit a bug report or a feature request](https://jira.percona.com/projects/PCSM)
3. Submit a pull request (PR) with the code patch
4. Contribute to documentation

## Prerequisites

Before submitting code contributions, we ask you to complete the following prerequisites.

### 1. Sign the CLA

Before you can contribute, we kindly ask you to sign our [Contributor License Agreement](https://cla-assistant.percona.com/percona/percona-clustersync-mongodb) (CLA). You can do this in one click using your GitHub account.

**Note**: You can sign it later, when submitting your first pull request. The CLA assistant validates the PR and asks you to sign the CLA to proceed.

### 2. Code of Conduct

Please make sure to read and agree to our [Code of Conduct](https://github.com/percona/community/blob/main/content/contribute/coc.md).

## Submitting a pull request

All bug reports, enhancements and feature requests are tracked in [Jira issue tracker](https://jira.percona.com/projects/PCSM). Though not mandatory, we encourage you to first check for a bug report among Jira issues and in the PR list: perhaps the bug has already been addressed.

For feature requests and enhancements, we do ask you to create a Jira issue, describe your idea and discuss the design with us. This way we align your ideas with our vision for the product development.

If the bug hasn't been reported / addressed, or we've agreed on the enhancement implementation with you, do the following:

1. [Fork](https://docs.github.com/en/github/getting-started-with-github/fork-a-repo) this repository
2. Clone this repository on your machine
3. Create a separate branch for your changes. If you work on a Jira issue, please include the issue number in the branch name so it reads as `pcsm-XXX-my-branch`. This makes it easier to track your contribution.
4. Make your changes. Please follow the guidelines outlined in the [Go Code Review Comments](https://github.com/golang/go/wiki/CodeReviewComments) to improve code readability.
5. Test your changes locally. See the [Running tests locally](#running-tests-locally) section for more information
6. Commit the changes. Add the Jira issue number at the beginning of your message subject so that it reads as `PCSM-XXX - My subject`. The [commit message guidelines](https://gist.github.com/robertpainsi/b632364184e70900af4ab688decf6f53) will help you with writing great commit messages
7. Open a PR to Percona
8. Our team will review your code and if everything is correct, will merge it. Otherwise, we will contact you for additional information or with the request to make changes.

## Building Percona ClusterSync for MongoDB

To build Percona ClusterSync for MongoDB from source code, you require the following:

- Go 1.27.0. See
  [Installing and setting up Go tools](https://golang.org/doc/install).
- make

To build the project, run the following commands:

```sh
git clone https://github.com/<your_name>/percona-clustersync-mongodb
cd percona-clustersync-mongodb
make build
```

After `make` completes, you can find the `pcsm` binary in the `./bin` directory:

```sh
./bin/pcsm version
```

By running `pcsm version`, you can verify if Percona ClusterSync for MongoDB has been built correctly and is ready for use.

For development with race detection and debugging enabled, use:

```sh
make test-build
```

## Running PCSM locally

Run commands from the repository root. Use one topology at a time: replica set
and sharded environments share host ports. Stop all PCSM processes and run
`./hack/cleanup.sh` before switching topology. Cleanup removes the disposable
cluster data.

### 1. Deploy MongoDB clusters

Choose sharded source and target clusters:

```sh
MONGO_VERSION="8.0.29-13" ./hack/sh/run.sh
```

Or choose replica sets:

```sh
MONGO_VERSION="8.0.29-13" ./hack/rs/run.sh
```

| Variable | Default | Purpose |
| --- | --- | --- |
| `MONGO_VERSION` | `8.0.29-13` | Fallback image for both clusters and Go testcontainers |
| `SRC_MONGO_VERSION` | unset | Source image override |
| `TGT_MONGO_VERSION` | unset | Target image override |
| `SRC_SHARDS` | `2` | Source shard count, up to 3 |
| `TGT_SHARDS` | `2` | Target shard count, up to 3 |

Per-side image precedence is side override, `MONGO_VERSION`, then the Compose
default. Runtime version compatibility compares major versions only: the source
major must not exceed the target major. Acceptance outside the CI matrix is not
validation.

### 2. Update /etc/hosts

Add the following entries to your `/etc/hosts` file to resolve the container hostnames:

```
# pcsm test clusters
127.0.0.1 rs00 rs01 rs02 rs10 rs11 rs12
127.0.0.1 src-rs00 src-rs10 src-rs20 src-cfg0 src-mongos
127.0.0.1 tgt-rs00 tgt-rs10 tgt-rs20 tgt-cfg0 tgt-mongos
```

### 3. Check cluster health

For sharded clusters:

```sh
docker exec src-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
docker exec tgt-mongos mongosh --port 27017 --quiet --eval "db.adminCommand('ping')"
```

For replica sets:

```sh
docker exec rs00 mongosh --port 30000 --quiet --eval "rs.status().ok"
docker exec rs10 mongosh --port 30100 --quiet --eval "rs.status().ok"
```

### 4. Start PCSM

Build with `make build`. Stop every existing PCSM server before using
`--reset-state`. For sharded clusters:

```sh
./bin/pcsm --source="mongodb://src-mongos:27017" \
  --target="mongodb://tgt-mongos:29017" --reset-state --log-level=debug
```

For replica sets:

```sh
./bin/pcsm --source="mongodb://rs00:30000" \
  --target="mongodb://rs10:30100" --reset-state --log-level=debug
```

Target mongos listens on host port 29017 but container port 27017. PCSM removes
unsupported URI options including `directConnection` and lets the driver
discover topology. `replicaSet` is accepted but unnecessary for these URIs.
The HTTP startup log precedes socket binding. Use `/metrics` to check HTTP
readiness:

```sh
curl -fsS http://localhost:2242/metrics >/dev/null
```

### 5. Run the replication lifecycle

Use another terminal to start replication and inspect progress:

```sh
curl -fsS -X POST http://localhost:2242/start \
  -H 'Content-Type: application/json' -d '{}' | jq -e '.ok'
curl -fsS http://localhost:2242/status | jq .
```

Wait for `initialSync.completed: true`, then request a pause:

```sh
curl -fsS -X POST http://localhost:2242/pause \
  -H 'Content-Type: application/json' -d '{}' | jq -e '.ok'
curl -fsS http://localhost:2242/status | jq .
```

Wait for `state: "paused"`, then finalize:

```sh
curl -fsS -X POST http://localhost:2242/finalize \
  -H 'Content-Type: application/json' -d '{}' | jq -e '.ok'
curl -fsS http://localhost:2242/status | jq .
```

Wait for `finalization.completed: true` before stopping PCSM. Pause acceptance,
draining, failure, and finalize retry rules are in
[Pausing replication](README.md#pausing-replication).

### 6. Clean up

Stop PCSM before removing its clusters:

```sh
./hack/cleanup.sh
```

No-argument cleanup stops `pcsm-ha` before `rs`, `sh`, and optional `sh-ha`.
Explicit arguments preserve caller order, so put `pcsm-ha` before its topology.
Missing Compose files are skipped. Teardown failures are suppressed; inspect
`docker ps -a` and overlapping host
port 30000 if a later start reports `port already allocated`.

### Runtime configuration

| Environment variable | Flag | Scope |
| --- | --- | --- |
| `PCSM_SOURCE_URI` | `--source` | Server source URI |
| `PCSM_TARGET_URI` | `--target` | Server/reset target URI |
| `PCSM_PORT` | `--port` | Server and HTTP clients, default 2242 |
| `PCSM_LISTEN_HOST` | `--listen-host` | Server listener |
| `PCSM_LOG_LEVEL` | `--log-level` | Logging |
| `PCSM_HA_LEASE_TTL` | `--ha-lease-ttl` | Server lease TTL |
| `PCSM_MONGODB_OPERATION_TIMEOUT` | `--mongodb-operation-timeout` | MongoDB operations |
| `PCSM_CLONE_SEGMENT_SIZE` | `--clone-segment-size` | Start command |
| `PCSM_CLONE_SKIP_PRESPLIT` | `--clone-skip-presplit` | Start command |
| `PCSM_RECOVERY_CHECKPOINT_INTERVAL` | `--recovery-checkpoint-interval` | Hidden server option |

Nonpositive checkpoint intervals use 15s. Only explicitly bound configuration
keys have environment support; namespace filters and `resume --from-failure`
are read directly from flags. The complete bindings are in
[`config/config.go`](config/config.go), and flag declarations and defaults are
in [`main.go`](main.go).

For failover, lease settings, reset procedures, and monitoring, use the
[HA reference](ha/README.md).

### Diagnostics

Install the Python dependencies described below before running these tools:

```sh
hack/change_stream.py -u "mongodb://src-mongos:27017"
hack/change_stream.py -u "mongodb://tgt-mongos:29017" --show-checkpoints
hack/change_stream.py -u "mongodb://rs00:30000"
hack/monitor_writes.py -u "mongodb://src-mongos:27017"
```

## Running tests locally

When you work, you should periodically run tests to check that your changes don't break existing code.

### Go Unit Tests

Run all unit tests with race detection:

```sh
go test -race ./...
```

Or using make:

```sh
make test
```

To run a single test:

```sh
go test -race -run TestName ./package
```

For example:

```sh
go test -race -run TestFilter ./sel
go test -race -v -run TestSanitizeConnString ./mdb
```

### Integration and lint checks

`make test-integration` runs uncached Go tests with the
`integration` build tag and a five-minute timeout across the root package,
catalog, clone, replication, driver helpers, and HA. Docker/testcontainers must
be available. `MONGO_VERSION` selects the test image.

```sh
go test -race -v -tags integration -run TestName ./pcsm/catalog/...
make lint
make lint-py
```

`make lint` runs golangci-lint. `make lint-py` runs Ruff check and format-check
over `tests/` and `hack/`; `make fmt-py` applies fixes and formatting there.
`make clean` removes binaries and Go build/test caches. Exercise `./bin/pcsm`
directly when automated tests do not cover the changed behavior.

### Python E2E Tests

Python E2E tests require MongoDB clusters to be running and PCSM to be available.

#### Requirements

- Python 3.13.15
- [Poetry 2.4.2](https://python-poetry.org/docs/#installation) for dependency management

Install the Python dependencies and activate the project environment before executing
`hack/*.py` directly:

```sh
poetry install
eval "$(poetry env activate)"
```

#### 1. Deploy MongoDB clusters and run PCSM

Follow the steps in [Running PCSM locally](#running-pcsm-locally).

#### 2. Set environment variables

```sh
export TEST_SOURCE_URI="mongodb://src-mongos:27017"
export TEST_TARGET_URI="mongodb://tgt-mongos:29017"
export TEST_PCSM_URL="http://localhost:2242"
```

You don't have to run PCSM manually, you can let pytest to automatically manage PCSM process. Just export this:

```sh
export TEST_PCSM_BIN="./bin/pcsm"
```

The managed binary does not replace the required PCSM URL. CLI options override
their corresponding environment variables:

| Environment variable | Pytest option |
| --- | --- |
| `TEST_SOURCE_URI` | `--source-uri` |
| `TEST_TARGET_URI` | `--target-uri` |
| `TEST_PCSM_URL` | `--pcsm_url` |
| `TEST_PCSM_BIN` | `--pcsm-bin` |

#### 3. Run tests

Run all E2E tests:

```sh
poetry run pytest
```

Run a single test:

```sh
poetry run pytest tests/test_collections.py::test_clone_collection
```

Run tests including slow tests (disabled by default):

```sh
poetry run pytest --runslow
```

Slow tests are skipped by default. Unqualified local pytest discovers the full
`tests/` tree, including sharded modules; this differs from the CI selections
below. The default fixture drops non-system databases on both clusters before
each test; `test_hack_clis.py` overrides that fixture.

If Poetry reports `bad interpreter` after a Python change, use
`.venv/bin/pytest` directly. For a focused test:

```sh
.venv/bin/pytest tests/test_documents.py::test_insert_one
```

### CI coverage and external QA

| Workflow | Scope |
| --- | --- |
| [`go.yml`](.github/workflows/go.yml) | Race unit tests, formatter suggestions, lint, and integration on MongoDB 6.0/7.0/8.0 |
| [`e2etests.yml`](.github/workflows/e2etests.yml) | Same-major and lower-to-higher RS/sharded E2E, plus the 8.0 unequal-shard case |
| [`ci.yml`](.github/workflows/ci.yml) | External `Percona-QA/psmdb-testing` suite on PSMDB 6.0/7.0/8.0/8.3, split into five pytest partitions |

The external workflow's `shard` is test partitioning, not MongoDB shard count.
Packaging builds do not establish Go test coverage.

For scope comparable to `e2etests.yml`:

- RS jobs select `test_collections`, `test_documents`, `test_indexes`,
  `test_selective`, `test_transactions`, `test_pipeline_updates`,
  `test_idle_index_build`, and `test_target_outage`.
- Normal sharded jobs discover broadly and deselect
  `tests/test_documents.py::test_clone_auto_segment_size_not_byte_count`.
- The 8.0 three-source/two-target-shard case runs only
  `tests/test_presplit_sharded.py`.

`ci.yml` ignores PR changes confined to `tests/**` and `packaging/**`, and skips
draft PRs. Mark a PR ready for review or dispatch explicitly:

```sh
gh workflow run ci.yml --ref <branch> -f pcsm_branch=<branch>
```

The QA repository is [Percona-QA/psmdb-testing](https://github.com/Percona-QA/psmdb-testing).
Branch selection is explicit `tests_ver`, then the first case-sensitive
`PCSM-[0-9]+` ticket in the PR title if that branch exists, then `main`.
Push approved QA fixes directly to the selected ticket branch; no QA PR is
required. A QA-only push does not trigger this repository's workflow.

The operational Jenkins entry is `hetzner-pcsm-functional-tests` at
<https://psmdb.cd.percona.com/view/PCSM/>. Its current parameters and availability
are not verified by this repository. Existing operational guidance prefers
Hetzner for cost, with AWS as the fallback for capacity-stuck builds.

## Contributing to documentation

We welcome contributions to our documentation.

Documentation source files are in the [dedicated docs repository](https://github.com/percona/pcsm-docs).

Please follow the contributing guidelines in that repository for how to contribute to documentation.

## After your pull request is merged

Once your pull request is merged, you are an official Percona Community Contributor. Welcome to the community!
