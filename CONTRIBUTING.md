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

* Go 1.25 or above. See [Installing and setting up Go tools](https://golang.org/doc/install) for more information
* make

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

To run PCSM locally for development and testing, follow these steps:

### 1. Deploy MongoDB clusters

Start the source and target sharded clusters using Docker Compose:

```sh
MONGO_VERSION="8.0" ./hack/sh/run.sh
```

For replica set clusters instead of sharded:

```sh
MONGO_VERSION="8.0" ./hack/rs/run.sh
```

### 2. Update /etc/hosts

Add the following entries to your `/etc/hosts` file to resolve the container hostnames:

```
# pcsm test clusters
127.0.0.1 rs00 rs01 rs02 rs10 rs11 rs12
127.0.0.1 src-rs00 src-rs10 src-cfg0 src-mongos
127.0.0.1 tgt-rs00 tgt-rs10 tgt-cfg0 tgt-mongos
```

### 3. Start PCSM

Run PCSM connecting to the source and target clusters:

```sh
make pcsm-start SOURCE="mongodb://src-mongos:27017" TARGET="mongodb://tgt-mongos:29017"
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
go test -race -v -run TestSanitizeConnString ./topo
```

### Python E2E Tests

Python E2E tests require MongoDB clusters to be running and PCSM to be available.

#### Requirements

* Python 3.13 or above
* [Poetry](https://python-poetry.org/docs/#installation) for dependency management

Install the Python dependencies:

```sh
poetry install
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

## Contributing to documentation

We welcome contributions to our documentation.

Documentation source files are in the [dedicated docs repository](https://github.com/percona/pcsm-docs).

Please follow the contributing guidelines in that repository for how to contribute to documentation.

## After your pull request is merged

Once your pull request is merged, you are an official Percona Community Contributor. Welcome to the community!
