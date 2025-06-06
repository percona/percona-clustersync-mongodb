# Percona Link for MongoDB

Percona Link for MongoDB is a tool for replicating data from a source MongoDB cluster to a target MongoDB cluster. It supports cloning data, replicating changes, and managing collections and indexes.

## Features

- **Clone**: Instantly transfer existing data from a source MongoDB to a target MongoDB.
- **Real-Time Replication**: Tail the oplog to keep your target cluster up to date.
- **Namespace Filtering**: Specify which databases and collections to include or exclude.
- **Automatic Index Management**: Ensure necessary indexes are created on the target.
- **HTTP API**: Start, finalize, pause, resume, and check replication status via REST endpoints.

## Setup

### Prerequisites

- Go 1.24 or later
- MongoDB 6.0 or later
- Python 3.13 or later (for testing)
- Poetry (for managing Python dependencies)

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/percona/percona-link-mongodb.git
    cd percona-link-mongodb
    ```

2. Build the project using the Makefile:

    ```sh
    make build
    ```

    Alternatively, you can install PLM from the cloned repo using `go install`:

    ```sh
    go install .
    ```

    > This will install `plm` into your `GOBIN` directory. If `GOBIN` is included in your `PATH`, you can run Percona Link for MongoDB by typing `plm` in your terminal.

3. Run the server:

    ```sh
    bin/plm --source <source-mongodb-uri> --target <target-mongodb-uri>
    ```

    Alternatively, you can use environment variables:

    ```sh
    export PLM_SOURCE_URI=<source-mongodb-uri>
    export PLM_TARGET_URI=<target-mongodb-uri>
    bin/plm
    ```

## Usage

### Starting the Replication

To start the replication process, you can either use the command-line interface or send a POST request to the `/start` endpoint with the desired options:

#### Using Command-Line Interface

```sh
bin/plm start
```

#### Using HTTP API

```sh
curl -X POST http://localhost:2242/start -d '{
    "includeNamespaces": ["db1.collection1", "db2.collection2"],
    "excludeNamespaces": ["db3.collection3"]
}'
```

### Finalizing the Replication

To finalize the replication process, you can either use the command-line interface or send a POST request to the `/finalize` endpoint:

#### Using Command-Line Interface

```sh
bin/plm finalize
```

#### Using HTTP API

```sh
curl -X POST http://localhost:2242/finalize
```

### Pausing the Replication

To pause the replication process, you can either use the command-line interface or send a POST request to the `/pause` endpoint:

#### Using Command-Line Interface

```sh
bin/plm pause
```

#### Using HTTP API

```sh
curl -X POST http://localhost:2242/pause
```

### Resuming the Replication

To resume the replication process, you can either use the command-line interface or send a POST request to the `/resume` endpoint:

#### Using Command-Line Interface

```sh
bin/plm resume
```

#### Using HTTP API

```sh
curl -X POST http://localhost:2242/resume
```

### Checking the Status

To check the current status of the replication process, you can either use the command-line interface or send a GET request to the `/status` endpoint:

#### Using Command-Line Interface

```sh
bin/plm status
```

#### Using HTTP API

```sh
curl http://localhost:2242/status
```

## PLM Options

When starting the PLM server, you can use the following options:

- `--port`: The port on which the server will listen (default: 2242)
- `--source`: The MongoDB connection string for the source cluster
- `--target`: The MongoDB connection string for the target cluster
- `--log-level`: The log level (default: "info")
- `--log-json`: Output log in JSON format with disabled color
- `--no-color`: Disable log ASCI color

Example:

```sh
bin/plm \
    --source <source-mongodb-uri> \
    --target <target-mongodb-uri> \
    --port 2242 \
    --log-level debug \
    --log-json
```

## Log JSON Fields

When using the `--log-json` option, the logs will be output in JSON format with the following fields:

- `time`: Unix time when the log entry was created.
- `level`: Log level (e.g., "debug", "info", "warn", "error").
- `message`: Log message, if any.
- `error`: Error message, if any.
- `s`: Scope of the log entry.
- `ns`: Namespace (database.collection format).
- `elapsed_secs`: The duration in seconds for the specific operation to complete.

Example:

```json
{ "level": "info",
  "s": "clone",
  "ns": "db_1.coll_1",
  "elapsed_secs": 0,
  "time": "2025-02-23 11:26:03.758",
  "message": "Cloned db_1.coll_1" }

{ "level": "info",
  "s": "plm",
  "elapsed_secs": 0,
  "time": "2025-02-23 11:26:03.857",
  "message": "Change replication stopped at 1740335163.1740335163 source cluster time" }
```

## HTTP API

### POST /start

Starts the replication process.

#### Request Body

- `includeNamespaces` (optional): List of namespaces to include in the replication.
- `excludeNamespaces` (optional): List of namespaces to exclude from the replication.

Example:

```json
{
    "includeNamespaces": ["dbName.*", "anotherDB.collName1", "anotherDB.collName2"],
    "excludeNamespaces": ["dbName.collName"]
}
```

#### Response

- `ok`: Boolean indicating if the operation was successful.
- `error` (optional): Error message if the operation failed.

Example:

```json
{ "ok": true }
```

### POST /finalize

Finalizes the replication process.

#### Response

- `ok`: Boolean indicating if the operation was successful.
- `error` (optional): Error message if the operation failed.

Example:

```json
{ "ok": true }
```

### POST /pause

Pauses the replication process.

#### Response

- `ok`: Boolean indicating if the operation was successful.
- `error` (optional): Error message if the operation failed.

Example:

```json
{ "ok": true }
```

### POST /resume

Resumes the replication process.

#### Request Body

- `fromFailure` (optional): Allows PLM to resume from failed state

Example:

```json
{
    "fromFailure": true
}
```

#### Response

- `ok`: Boolean indicating if the operation was successful.
- `error` (optional): Error message if the operation failed.

Example:

```json
{ "ok": true }
```

### GET /status

The /status endpoint provides the current state of the PLM replication process, including its progress, lag, and event processing details.

#### Response

- `ok`: indicates if the operation was successful.
- `state`: the current state of the replication.
- `info`: provides additional information about the current state.
- `error` (optional): the error message if the operation failed.

- `lagTime`: the current lag time in logical seconds between source and target clusters.
- `eventsProcessed`: the number of events processed.
- `lastReplicatedOpTime`: the last replicated operation time.

- `initialSync.completed`: indicates if the initial sync is completed.
- `initialSync.lagTime`: the lag time in logical seconds until the initial sync completed.

- `initialSync.cloneCompleted`: indicates if the cloning process is completed.
- `initialSync.estimatedCloneSize`: the estimated total size of the clone.
- `initialSync.clonedSize`: the size of the data that has been cloned.

Example:

```json
{
    "ok": true,
    "state": "running",
    "info": "Initial Sync",

    "lagTime": 22,
    "eventsProcessed": 5000,
    "lastReplicatedOpTime": "1740335200.5",

    "initialSync": {
        "completed": false,
        "lagTime": 5,

        "cloneCompleted": false,
        "estimatedCloneSize": 5000000000,
        "clonedSize": 2500000000
    }
}
```

## Testing

### Prerequisites

- Install Poetry:

    ```sh
    curl -sSL https://install.python-poetry.org | python3 -
    ```

- Install the required Python packages:

    ```sh
    poetry install
    ```

### Build for Testing

To build the project for testing, use the following command:

```sh
make test-build
```

### Running Tests

To run the tests, use the following command:

```sh
poetry run pytest \
    --source-uri <source-mongodb-uri> \
    --target-uri <target-mongodb-uri> \
    --plm_url http://localhost:2242 \
    --plm-bin bin/plm_test
```

Alternatively, you can use environment variables:

```sh
export TEST_SOURCE_URI=<source-mongodb-uri>
export TEST_TARGET_URI=<target-mongodb-uri>
export TEST_PLM_URL=http://localhost:2242
export TEST_PLM_BIN=bin/plm_test
poetry run pytest
```

> The `--plm-bin` flag or `TEST_PLM_BIN` environment variable specifies the path to the PLM binary. This allows the test suite to manage the PLM process, ensuring it starts and stops as needed during the tests. If neither the flag nor the environment variable is provided, you must run PLM externally before running the tests.

## Contributing

Contributions are welcome. Please open a [JIRA](https://perconadev.atlassian.net/jira/software/c/projects/PLM/issues) issue describing the proposed change, then submit a pull request on GitHub.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
