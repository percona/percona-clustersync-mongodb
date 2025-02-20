# Percona MongoLink

Percona MongoLink is a tool for replicating data from a source MongoDB cluster to a target MongoDB cluster. It supports cloning data, replicating changes, and managing collections and indexes.

## Features

- **Clone**: Instantly transfer existing data from a source MongoDB to a target MongoDB.
- **Real-Time Replication**: Tail the oplog to keep your target cluster up to date.
- **Namespace Filtering**: Specify which databases and collections to include or exclude.
- **Automatic Index Management**: Ensure necessary indexes are created on the target.
- **HTTP API**: Start, finalize, and check replication status via REST endpoints.

## Setup

### Prerequisites

- Go 1.24 or later
- MongoDB 6.0 or later
- Python 3.13 or later (for testing)
- Poetry (for managing Python dependencies)

### Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/percona-lab/percona-mongolink.git
    cd percona-mongolink
    ```

2. Build the project:

    ```sh
    go build -o bin/mongolink ./mongolink
    ```

3. Run the server:

    ```sh
    bin/mongolink --source <source-mongodb-uri> --target <target-mongodb-uri>
    ```

    Alternatively, you can use environment variables:

    ```sh
    export SOURCE_URI=<source-mongodb-uri>
    export TARGET_URI=<target-mongodb-uri>
    bin/mongolink --source $SOURCE_URI --target $TARGET_URI
    ```

    **Note:** Connections to the source and target must have `readPreference=primary` and `writeConcern=majority` explicitly or unset.

## Usage

### Starting the Replication

To start the replication process, you can either use the command-line interface or send a POST request to the `/start` endpoint with the desired options:

#### Using Command-Line Interface

```sh
bin/mongolink start --port <port>
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
bin/mongolink finalize --port <port>
```

#### Using HTTP API

```sh
curl -X POST http://localhost:2242/finalize
```

### Checking the Status

To check the current status of the replication process, you can either use the command-line interface or send a GET request to the `/status` endpoint:

#### Using Command-Line Interface

```sh
bin/mongolink status --port <port>
```

#### Using HTTP API

```sh
curl http://localhost:2242/status
```

## MongoLink Options

When starting the MongoLink server, you can use the following options:

- `--port`: The port on which the server will listen (default: 2242)
- `--source`: The MongoDB connection string for the source cluster
- `--target`: The MongoDB connection string for the target cluster
- `--log-level`: The log level (default: "info")
- `--log-json`: Output log in JSON format with disabled color
- `--no-color`: Disable log ASCI color

Example:

```sh
bin/mongolink --source <source-mongodb-uri> --target <target-mongodb-uri> --port 2242 --log-level debug --log-json
```

## Log JSON Fields

When using the `--log-json` option, the logs will be output in JSON format with the following fields:

- `time`: Unix time when the log entry was created.
- `level`: Log level (e.g., "debug", "info", "warn", "error").
- `message`: Log message, if any.
- `error`: Error message, if any.
- `s`: Scope of the log entry.
- `ns`: Namespace (database.collection format).

Example:

```json
{
    "level": "info",
    "s": "mongolink",
    "time": 1740077878718,
    "message": "starting change replication at 1740077878.3",
}
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
{
  "ok": true
}
```

### POST /finalize

Finalizes the replication process.

#### Response

- `ok`: Boolean indicating if the operation was successful.
- `error` (optional): Error message if the operation failed.

Example:

```json
{
  "ok": true
}
```

### GET /status

Gets the current status of the replication process.

#### Response

- `ok`: Boolean indicating if the operation was successful.
- `error` (optional): Error message if the operation failed.
- `state`: Current state of the replication process.
- `finalizable`: Boolean indicating if the process can be finalized.
- `lastAppliedOpTime`: Last applied operation time.
- `info`: Additional information.
- `eventsProcessed`: Number of events processed.
- `clone`: Status of the cloning process.

Example:

```json
{
  "ok": true,
  "state": "running",
  "finalizable": false,
  "lastAppliedOpTime": "1622547800.1",
  "info": "replicating changes",
  "eventsProcessed": 100,
  "clone": {
    "finished": true,
    "estimatedTotalBytes": 1000000,
    "estimatedClonedBytes": 1000000
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

### Running Tests

To run the tests, use the following command:

```sh
poetry run pytest --source-uri <source-mongodb-uri> --target-uri <target-mongodb-uri> --mongolink-url http://localhost:2242
```

Alternatively, you can use environment variables:

```sh
export TEST_SOURCE_URI=<source-mongodb-uri>
export TEST_TARGET_URI=<target-mongodb-uri>
export TEST_MONGOLINK_URL=http://localhost:2242
poetry run pytest
```

## Contributing

Contributions are welcome. Please open a [JIRA](https://perconadev.atlassian.net/jira/software/c/projects/PML/issues) issue describing the proposed change, then submit a pull request on GitHub.

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
