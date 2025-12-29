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

## Join Percona Squad!

Participate in monthly SWAG raffles, get early access to new product features, and invite-only "ask me anything" sessions with database performance experts. Interested? Fill in the form at [squad.percona.com/mongodb](https://squad.percona.com/mongodb)

```
                    %                        _____
                   %%%                      |  __ \
                 ###%%%%%%%%%%%%*           | |__) |__ _ __ ___ ___  _ __   __ _
                ###  ##%%      %%%%         |  ___/ _ \ '__/ __/ _ \| '_ \ / _` |
              ####     ##%       %%%%       | |  |  __/ | | (_| (_) | | | | (_| |
             ###        ####      %%%       |_|   \___|_|  \___\___/|_| |_|\__,_|
           ,((###         ###     %%%         _____                       _
          (((( (###        ####  %%%%        / ____|                     | |
         (((     ((#         ######         | (___   __ _ _   _  __ _  __| |
       ((((       (((#        ####           \___ \ / _` | | | |/ _` |/ _` |
      /((          ,(((        *###          ____) | (_| | |_| | (_| | (_| |
    ////             (((         ####       |_____/ \__, |\__,_|\__,_|\__,_|
   ///                ((((        ####                 | |
 /////////////(((((((((((((((((########                |_|
```

## Contact

You can reach us:

- on [Forums](https://forums.percona.com/c/mongodb/mongodb-general-discussion/33)
- or [Professional Support](https://www.percona.com/about/contact)
