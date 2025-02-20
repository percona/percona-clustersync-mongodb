/*
Package mlink provides functionality for replicating and cloning data between MongoDB instances.

This package includes the following main components:

- Coordinator: Manages the overall replication process, including cloning and change replication.
- Clone: Handles the cloning of data from a source MongoDB to a target MongoDB.
- Repl: Handles the replication of changes from a source MongoDB to a target MongoDB.
- Catalog: Manages collections and indexes in the target MongoDB.

Example usage:

	import (
		"context"
		"go.mongodb.org/mongo-driver/v2/mongo"
		"go.mongodb.org/mongo-driver/v2/mongo/options"
		"github.com/percona-lab/percona-mongolink/mlink"
	)

	func main() {
		sourceClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://source:27017").
			SetReadPreference(readpref.Primary()).
			SetWriteConcern(writeconcern.New(writeconcern.WMajority())))
		if err != nil {
			log.Fatal(err)
		}
		defer sourceClient.Disconnect(context.Background())

		targetClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://target:27017").
			SetReadPreference(readpref.Primary()).
			SetWriteConcern(writeconcern.New(writeconcern.WMajority())))
		if err != nil {
			log.Fatal(err)
		}
		defer targetClient.Disconnect(context.Background())

		coordinator := mlink.New(sourceClient, targetClient)
		options := &mlink.StartOptions{
			IncludeNamespaces: []string{"db1", "db2"},
			ExcludeNamespaces: []string{"db3"},
		}

		err = coordinator.Start(context.Background(), options)
		if err != nil {
			log.Fatal(err)
		}

		status, err := coordinator.Status(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("Replication status: %+v", status)
	}
*/
package mlink
