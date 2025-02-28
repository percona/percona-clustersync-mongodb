package topo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/percona-lab/percona-mongolink/config"
	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

// Connect establishes a connection to a MongoDB instance using the provided URI.
// If the URI is empty, it returns an error.
func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	if uri == "" {
		return nil, errors.New("invalid MongoDB URI")
	}

	opts := options.Client().ApplyURI(uri).
		SetServerAPIOptions(
			options.ServerAPI(options.ServerAPIVersion1).
				SetStrict(false).
				SetDeprecationErrors(true)).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority())

	if config.MongoLogEnabled {
		opts = opts.SetLoggerOptions(options.Logger().
			SetSink(log.NewMongoLogger(ctx)).
			SetComponentLevel(config.MongoLogComponent, config.MongoLogLevel))
	}

	conn, err := mongo.Connect(opts)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		if err1 := conn.Disconnect(ctx); err1 != nil {
			log.Ctx(ctx).Warn("Disconnect: " + err1.Error())
		}

		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}
