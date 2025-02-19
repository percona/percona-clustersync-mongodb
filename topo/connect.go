package topo

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"

	"github.com/percona-lab/percona-mongolink/errors"
	"github.com/percona-lab/percona-mongolink/log"
)

const DefaultMongoURI = "mongodb://localhost:27017"

func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	if uri == "" {
		uri = DefaultMongoURI
	} else if !strings.HasPrefix(uri, "mongodb://") {
		uri = "mongodb://" + uri
	}

	opts := options.Client().ApplyURI(uri).
		SetServerAPIOptions(
			options.ServerAPI(options.ServerAPIVersion1).
				SetStrict(false).
				SetDeprecationErrors(true)).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority())

	conn, err := mongo.Connect(opts)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	err = conn.Ping(ctx, nil)
	if err != nil {
		if err1 := conn.Disconnect(ctx); err1 != nil {
			log.Warn(ctx, "disconnect: "+err1.Error())
		}
		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}
