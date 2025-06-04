package topo

import (
	"context"
	"net/url"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/connstring"

	"github.com/percona/percona-link-mongodb/config"
	"github.com/percona/percona-link-mongodb/errors"
	"github.com/percona/percona-link-mongodb/log"
	"github.com/percona/percona-link-mongodb/util"
)

type ConnectOptions struct {
	Compressors []string
}

// Connect establishes a connection to a MongoDB instance using the provided URI.
// If the URI is empty, it returns an error.
func Connect(ctx context.Context, uri string) (*mongo.Client, error) {
	return ConnectWithOptions(ctx, uri, &ConnectOptions{})
}

// Connect establishes a connection to a MongoDB instance using the provided URI and options.
// If the URI is empty, it returns an error.
func ConnectWithOptions(
	ctx context.Context,
	uri string,
	connOpts *ConnectOptions,
) (*mongo.Client, error) {
	if uri == "" {
		return nil, errors.New("invalid MongoDB URI")
	}

	_, err := connstring.ParseAndValidate(uri)
	if err != nil {
		return nil, errors.Wrap(err, "parse and validate MongoDB URI")
	}

	sanitizedURI, err := sanitizeMongoURI(uri)
	if err != nil {
		return nil, errors.Wrap(err, "sanitize MongoDB URI")
	}

	opts := options.Client().ApplyURI(sanitizedURI).
		SetServerAPIOptions(
			options.ServerAPI(options.ServerAPIVersion1).
				SetStrict(false).
				SetDeprecationErrors(true)).
		SetReadPreference(readpref.Primary()).
		SetReadConcern(readconcern.Majority()).
		SetWriteConcern(writeconcern.Majority()).
		SetTimeout(config.OperationTimeout)

	if connOpts != nil && connOpts.Compressors != nil {
		opts.SetCompressors(connOpts.Compressors)
	}

	if config.MongoLogEnabled {
		opts = opts.SetLoggerOptions(options.Logger().
			SetSink(log.MongoLogger(ctx)).
			SetComponentLevel(config.MongoLogComponent, config.MongoLogLevel))
	}

	conn, err := mongo.Connect(opts)
	if err != nil {
		return nil, err //nolint:wrapcheck
	}

	err = util.CtxWithTimeout(ctx, config.PingTimeout, func(ctx context.Context) error {
		return conn.Ping(ctx, nil)
	})
	if err != nil {
		err1 := util.CtxWithTimeout(ctx, config.DisconnectTimeout, conn.Disconnect)
		if err1 != nil {
			log.Ctx(ctx).Warn("Disconnect: " + err1.Error())
		}

		return nil, errors.Wrap(err, "ping")
	}

	return conn, nil
}

func sanitizeMongoURI(uri string) (string, error) {
	idx := strings.IndexRune(uri, '?')
	if idx == -1 {
		return uri, nil
	}

	pairs := strings.FieldsFunc(uri[idx+1:], func(r rune) bool { return r == '&' || r == ';' })
	allowed := make([]string, 0, len(pairs))
	for _, p := range pairs {
		key, _, _ := strings.Cut(p, "=")

		k, err := url.QueryUnescape(key)
		if err != nil {
			return "", errors.Wrapf(err, "invalid option key %q", key)
		}

		if !slices.Contains(allowedConnStringOptions, strings.ToLower(k)) {
			log.New("connect").Warnf("Connection string option %q is not allowed", key)

			continue
		}

		allowed = append(allowed, p)
	}

	ret := uri[:idx] + "?" + strings.Join(allowed, "&")

	return ret, nil
}

//nolint:gochecknoglobals
var allowedConnStringOptions = []string{
	"appname",
	"replicaset",

	"authsource",
	"authmechanism",
	"authmechanismproperties",
	"gssapiservicename",

	"tls",
	"ssl",
	"tlscertificatekeyfile",
	"tlscertificatekeyfilepassword",
	"tlscafile",
	"tlsallowinvalidcertificates",
	"tlsallowinvalidhostnames",
	"tlsinsecure",
}
