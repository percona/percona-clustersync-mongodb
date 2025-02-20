package log

import (
	"context"

	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mongoLogger is a wrapper for zerolog.Logger that implements the options.LogSink interface.
type mongoLogger struct {
	zl *zerolog.Logger
}

var _ options.LogSink = mongoLogger{}

// MongoLogger returns a logSink for MongoDB logging.
func MongoLogger(ctx context.Context) mongoLogger {
	return mongoLogger{zerolog.Ctx(ctx)}
}

// Info logs an info level message with additional key-value pairs.
func (s mongoLogger) Info(level int, message string, keysAndValues ...any) {
	s.zl.Info().Timestamp().Int("level", level).Fields(keysAndValues).Msg(message)
}

// Error logs an error level message with an error and additional key-value pairs.
func (s mongoLogger) Error(err error, message string, keysAndValues ...any) {
	s.zl.Error().Timestamp().Err(err).Fields(keysAndValues).Msg(message)
}
