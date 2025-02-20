package log

import (
	"context"

	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// MongoLogSink is a wrapper for zerolog.Logger that implements the options.LogSink interface.
type MongoLogSink struct {
	zl *zerolog.Logger
}

var _ options.LogSink = MongoLogSink{}

// NewMongoLogger returns a logSink for MongoDB logging.
func NewMongoLogger(ctx context.Context) MongoLogSink {
	return MongoLogSink{zerolog.Ctx(ctx)}
}

// Info logs an info level message with additional key-value pairs.
func (s MongoLogSink) Info(level int, message string, keysAndValues ...any) {
	s.zl.Info().Int("level", level).Fields(keysAndValues).Msg(message)
}

// Error logs an error level message with an error and additional key-value pairs.
func (s MongoLogSink) Error(err error, message string, keysAndValues ...any) {
	s.zl.Error().Err(err).Fields(keysAndValues).Msg(message)
}
