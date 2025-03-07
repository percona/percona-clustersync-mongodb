// Package log provides logging utilities for the MongoLink application.
package log

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
)

const TimeFieldFormat = "2006-01-02 15:04:05.000"

// InitGlobals initializes the logger with the specified level and settings.
//   - level: the log level (e.g., debug, info, warn, error).
//   - json: if true, output logs in JSON format with disabled color.
//   - noColor: if true, disable color in the console output.
func InitGlobals(level zerolog.Level, json, noColor bool) *zerolog.Logger {
	zerolog.TimeFieldFormat = TimeFieldFormat
	zerolog.DurationFieldUnit = time.Second
	zerolog.DurationFieldInteger = true

	var logWriter io.Writer = os.Stdout
	if !json {
		logWriter = zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
			w.NoColor = noColor
			w.TimeFormat = TimeFieldFormat
		})
	}

	l := zerolog.New(logWriter).Level(level).With().Timestamp().Logger()
	zerolog.DefaultContextLogger = &l

	return &l
}

// AttrFn defines a function type for setting log attributes.
type AttrFn func(l zerolog.Context) zerolog.Context

func Elapsed(dur time.Duration) AttrFn {
	return func(l zerolog.Context) zerolog.Context {
		return l.Dur("elapsed_secs", dur)
	}
}

// Op sets the operation attribute.
func Op(op string) AttrFn {
	return func(l zerolog.Context) zerolog.Context {
		return l.Str("op", op)
	}
}

// NS sets the namespace attribute.
func NS(database, collection string) AttrFn {
	return func(l zerolog.Context) zerolog.Context {
		if collection == "" {
			return l.Str("ns", database)
		}

		return l.Str("ns", database+"."+collection)
	}
}

// OpTime sets the operation time attribute.
func OpTime(t, i uint32) AttrFn {
	return func(l zerolog.Context) zerolog.Context {
		return l.Uints32("op_ts", []uint32{t, i})
	}
}

// Tx sets the transaction and session ID attributes.
func Tx(txn *int64, lsid []byte) AttrFn {
	if txn == nil {
		return func(l zerolog.Context) zerolog.Context { return l }
	}

	hash := sha256.Sum224(lsid)
	encoded := base64.RawStdEncoding.EncodeToString(hash[:8])

	return func(l zerolog.Context) zerolog.Context {
		return l.Int64("txn", *txn).Str("sid", encoded)
	}
}

// New creates a new Logger with the specified scope.
func New(scope string) Logger {
	log := zerolog.Ctx(context.Background()).With().Logger()
	log.UpdateContext(func(logContext zerolog.Context) zerolog.Context {
		return logContext.Str("s", scope) // replace "s". (With() adds one more "s" to JSON)
	})

	return Logger{&log}
}

// Logger wraps a zerolog.Logger to provide additional functionality.
type Logger struct {
	zl *zerolog.Logger
}

// Ctx returns a Logger from the context.
//
//go:inline
func Ctx(ctx context.Context) Logger {
	return Logger{zerolog.Ctx(ctx)}
}

// WithContext returns a new context with the Logger.
//
//go:inline
func (l Logger) WithContext(ctx context.Context) context.Context {
	return l.zl.WithContext(ctx)
}

// With returns a new Logger with the specified attributes.
//
//go:inline
func (l Logger) With(opts ...AttrFn) Logger {
	c := l.zl.With()
	for _, opt := range opts {
		c = opt(c)
	}

	zl := c.Logger()

	return Logger{&zl}
}

//go:inline
func (l Logger) Unwrap() *zerolog.Logger {
	return l.zl
}

// Trace logs a trace level message.
//
//go:inline
func (l Logger) Trace(msg string) {
	l.zl.Trace().Msg(msg)
}

// Tracef logs a formatted trace level message.
//
//go:inline
func (l Logger) Tracef(msg string, args ...any) {
	l.zl.Trace().Msgf(msg, args...)
}

// Debug logs a debug level message.
//
//go:inline
func (l Logger) Debug(msg string) {
	l.zl.Debug().Msg(msg)
}

// Debugf logs a formatted debug level message.
//
//go:inline
func (l Logger) Debugf(msg string, args ...any) {
	l.zl.Debug().Msgf(msg, args...)
}

// Info logs an info level message.
//
//go:inline
func (l Logger) Info(msg string) {
	l.zl.Info().Msg(msg)
}

// Infof logs a formatted info level message.
//
//go:inline
func (l Logger) Infof(msg string, args ...any) {
	l.zl.Info().Msgf(msg, args...)
}

// Warn logs a warning level message.
//
//go:inline
func (l Logger) Warn(msg string) {
	l.zl.Warn().Msg(msg)
}

// Warnf logs a formatted warning level message.
//
//go:inline
func (l Logger) Warnf(msg string, args ...any) {
	l.zl.Warn().Msgf(msg, args...)
}

// Error logs an error level message with an error.
//
//go:inline
func (l Logger) Error(err error, msg string) {
	l.zl.Error().Err(err).Msg(msg)
}

// Errorf logs a formatted error level message with an error.
//
//go:inline
func (l Logger) Errorf(err error, msg string, args ...any) {
	l.zl.Error().Err(err).Msgf(msg, args...)
}
