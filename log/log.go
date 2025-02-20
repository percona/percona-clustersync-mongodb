package log

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"time"

	"github.com/rs/zerolog"
)

// AttrOption defines a function type for setting log attributes.
type AttrOption func(l zerolog.Context) zerolog.Context

// Scope sets the scope attribute.
func Scope(s string) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		return l.Str("s", s)
	}
}

// Operation sets the operation attribute.
func Operation(op string) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		return l.Str("op", op)
	}
}

// NS sets the namespace attribute.
func NS(db, coll string) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		if coll == "" {
			return l.Str("ns", db)
		}
		return l.Str("ns", db+"."+coll)
	}
}

// OpTime sets the operation time attribute.
func OpTime(t, i uint32) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		return l.Uints32("op_ts", []uint32{t, i})
	}
}

// Tx sets the transaction and session ID attributes.
func Tx(txn *int64, lsid []byte) AttrOption {
	if txn == nil {
		return func(l zerolog.Context) zerolog.Context { return l }
	}

	hash := sha256.Sum224(lsid)
	encoded := base64.RawStdEncoding.EncodeToString(hash[:8])

	return func(l zerolog.Context) zerolog.Context {
		if txn == nil {
			return l
		}
		return l.Int64("txn", *txn).Str("sid", encoded)
	}
}

// WithAttrs adds attributes to the logger context.
func WithAttrs(ctx context.Context, opts ...AttrOption) context.Context {
	l := zerolog.Ctx(ctx).With()
	for _, opt := range opts {
		l = opt(l)
	}
	return l.Logger().WithContext(ctx)
}

// CopyContext copies the logger from one context to another.
func CopyContext(from, to context.Context) context.Context {
	return zerolog.Ctx(from).WithContext(to)
}

// Trace logs a trace level message.
func Trace(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Trace().Timestamp().Msg(msg)
}

// Tracef logs a formatted trace level message.
func Tracef(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Trace().Timestamp().Msgf(msg, args...)
}

// Debug logs a debug level message.
func Debug(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Debug().Timestamp().Msg(msg)
}

// Debugf logs a formatted debug level message.
func Debugf(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Debug().Timestamp().Msgf(msg, args...)
}

// Info logs an info level message.
func Info(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Info().Timestamp().Msg(msg)
}

// Infof logs a formatted info level message.
func Infof(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Info().Timestamp().Msgf(msg, args...)
}

// Warn logs a warning level message.
func Warn(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Warn().Timestamp().Msg(msg)
}

// Warnf logs a formatted warning level message.
func Warnf(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Warn().Timestamp().Msgf(msg, args...)
}

// Error logs an error level message with an error.
func Error(ctx context.Context, err error, msg string) {
	zerolog.Ctx(ctx).Error().Err(err).Timestamp().Msg(msg)
}

// Errorf logs a formatted error level message with an error.
func Errorf(ctx context.Context, err error, msg string, args ...any) {
	zerolog.Ctx(ctx).Error().Err(err).Timestamp().Msgf(msg, args...)
}

// New creates a new logger with the specified level and color settings.
func New(level zerolog.Level, noColor bool) *zerolog.Logger {
	w := zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.NoColor = noColor
		w.TimeFormat = time.DateTime
	})
	l := zerolog.New(w).Level(level)
	return &l
}

// SetFallbackLogger sets the fallback logger.
func SetFallbackLogger(l *zerolog.Logger) {
	zerolog.DefaultContextLogger = l
}
