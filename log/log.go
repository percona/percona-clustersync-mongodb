package log

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type Attrs struct {
	Scope  string
	Op     string
	OpTime bson.Timestamp
	OpNS   string
}

type AttrOption func(l zerolog.Context) zerolog.Context

func Scope(s string) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		return l.Str("s", s)
	}
}

func Operation(op string) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		return l.Str("op", op)
	}
}

func NS(db, coll string) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		if coll == "" {
			return l.Str("ns", db)
		}
		return l.Str("ns", db+"."+coll)
	}
}

func OpTime(ts bson.Timestamp) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		return l.Uints32("op_ts", []uint32{ts.T, ts.I})
	}
}

func Tx(txn *int64, lsid bson.Raw) AttrOption {
	return func(l zerolog.Context) zerolog.Context {
		if txn == nil {
			return l
		}
		return l.Int64("txn", *txn)
	}
}

func WithAttrs(ctx context.Context, opts ...AttrOption) context.Context {
	l := zerolog.Ctx(ctx).With()
	for _, opt := range opts {
		l = opt(l)
	}
	return l.Logger().WithContext(ctx)
}

func CopyContext(from, to context.Context) context.Context {
	return zerolog.Ctx(from).WithContext(to)
}

func Trace(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Trace().Timestamp().Msg(msg)
}

func Tracef(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Trace().Timestamp().Msgf(msg, args...)
}

func Debug(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Debug().Timestamp().Msg(msg)
}

func Debugf(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Debug().Timestamp().Msgf(msg, args...)
}

func Info(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Info().Timestamp().Msg(msg)
}

func Infof(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Info().Timestamp().Msgf(msg, args...)
}

func Warn(ctx context.Context, msg string) {
	zerolog.Ctx(ctx).Warn().Timestamp().Msg(msg)
}

func Warnf(ctx context.Context, msg string, args ...any) {
	zerolog.Ctx(ctx).Warn().Timestamp().Msgf(msg, args...)
}

func Error(ctx context.Context, err error, msg string) {
	zerolog.Ctx(ctx).Error().Err(err).Timestamp().Msg(msg)
}

func Errorf(ctx context.Context, err error, msg string, args ...any) {
	zerolog.Ctx(ctx).Error().Err(err).Timestamp().Msgf(msg, args...)
}

func New(level zerolog.Level, noColor bool) *zerolog.Logger {
	w := zerolog.NewConsoleWriter(func(w *zerolog.ConsoleWriter) {
		w.NoColor = noColor
		w.TimeFormat = time.DateTime
	})
	l := zerolog.New(w).Level(level)
	return &l
}

func SetFallbackLogger(l *zerolog.Logger) {
	zerolog.DefaultContextLogger = l
}
