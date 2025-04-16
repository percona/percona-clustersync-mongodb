package util

import (
	"context"
	"time"
)

func WithTimeout(ctx context.Context, dur time.Duration, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, dur)
	defer cancelTimeout()

	return fn(timeoutCtx)
}
