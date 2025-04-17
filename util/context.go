package util

import (
	"context"
	"time"
)

// CtxWithTimeout wraps a context with a timeout duration and invokes the provided function.
// Returns the error returned by the function fn.
func CtxWithTimeout(ctx context.Context, dur time.Duration, fn func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	timeoutCtx, cancelTimeout := context.WithTimeout(ctx, dur)
	defer cancelTimeout()

	return fn(timeoutCtx)
}
