package util

import (
	"context"
	"time"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
)

const (
	DefaultRetryInterval = 5 * time.Second
	DefaultMaxRetries    = 3
)

// RunWithRetry executes fn with retry logic. Only errors where isRetryable
// returns true are retried, up to maxRetries times with exponential backoff
// starting from retryInterval.
func RunWithRetry(
	ctx context.Context,
	fn func(context.Context) error,
	isRetryable func(error) bool,
	retryInterval time.Duration,
	maxRetries int,
) error {
	if retryInterval <= 0 || maxRetries <= 0 {
		return errors.New("retryInterval and maxRetries must be greater than zero")
	}

	var err error

	currentInterval := retryInterval

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = fn(ctx)
		if err == nil {
			return nil
		}

		if ctx.Err() != nil {
			return err
		}

		if !isRetryable(err) {
			return err
		}

		log.Ctx(ctx).Warnf("Transient write error: %v, retry attempt %d retrying in %s",
			err, attempt, currentInterval)

		time.Sleep(currentInterval)
		currentInterval *= 2 //nolint:mnd
	}

	return err
}

// RetryWithBackoff retries fn indefinitely with exponential backoff until it
// succeeds, the context is canceled, or isUnrecoverable classifies the error
// as permanent. Context errors (Canceled, DeadlineExceeded) always stop retries.
func RetryWithBackoff(
	ctx context.Context,
	fn func() error,
	isUnrecoverable func(error) bool,
	initialDelay time.Duration,
	maxDelay time.Duration,
) error {
	delay := initialDelay

	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		if ctx.Err() != nil ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if isUnrecoverable != nil && isUnrecoverable(err) {
			return err
		}

		log.Ctx(ctx).Warnf("Retryable error (attempt %d): %v, retrying in %s",
			attempt, err, delay)

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "retry wait")
		}

		delay = min(delay*2, maxDelay) //nolint:mnd
	}
}
