package mdb

import (
	"context"
	"time"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/log"
)

const (
	// DefaultRetryInterval is the default interval for retrying transient errors.
	DefaultRetryInterval = 5 * time.Second
	// DefaultMaxRetries is the default maximum number of retries for transient errors.
	DefaultMaxRetries = 3
)

// RunWithRetry executes the provided function with retry logic for transient errors.
// It retries the function up to maxRetries times,
// with an exponential backoff starting from retryInterval.
func RunWithRetry(
	ctx context.Context,
	fn func(context.Context) error,
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

		if !IsTransient(err) {
			return err
		}

		log.Ctx(ctx).Warnf("Transient write error: %v, retry attempt %d retrying in %s",
			err, attempt, currentInterval)

		time.Sleep(currentInterval)
		currentInterval *= 2
	}

	return err
}

// RetryWithBackoff retries fn with exponential backoff up to maxRetries times.
// It stops early on context cancellation or when isUnrecoverable classifies the
// error as permanent.
func RetryWithBackoff(
	ctx context.Context,
	fn func() error,
	isUnrecoverable func(error) bool,
	initialDelay time.Duration,
	maxDelay time.Duration,
	maxRetries int,
) error {
	delay := initialDelay

	var err error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		err = fn()
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

		log.Ctx(ctx).Warnf("Retryable error (attempt %d/%d): %v, retrying in %s",
			attempt, maxRetries, err, delay)

		if attempt < maxRetries {
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "retry wait")
			}

			delay = min(delay*2, maxDelay) //nolint:mnd
		}
	}

	return err
}
