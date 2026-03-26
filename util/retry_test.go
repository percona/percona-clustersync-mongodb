package util //nolint:testpackage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunWithRetry_NonTransientError(t *testing.T) {
	t.Parallel()

	nonTransientErr := errors.New("non-transient error") //nolint:err113
	calls := 0

	fn := func(_ context.Context) error {
		calls++

		return nonTransientErr
	}

	alwaysFalse := func(_ error) bool { return false }

	err := RunWithRetry(t.Context(), fn, alwaysFalse, 10*time.Millisecond, 2)
	require.ErrorIs(t, err, nonTransientErr)
	assert.Equal(t, 1, calls)
}

func TestRunWithRetry_FailureOnAllRetries(t *testing.T) {
	t.Parallel()

	retryableErr := errors.New("retryable") //nolint:err113
	calls := 0

	fn := func(_ context.Context) error {
		calls++

		return retryableErr
	}

	alwaysTrue := func(_ error) bool { return true }
	maxRetries := 3

	err := RunWithRetry(t.Context(), fn, alwaysTrue, time.Millisecond, maxRetries)
	require.ErrorIs(t, err, retryableErr)
	assert.Equal(t, maxRetries, calls)
}

func TestRunWithRetry_SuccessOnRetry(t *testing.T) {
	t.Parallel()

	retryableErr := errors.New("retryable") //nolint:err113
	calls := 0

	fn := func(_ context.Context) error {
		calls++
		if calls < 2 {
			return retryableErr
		}

		return nil
	}

	alwaysTrue := func(_ error) bool { return true }

	err := RunWithRetry(t.Context(), fn, alwaysTrue, time.Millisecond, 3)
	require.NoError(t, err)
	assert.Equal(t, 2, calls)
}

func TestRetryWithBackoff_TransientError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	fn := func() error {
		if calls.Add(1) == 1 {
			return errors.New("transient") //nolint:err113
		}

		return nil
	}

	err := RetryWithBackoff(t.Context(), fn, nil, time.Millisecond, time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, int32(2), calls.Load())
}

func TestRetryWithBackoff_UnrecoverableError(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	unrecoverableErr := errors.New("permanent") //nolint:err113

	fn := func() error {
		calls.Add(1)

		return unrecoverableErr
	}

	isUnrecoverable := func(err error) bool {
		return errors.Is(err, unrecoverableErr)
	}

	err := RetryWithBackoff(t.Context(), fn, isUnrecoverable, time.Millisecond, time.Millisecond)
	require.ErrorIs(t, err, unrecoverableErr)
	assert.Equal(t, int32(1), calls.Load())
}

func TestRetryWithBackoff_ContextCanceled(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		fn := func() error {
			<-ctx.Done()

			return ctx.Err()
		}

		var err error

		go func() {
			err = RetryWithBackoff(ctx, fn, nil, time.Second, time.Second)
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()

		require.ErrorIs(t, err, context.Canceled)
	})
}
