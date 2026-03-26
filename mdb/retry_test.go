package mdb //nolint:testpackage

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestRunWithRetry_NonTransientError(t *testing.T) {
	t.Parallel()

	nonTransiantErr := errors.New("non-transient error") //nolint:err113
	calls := 0

	fn := func(_ context.Context) error {
		calls++

		return nonTransiantErr
	}

	err := RunWithRetry(t.Context(), fn, 10*time.Millisecond, 2)
	if !errors.Is(err, nonTransiantErr) {
		t.Errorf("expected error %v, got %v", nonTransiantErr, err)
	}

	if calls != 1 {
		t.Errorf("expected fn to be called once, got %d", calls)
	}
}

func TestRunWithRetry_FalureOnAllRetries(t *testing.T) {
	t.Parallel()

	transientErr := mongo.WriteException{
		WriteErrors: []mongo.WriteError{
			{
				Code:    91, // ShutdownInProgress
				Message: "transient error",
			},
		},
	}

	calls := 0

	fn := func(_ context.Context) error {
		calls++

		return transientErr
	}

	maxRetries := 3

	err := RunWithRetry(t.Context(), fn, 1*time.Millisecond, maxRetries)
	if !errors.As(err, &transientErr) {
		t.Errorf("expected error %v, got %v", transientErr, err)
	}

	if calls != maxRetries {
		t.Errorf("expected fn to be called %d times, got %d", maxRetries, calls)
	}
}

func TestRunWithRetry_SuccessOnRetry(t *testing.T) {
	t.Parallel()

	transientErr := mongo.WriteException{
		WriteErrors: []mongo.WriteError{
			{
				Code:    91, // ShutdownInProgress
				Message: "transient error",
			},
		},
	}
	calls := 0

	fn := func(_ context.Context) error {
		calls++
		if calls < 2 {
			return transientErr
		}

		return nil
	}

	err := RunWithRetry(t.Context(), fn, 1*time.Millisecond, 3)
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	if calls != 2 {
		t.Errorf("expected fn to be called 2 times, got %d", calls)
	}
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

	err := RetryWithBackoff(t.Context(), fn, nil, time.Millisecond, time.Millisecond, 3)
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

	err := RetryWithBackoff(t.Context(), fn, isUnrecoverable, time.Millisecond, time.Millisecond, 3)
	require.ErrorIs(t, err, unrecoverableErr)
	assert.Equal(t, int32(1), calls.Load())
}

func TestRetryWithBackoff_MaxRetriesExhausted(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	retryableErr := errors.New("retryable") //nolint:err113

	fn := func() error {
		calls.Add(1)

		return retryableErr
	}

	err := RetryWithBackoff(t.Context(), fn, nil, time.Millisecond, time.Millisecond, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "max retries exhausted")
	assert.Equal(t, int32(3), calls.Load())
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
			err = RetryWithBackoff(ctx, fn, nil, time.Second, time.Second, 3)
		}()

		synctest.Wait()
		cancel()
		synctest.Wait()

		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestRetryWithBackoff_UnlimitedRetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	retryableErr := errors.New("retryable") //nolint:err113

	fn := func() error {
		if calls.Add(1) < 10 {
			return retryableErr
		}

		return nil
	}

	err := RetryWithBackoff(t.Context(), fn, nil, time.Millisecond, time.Millisecond, 0)
	require.NoError(t, err)
	assert.Equal(t, int32(10), calls.Load())
}
