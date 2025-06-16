package topo //nolint:testpackage

import (
	"context"
	"errors"
	"testing"
	"time"

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
