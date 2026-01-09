package topo //nolint:testpackage

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestCollStats_DecodeFromFloat64(t *testing.T) {
	t.Parallel()

	input := bson.M{
		"count":      0.0,
		"size":       73179136.0,
		"avgObjSize": 0.0,
	}

	data, err := bson.Marshal(input)
	require.NoError(t, err)

	var stats CollStats
	err = bson.Unmarshal(data, &stats)
	require.NoError(t, err)

	assert.Equal(t, int64(0), stats.Count)
	assert.Equal(t, int64(73179136), stats.Size)
	assert.Equal(t, int64(0), stats.AvgObjSize)
}

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
