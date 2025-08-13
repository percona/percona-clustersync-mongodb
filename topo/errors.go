package topo

import (
	"context"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-link-mongodb/errors"
)

// IsIndexNotFound checks if an error is an index not found error.
func IsIndexNotFound(err error) bool {
	return isMongoCommandError(err, "IndexNotFound")
}

// IsIndexOptionsConflict checks if an error is an index options conflict error.
func IsIndexOptionsConflict(err error) bool {
	return isMongoCommandError(err, "IndexOptionsConflict")
}

func IsNamespaceNotFound(err error) bool {
	return isMongoCommandError(err, "NamespaceNotFound")
}

// IsCollectionDropped checks if the error is caused by a collection being dropped.
func IsCollectionDropped(err error) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && cmdErr.Name == "QueryPlanKilled" {
		return strings.Contains(cmdErr.Message, "collection dropped")
	}

	return false
}

// IsCollectionRenamed checks if the error is caused by a collection being renamed.
func IsCollectionRenamed(err error) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && cmdErr.Name == "QueryPlanKilled" {
		return strings.Contains(cmdErr.Message, "collection renamed")
	}

	return false
}

func IsChangeStreamHistoryLost(err error) bool {
	return isMongoCommandError(err, "ChangeStreamHistoryLost")
}

func IsCappedPositionLost(err error) bool {
	return isMongoCommandError(err, "CappedPositionLost")
}

// isMongoCommandError checks if an error is a MongoDB error with the specified name.
func isMongoCommandError(err error, name string) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		return cmdErr.Name == name
	}

	return false
}

// IsTransient checks if the error is a transient error that can be retried.
// It checks for specific MongoDB error codes that indicate transient issues.
func IsTransient(err error) bool {
	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	le, ok := err.(mongo.LabeledError) //nolint:errorlint
	if ok && le.HasErrorLabel("RetryableWriteError") {
		return true
	}

	transientErrorCodes := map[int]struct{}{
		11602: {}, // InterruptedDueToReplStateChange
		91:    {}, // ShutdownInProgress
		189:   {}, // PrimarySteppedDown
		10107: {}, // NotWritablePrimary
		13435: {}, // NotPrimaryNoSecondaryOk
	}

	var wEx mongo.WriteException
	if errors.As(err, &wEx) {
		for _, we := range wEx.WriteErrors {
			_, ok := transientErrorCodes[we.Code]

			return ok
		}

		if wEx.WriteConcernError != nil {
			_, ok := transientErrorCodes[wEx.WriteConcernError.Code]

			return ok
		}
	}

	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) {
		_, ok := transientErrorCodes[int(cmdErr.Code)]

		return ok
	}

	return false
}
