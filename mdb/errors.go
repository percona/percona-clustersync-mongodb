package mdb

import (
	"context"
	"slices"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/errors"
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

// IsInvalidOptions checks if an error is an invalid options error (e.g. collMod on
// a non-existent or incompatible collection).
func IsInvalidOptions(err error) bool {
	return isMongoCommandError(err, "InvalidOptions")
}

func IsNamespaceExists(err error) bool {
	return isMongoCommandError(err, "NamespaceExists")
}

// IsCollectionDropped checks if the error is caused by a collection being dropped.
func IsCollectionDropped(err error) bool {
	var cmdErr mongo.CommandError
	if errors.As(err, &cmdErr) && cmdErr.Name == "QueryPlanKilled" {
		return strings.Contains(cmdErr.Message, "collection dropped") ||
			strings.Contains(cmdErr.Message, "index '_id_' dropped")
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

// IsDatabaseDropPending checks if an error is caused by a database drop pending state.
// MongoDB returns this error (code 357) when an operation targets a database
// that is in the process of being dropped.
func IsDatabaseDropPending(err error) bool {
	return isMongoCommandError(err, "DatabaseDropPending")
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
// Context cancellation is never transient — it signals intentional shutdown.
func IsTransient(err error) bool {
	if err == nil {
		return false
	}

	if errors.Is(err, context.Canceled) {
		return false
	}

	if mongo.IsNetworkError(err) || mongo.IsTimeout(err) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var le mongo.LabeledError
	if errors.As(err, &le) && le.HasErrorLabel("RetryableWriteError") {
		return true
	}

	var re interface{ Retryable() bool }
	if errors.As(err, &re) && re.Retryable() {
		return true
	}

	if isAuthKeyNotFound(err) {
		return true
	}

	return hasTransientServerError(err)
}

func hasTransientServerError(err error) bool {
	var serverErr mongo.ServerError
	if !errors.As(err, &serverErr) {
		return false
	}

	return slices.ContainsFunc(serverErr.ErrorCodes(), isTransientServerCode)
}

func isTransientServerCode(code int) bool {
	const (
		interruptedDueToReplStateChangeCode = 11602
		shutdownInProgressCode              = 91
		primarySteppedDownCode              = 189
		notWritablePrimaryCode              = 10107
		notPrimaryNoSecondaryOkCode         = 13435
	)

	switch code {
	case interruptedDueToReplStateChangeCode,
		shutdownInProgressCode,
		primarySteppedDownCode,
		notWritablePrimaryCode,
		notPrimaryNoSecondaryOkCode:
		return true
	default:
		return false
	}
}

func isAuthKeyNotFound(err error) bool {
	return strings.Contains(err.Error(), "KeyNotFound")
}
