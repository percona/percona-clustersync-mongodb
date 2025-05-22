package topo

import (
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// IsRetryableWrite checks if the error has the "RetryableWriteError" label.
func IsRetryableWrite(err error) bool {
	for err != nil {
		le, ok := err.(mongo.LabeledError) //nolint:errorlint
		if ok {
			return le.HasErrorLabel("RetryableWriteError")
		}

		err = errors.Unwrap(err)
	}

	return false
}

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
