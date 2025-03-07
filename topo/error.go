package topo

import (
	"errors"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// IsIndexNotFound checks if an error is an index not found error.
func IsIndexNotFound(err error) bool {
	return isMongoCommandError(err, "IndexNotFound")
}

// IsIndexOptionsConflict checks if an error is an index options conflict error.
func IsIndexOptionsConflict(err error) bool {
	return isMongoCommandError(err, "IndexOptionsConflict")
}

// isMongoCommandError checks if an error is a MongoDB error with the specified name.
func isMongoCommandError(err error, name string) bool {
	for err != nil {
		le, ok := err.(mongo.CommandError) //nolint:errorlint
		if ok && le.Name == name {
			return true
		}

		err = errors.Unwrap(err)
	}

	return false
}
