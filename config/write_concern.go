package config

import (
	"strconv"

	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// ParseTargetWriteConcern accepts majority or a positive decimal w up to int32.
// Empty input preserves the majority default for legacy checkpoints.
func ParseTargetWriteConcern(value string) (*writeconcern.WriteConcern, error) {
	if value == "" || value == "majority" {
		return writeconcern.Majority(), nil
	}

	for _, digit := range value {
		if digit < '0' || digit > '9' {
			return nil, errors.Errorf("invalid target write concern %q: expected majority or a positive int32", value)
		}
	}

	w, err := strconv.ParseUint(value, 10, 31)
	if err != nil || w == 0 {
		return nil, errors.Errorf("invalid target write concern %q: expected majority or a positive int32", value)
	}

	return &writeconcern.WriteConcern{W: int(w)}, nil
}
