package topo

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-mongolink/errors"
)

type ServerVersion [4]uint8

func (v ServerVersion) Major() int {
	return int(v[0])
}

func (v ServerVersion) Minor() int {
	return int(v[1])
}

func (v ServerVersion) Patch() int {
	return int(v[2])
}

func (v ServerVersion) Build() int {
	return int(v[3])
}

func (v ServerVersion) IsPSMDB() bool {
	return v.Build() != 0
}

func (v ServerVersion) String() string {
	if v[3] == 0 {
		return fmt.Sprintf("%d.%d.%d", v[0], v[1], v[2])
	}

	return fmt.Sprintf("%d.%d.%d-%d", v[0], v[1], v[2], v[3])
}

func (v ServerVersion) FullString() string {
	if v[3] == 0 {
		return fmt.Sprintf("MongoDB %d.%d.%d", v[0], v[1], v[2])
	}

	return fmt.Sprintf("Percona Server for MongoDB %d.%d.%d-%d", v[0], v[1], v[2], v[3])
}

func Version(ctx context.Context, m *mongo.Client) (ServerVersion, error) {
	raw, err := m.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}).Raw()
	if err != nil {
		return ServerVersion{}, errors.Wrap(err, "$buildInfo")
	}

	return parseServerVersion(raw.Lookup("version").StringValue())
}

type InvalidVersionError struct {
	value string
}

func (e InvalidVersionError) Error() string {
	return "invalid version: " + e.value
}

// parseServerVersion parse buildInfo version into ServerVersion.
// accaptable pattern: MAJOR.MINOR.PATCH[-BUILD].
func parseServerVersion(s string) (ServerVersion, error) {
	var version ServerVersion

	server, build, hasBuildVersion := strings.Cut(s, "-")
	parts := strings.SplitN(server, ".", 3) //nolint:mnd

	if len(parts) != 3 { //nolint:mnd
		return version, InvalidVersionError{s}
	}

	for i, p := range parts {
		if p == "" {
			return version, InvalidVersionError{s}
		}

		n, err := strconv.Atoi(p)
		if err != nil || n < 0 || n > math.MaxUint8 {
			return version, InvalidVersionError{s}
		}

		version[i] = uint8(n)
	}

	if hasBuildVersion {
		n, err := strconv.Atoi(build)
		if err != nil || n < 0 || n > math.MaxUint8 {
			return version, InvalidVersionError{s}
		}

		version[3] = uint8(n)
	}

	return version, nil
}

type Support ServerVersion

func (s Support) ClientBulkWrite() bool {
	return ServerVersion(s).Major() >= 8 //nolint:mnd
}
