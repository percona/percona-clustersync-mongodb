package mdb

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/percona/percona-clustersync-mongodb/errors"
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
	raw, err := RunWithRetryVal(ctx, func(ctx context.Context) (bson.Raw, error) {
		return m.Database("admin").RunCommand(ctx, bson.D{{"buildInfo", 1}}).Raw() //nolint:wrapcheck
	}, DefaultRetryInterval, DefaultMaxRetries)
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

// parseServerVersion parses buildInfo version into ServerVersion.
// Acceptable pattern: MAJOR.MINOR.PATCH[-BUILD].
// If BUILD cannot be parsed as a uint8, the suffix is discarded.
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
		if err == nil && n >= 0 && n <= math.MaxUint8 {
			version[3] = uint8(n)
		}
	}

	return version, nil
}

var ErrDowngrade = errors.New("downgrade not supported") //nolint:gochecknoglobals

// CheckVersionCompat validates source and target MongoDB major versions.
// Returns ErrDowngrade if source major > target major (downgrade).
// Returns true if source major < target major (lower-to-higher replication).
func CheckVersionCompat(source, target ServerVersion) (bool, error) {
	if source.Major() > target.Major() {
		return false, errors.Wrapf(ErrDowngrade, "source %s > target %s", source, target)
	}

	return source.Major() < target.Major(), nil
}

// FCV is a featureCompatibilityVersion value (MAJOR.MINOR).
type FCV struct {
	Major int
	Minor int
}

func (f FCV) String() string {
	return fmt.Sprintf("%d.%d", f.Major, f.Minor)
}

// Compare returns -1, 0 or 1 if f is lower than, equal to or higher than other.
// Components are compared numerically, so 8.10 is higher than 8.2.
func (f FCV) Compare(other FCV) int {
	if f.Major != other.Major {
		if f.Major < other.Major {
			return -1
		}

		return 1
	}

	switch {
	case f.Minor < other.Minor:
		return -1
	case f.Minor > other.Minor:
		return 1
	default:
		return 0
	}
}

// InvalidFCVError is returned when the featureCompatibilityVersion value
// cannot be parsed.
type InvalidFCVError struct {
	value string
}

func (e InvalidFCVError) Error() string {
	return "invalid featureCompatibilityVersion: " + e.value
}

// parseFCV parses a featureCompatibilityVersion string. Acceptable pattern: MAJOR.MINOR.
func parseFCV(s string) (FCV, error) {
	majorStr, minorStr, ok := strings.Cut(s, ".")
	if !ok {
		return FCV{}, InvalidFCVError{s}
	}

	major, err := strconv.Atoi(majorStr)
	if err != nil || major < 0 {
		return FCV{}, InvalidFCVError{s}
	}

	minor, err := strconv.Atoi(minorStr)
	if err != nil || minor < 0 {
		return FCV{}, InvalidFCVError{s}
	}

	return FCV{Major: major, Minor: minor}, nil
}

// FCVTransitionError is returned when the deployment is in the middle of an FCV
// upgrade or downgrade, so its effective FCV is not settled.
type FCVTransitionError struct {
	Version  string
	Target   string
	Previous string
}

func (e FCVTransitionError) Error() string {
	msg := "featureCompatibilityVersion change in progress (version " + e.Version
	if e.Target != "" {
		msg += ", targetVersion " + e.Target
	}

	if e.Previous != "" {
		msg += ", previousVersion " + e.Previous
	}

	return msg + "); wait for it to finish and retry"
}

// GetFCV returns the featureCompatibilityVersion of the deployment.
// Returns FCVTransitionError while an FCV upgrade or downgrade is in progress.
//
// mongos before 8.0 does not support the featureCompatibilityVersion
// getParameter option, so for it the FCV document is read from
// admin.system.version, which mongos routes to the config server.
func GetFCV(ctx context.Context, m *mongo.Client, isMongos bool, version ServerVersion) (FCV, error) {
	if fcvFromSystemVersionRequired(isMongos, version) {
		return fcvFromSystemVersion(ctx, m)
	}

	raw, err := RunWithRetryVal(ctx, func(ctx context.Context) (bson.Raw, error) {
		return m.Database("admin").RunCommand(ctx, bson.D{
			{"getParameter", 1},
			{"featureCompatibilityVersion", 1},
		}).Raw() //nolint:wrapcheck // raw for retry; wrapped below
	}, DefaultRetryInterval, DefaultMaxRetries)
	if err != nil {
		return FCV{}, errors.Wrap(err, "getParameter featureCompatibilityVersion")
	}

	return fcvFromRaw(raw)
}

// fcvFromSystemVersionRequired reports whether the FCV must be read from
// admin.system.version instead of getParameter: true for mongos before 8.0.
func fcvFromSystemVersionRequired(isMongos bool, version ServerVersion) bool {
	return isMongos && version.Major() < 8 //nolint:mnd // mongos getParameter FCV added in 8.0
}

// fcvFromSystemVersion reads the FCV document from admin.system.version.
func fcvFromSystemVersion(ctx context.Context, m *mongo.Client) (FCV, error) {
	doc, err := RunWithRetryVal(ctx, func(ctx context.Context) (bson.Raw, error) {
		return m.Database("admin").Collection("system.version").
			FindOne(ctx, bson.D{{"_id", "featureCompatibilityVersion"}}).Raw() //nolint:wrapcheck // raw for retry; wrapped below
	}, DefaultRetryInterval, DefaultMaxRetries)
	if err != nil {
		return FCV{}, errors.Wrap(err, "read featureCompatibilityVersion from admin.system.version")
	}

	return fcvFromDoc(doc)
}

// fcvFromRaw extracts the FCV from a getParameter response.
func fcvFromRaw(raw bson.Raw) (FCV, error) {
	val, err := raw.LookupErr("featureCompatibilityVersion")
	if err != nil {
		return FCV{}, errors.Wrap(err, "lookup featureCompatibilityVersion")
	}

	// 4.4+ returns a document {version: "X.Y"}; older servers return a string.
	s, ok := val.StringValueOK()
	if ok {
		return parseFCV(s)
	}

	doc, ok := val.DocumentOK()
	if !ok {
		return FCV{}, InvalidFCVError{val.String()}
	}

	return fcvFromDoc(doc)
}

// fcvFromDoc extracts the FCV from a {version, targetVersion?, previousVersion?}
// document, rejecting one that describes a transition in progress.
func fcvFromDoc(doc bson.Raw) (FCV, error) {
	s, ok := doc.Lookup("version").StringValueOK()
	if !ok {
		return FCV{}, InvalidFCVError{doc.String()}
	}

	target, err := optionalFCVField(doc, "targetVersion")
	if err != nil {
		return FCV{}, err
	}

	previous, err := optionalFCVField(doc, "previousVersion")
	if err != nil {
		return FCV{}, err
	}

	if target != "" || previous != "" {
		return FCV{}, FCVTransitionError{Version: s, Target: target, Previous: previous}
	}

	return parseFCV(s)
}

// optionalFCVField returns the value of a transition field of the FCV document,
// or "" when the field is absent. A present field must be a nonempty string,
// so a malformed document is never mistaken for a settled FCV.
func optionalFCVField(doc bson.Raw, key string) (string, error) {
	val := doc.Lookup(key)
	if val.IsZero() {
		return "", nil
	}

	s, ok := val.StringValueOK()
	if !ok || s == "" {
		return "", InvalidFCVError{doc.String()}
	}

	return s, nil
}

// FCVDowngradeError is returned when the source FCV is higher than the target FCV.
type FCVDowngradeError struct {
	Source FCV
	Target FCV
}

func (e FCVDowngradeError) Error() string {
	return fmt.Sprintf("source FCV %s is higher than target FCV %s; replication is not supported",
		e.Source, e.Target)
}

// CheckFCVCompat validates source and target featureCompatibilityVersion.
// Returns FCVDowngradeError if source FCV > target FCV.
func CheckFCVCompat(source, target FCV) error {
	if source.Compare(target) > 0 {
		return FCVDowngradeError{Source: source, Target: target}
	}

	return nil
}

type Support ServerVersion

func (s Support) ClientBulkWrite() bool {
	return ServerVersion(s).Major() >= 8 //nolint:mnd
}
