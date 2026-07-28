package catalog //nolint:testpackage

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
	"github.com/percona/percona-clustersync-mongodb/mdb"
)

const testFinalizeIndexName = "email_1"

func TestDecideFinalizeUnsuccessfulIndex(t *testing.T) {
	t.Parallel()

	stored := testIndexSpec(t)
	changed := testIndexSpec(t)
	changed.Hidden = new(false)
	sourceErr := errors.New("source lookup failed")

	tests := []struct {
		name string
		in   finalizeIndexDecisionInput
		want finalizeIndexDecision
	}{
		{
			name: "recreates incomplete index when conditions cleared and source spec matches",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				sourceSpec:   testIndexSpec(t),
				originalType: IndexIncomplete,
			},
			want: finalizeIndexDecision{recreate: true},
		},
		{
			name: "reports incomplete index still building on source",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				originalType: IndexIncomplete,
				inProgress:   true,
			},
			want: finalizeIndexDecision{reportType: IndexIncomplete, reason: finalizeReasonSourceIndexBuilding},
		},
		{
			name: "reports inconsistent index still inconsistent on source",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				originalType: IndexInconsistent,
				inconsistent: true,
			},
			want: finalizeIndexDecision{reportType: IndexInconsistent, reason: finalizeReasonSourceIndexInconsistent},
		},
		{
			name: "reports incomplete index that became inconsistent on source",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				originalType: IndexIncomplete,
				inconsistent: true,
			},
			want: finalizeIndexDecision{reportType: IndexInconsistent, reason: finalizeReasonSourceIndexInconsistent},
		},
		{
			name: "reports inconsistent index now building on source",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				originalType: IndexInconsistent,
				inProgress:   true,
			},
			want: finalizeIndexDecision{reportType: IndexIncomplete, reason: finalizeReasonSourceIndexBuilding},
		},
		{
			name: "reports dropped incomplete unique index as no longer present",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				originalType: IndexIncomplete,
			},
			want: finalizeIndexDecision{reportType: IndexIncomplete, reason: finalizeReasonNoLongerPresent},
		},
		{
			name: "reports dropped formerly inconsistent index as no longer present",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				originalType: IndexInconsistent,
			},
			want: finalizeIndexDecision{reportType: IndexInconsistent, reason: finalizeReasonNoLongerPresent},
		},
		{
			name: "reports source recheck error with original type",
			in: finalizeIndexDecisionInput{
				storedSpec:     stored,
				sourceCheckErr: sourceErr,
				originalType:   IndexIncomplete,
			},
			want: finalizeIndexDecision{reportType: IndexIncomplete, reason: sourceErr.Error()},
		},
		{
			name: "recreates failed index without source recheck",
			in: finalizeIndexDecisionInput{
				storedSpec:     stored,
				sourceCheckErr: sourceErr,
				originalType:   IndexFailed,
			},
			want: finalizeIndexDecision{recreate: true},
		},
		{
			name: "reports changed source spec",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				sourceSpec:   changed,
				originalType: IndexIncomplete,
			},
			want: finalizeIndexDecision{reportType: IndexIncomplete, reason: finalizeReasonSourceSpecChanged},
		},
		{
			name: "recreates inconsistent index when replica set has no inconsistent state and source spec matches",
			in: finalizeIndexDecisionInput{
				storedSpec:   stored,
				sourceSpec:   testIndexSpec(t),
				originalType: IndexInconsistent,
			},
			want: finalizeIndexDecision{recreate: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := decideFinalizeUnsuccessfulIndex(tt.in)

			require.Equal(t, tt.want, got)
		})
	}
}

func TestIndexCreateSpecsEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(*mdb.IndexSpecification)
		equal  bool
	}{
		{
			name: "ignores server managed and non create fields",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.Namespace = "other.users"
				spec.Version = 99
				spec.Clustered = new(true)
			},
			equal: true,
		},
		{
			name: "detects key pattern change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.KeysDocument = mustTestRaw(t, bson.D{{Key: "email", Value: -1}})
			},
		},
		{
			name: "detects uniqueness option change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.Unique = new(false)
			},
		},
		{
			name: "detects partial filter change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.PartialFilterExpression = bson.D{{Key: "active", Value: false}}
			},
		},
		{
			name: "detects collation change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.Collation = mustTestRaw(t, bson.D{{Key: "simple", Value: "en"}})
			},
		},
		{
			name: "detects text option change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.TextVersion = new(int32(4))
			},
		},
		{
			name: "detects wildcard projection change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.WildcardProjection = bson.D{{Key: "profile.private", Value: 0}}
			},
		},
		{
			name: "detects geo option change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.Max = new(200.0)
			},
		},
		{
			name: "detects storage engine change",
			mutate: func(spec *mdb.IndexSpecification) {
				spec.StorageEngine = mustTestRaw(t, bson.D{{Key: "wiredTiger", Value: bson.D{}}})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			stored := testIndexSpec(t)
			source := testIndexSpec(t)
			tt.mutate(source)

			got := indexCreateSpecsEqual(stored, source)

			require.Equal(t, tt.equal, got)
		})
	}
}

// TestIndexCreateSpecsEqualFieldCoverage fails when a new IndexSpecification
// field is neither compared by indexCreateSpecsEqual nor deliberately ignored,
// so finalize cannot silently stop checking a field. Behavioral coverage lives
// in TestIndexCreateSpecsEqual.
func TestIndexCreateSpecsEqualFieldCoverage(t *testing.T) {
	t.Parallel()

	compared := map[string]bool{
		"KeysDocument":            true,
		"Sparse":                  true,
		"Hidden":                  true,
		"Unique":                  true,
		"PrepareUnique":           true,
		"ExpireAfterSeconds":      true,
		"Weights":                 true,
		"DefaultLanguage":         true,
		"LanguageOverride":        true,
		"TextVersion":             true,
		"Collation":               true,
		"StorageEngine":           true,
		"WildcardProjection":      true,
		"PartialFilterExpression": true,
		"Bits":                    true,
		"Min":                     true,
		"Max":                     true,
		"GeoIdxVer":               true,
	}
	ignored := map[string]bool{
		"Name":      true, // index identity, matched by name before comparison
		"Namespace": true, // server-managed (ns)
		"Version":   true, // server-managed (v)
		"Clustered": true, // collection property, not recreated via createIndexes
	}

	for _, f := range reflect.VisibleFields(reflect.TypeFor[mdb.IndexSpecification]()) {
		if !f.IsExported() {
			continue
		}

		switch {
		case compared[f.Name] && ignored[f.Name]:
			t.Errorf("IndexSpecification field %q classified as both compared and ignored", f.Name)
		case !compared[f.Name] && !ignored[f.Name]:
			t.Errorf("IndexSpecification field %q is unclassified: add it to indexCreateSpecsEqual "+
				"and the compared set, or to the ignored set", f.Name)
		}
	}
}

func testIndexSpec(t *testing.T) *mdb.IndexSpecification {
	t.Helper()

	return &mdb.IndexSpecification{
		Name:                    testFinalizeIndexName,
		Namespace:               "db.users",
		KeysDocument:            mustTestRaw(t, bson.D{{Key: "email", Value: 1}}),
		Version:                 2,
		Sparse:                  new(true),
		Hidden:                  new(true),
		Unique:                  new(true),
		ExpireAfterSeconds:      new(int64(3600)),
		Weights:                 bson.D{{Key: "email", Value: 1}},
		DefaultLanguage:         new("english"),
		LanguageOverride:        new("language"),
		TextVersion:             new(int32(3)),
		Collation:               mustTestRaw(t, bson.D{{Key: "simple", Value: "local"}}),
		WildcardProjection:      bson.D{{Key: "profile", Value: 1}},
		PartialFilterExpression: bson.D{{Key: "active", Value: true}},
		Bits:                    new(int32(26)),
		Min:                     new(-180.0),
		Max:                     new(180.0),
		GeoIdxVer:               new(int32(3)),
	}
}

func mustTestRaw(t *testing.T, doc bson.D) bson.Raw {
	t.Helper()

	raw, err := bson.Marshal(doc)
	require.NoError(t, err)

	return raw
}
