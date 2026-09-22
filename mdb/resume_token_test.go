package mdb_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/mdb"
)

func TestResumeTokenTimestamp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		token   bson.D
		want    bson.Timestamp
		wantErr bool
	}{
		{
			name:  "known timestamp with trailing KeyString data",
			token: bson.D{{"_data", "8263F2B1A3000000012B022C0100296E5A1004"}},
			want:  bson.Timestamp{T: 1676849571, I: 1}, // 0x63F2B1A3
		},
		{
			name:  "timestamp only with optional type bits",
			token: bson.D{{"_data", "8263f2b1a300000001"}, {"_typeBits", bson.Binary{Data: []byte{0}}}},
			want:  bson.Timestamp{T: 1676849571, I: 1},
		},
		{name: "missing data", token: bson.D{}, wantErr: true},
		{name: "non string data", token: bson.D{{"_data", 82}}, wantErr: true},
		{name: "invalid hex", token: bson.D{{"_data", "82nothex"}}, wantErr: true},
		{name: "odd length hex", token: bson.D{{"_data", "8263F2B1A3000000010"}}, wantErr: true},
		{name: "empty data", token: bson.D{{"_data", ""}}, wantErr: true},
		{name: "short data", token: bson.D{{"_data", "8263F2B1A3000000"}}, wantErr: true},
		{name: "wrong KeyString type", token: bson.D{{"_data", "8163F2B1A300000001"}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			raw, err := bson.Marshal(tt.token)
			require.NoError(t, err)
			got, err := mdb.ResumeTokenTimestamp(raw)
			if tt.wantErr {
				require.Error(t, err)
				assert.Zero(t, got)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}

	// A cursor that has not received a postBatchResumeToken yet returns a nil token.
	t.Run("nil token", func(t *testing.T) {
		t.Parallel()

		got, err := mdb.ResumeTokenTimestamp(nil)
		require.Error(t, err)
		assert.Zero(t, got)
	})
}
