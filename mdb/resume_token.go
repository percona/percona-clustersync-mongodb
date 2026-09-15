package mdb

import (
	"encoding/binary"
	"encoding/hex"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// ResumeTokenTimestamp extracts the scanned timestamp from a change-stream
// resume token's KeyString. The remaining KeyString fields and _typeBits are
// irrelevant to the timestamp.
func ResumeTokenTimestamp(token bson.Raw) (bson.Timestamp, error) {
	data, ok := token.Lookup("_data").StringValueOK()
	if !ok {
		return bson.Timestamp{}, errors.New("resume token _data is missing or not a string")
	}

	keyString, err := hex.DecodeString(data)
	if err != nil {
		return bson.Timestamp{}, errors.Wrap(err, "decode resume token _data")
	}

	const (
		timestampType = 0x82
		timestampSize = 9 // CType byte followed by two big-endian uint32 values.
	)
	if len(keyString) < timestampSize {
		return bson.Timestamp{}, errors.New("resume token KeyString is shorter than a timestamp")
	}
	if keyString[0] != timestampType {
		return bson.Timestamp{}, errors.New("resume token KeyString does not start with a timestamp")
	}

	return bson.Timestamp{
		T: binary.BigEndian.Uint32(keyString[1:5]),
		I: binary.BigEndian.Uint32(keyString[5:9]),
	}, nil
}
