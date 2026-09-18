package mdb

import (
	"encoding/binary"
	"encoding/hex"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/percona/percona-clustersync-mongodb/errors"
)

// ResumeTokenTimestamp extracts the scanned timestamp from a change-stream
// resume token.
//
// A token's "_data" is a hex-encoded KeyString: the server's order-preserving
// byte encoding of the token fields. clusterTime is encoded first, so the
// timestamp is readable without decoding the rest:
//
//	byte 0     canonical BSON type of a timestamp: 130 (0x82)
//	bytes 1-4  seconds, big-endian uint32
//	bytes 5-8  increment, big-endian uint32
//
// Big-endian because KeyString compares bytewise, unlike BSON's little-endian
// integers. Trailing KeyString fields and "_typeBits" are irrelevant here.
//
// The layout is not driver API, so
// TestWatchChangeEvents_TickMatchesServerScannedFrontier decodes a real token
// from the server image under test and fails CI if it ever changes.
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
		canonicalTypeTimestamp = 0x82
		timestampSize          = 9 // Type byte followed by two big-endian uint32 values.
	)
	if len(keyString) < timestampSize {
		return bson.Timestamp{}, errors.New("resume token KeyString is shorter than a timestamp")
	}
	if keyString[0] != canonicalTypeTimestamp {
		return bson.Timestamp{}, errors.New("resume token KeyString does not start with a timestamp")
	}

	return bson.Timestamp{
		T: binary.BigEndian.Uint32(keyString[1:5]),
		I: binary.BigEndian.Uint32(keyString[5:9]),
	}, nil
}
