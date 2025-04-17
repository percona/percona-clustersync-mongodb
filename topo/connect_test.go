package topo //nolint:testpackage

import (
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSanitizeConnString(t *testing.T) {
	t.Parallel()

	const baseURI = "mongodb://usr:pass@mongo:27018"

	// realOptions is list of real options that are not in the allowed list.
	realOptions := []string{
		"directConnection",
		"connectTimeoutMS",
		"socketTimeoutMS",
		"compressors",
		"zlibCompressionLevel",
		"maxPoolSize",
		"minPoolSize",
		"maxConnecting",
		"maxIdleTimeMS",
		"waitQueueMultiple",
		"waitQueueTimeoutMS",
		"w",
		"wtimeoutMS",
		"journal",
		"readConcernLevel",
		"readPreference",
		"maxStalenessSeconds",
		"readPreferenceTags",
		"localThresholdMS",
		"serverSelectionTimeoutMS",
		"serverSelectionTryOnce",
		"heartbeatFrequencyMS",
		"socketCheckIntervalMS",
		"retryReads",
		"retryWrites",
		"uuidRepresentation",
		"loadBalanced",
		"srvMaxHosts",
	}

	t.Run("allowed included", func(t *testing.T) {
		t.Parallel()

		rv := make([]string, len(allowedConnStringOptions))
		for i, s := range allowedConnStringOptions {
			rv[i] = fmt.Sprintf("%s=%d", s, i)
		}

		uri := baseURI + "/admin?" + strings.Join(rv, "&")
		res, _ := sanitizeMongoURI(uri)
		assert.Equal(t, uri, res)
	})

	t.Run("allowed included with mixed case", func(t *testing.T) {
		t.Parallel()

		rv := make([]string, len(allowedConnStringOptions))
		for i, s := range allowedConnStringOptions {
			if i%2 == 0 {
				s = strings.ToUpper(s)
			} else {
				s = strings.ToLower(s)
			}

			rv[i] = fmt.Sprintf("%s=%d", s, i)
		}

		uri := baseURI + "/admin?" + strings.Join(rv, "&")
		res, _ := sanitizeMongoURI(uri)
		assert.Equal(t, uri, res)
	})

	t.Run("not allowed excluded", func(t *testing.T) {
		t.Parallel()

		rv := make([]string, len(realOptions))
		for i, s := range realOptions {
			rv[i] = fmt.Sprintf("%s=%d", s, i)
		}

		res, _ := sanitizeMongoURI(baseURI + "/?" + strings.Join(rv, "&"))
		assert.Equal(t, baseURI+"/?", res)
	})

	t.Run("not allowed with mixed case", func(t *testing.T) {
		t.Parallel()

		rv := make([]string, len(realOptions))
		for i, s := range realOptions {
			if i%2 == 0 {
				s = strings.ToUpper(s)
			} else {
				s = strings.ToLower(s)
			}

			rv[i] = fmt.Sprintf("%s=%d", s, i)
		}

		res, _ := sanitizeMongoURI(baseURI + "/?" + strings.Join(rv, "&"))
		assert.Equal(t, baseURI+"/?", res)
	})

	t.Run("allowed and not allowed", func(t *testing.T) {
		t.Parallel()

		var allowed []string

		all := make([]string, 20)
		for i := range all {
			if rand.IntN(2) == 0 { //nolint:gosec
				s := allowedConnStringOptions[rand.IntN(len(allowedConnStringOptions))] //nolint:gosec
				s = fmt.Sprintf("%s=%d", s, i)
				all[i] = s

				allowed = append(allowed, s)
			} else {
				s := realOptions[rand.IntN(len(realOptions))] //nolint:gosec
				all[i] = fmt.Sprintf("%s=%d", s, i)
			}
		}

		res, _ := sanitizeMongoURI(baseURI + "/?" + strings.Join(all, ";"))
		assert.Equal(t, baseURI+"/?"+strings.Join(allowed, "&"), res)
	})

	t.Run("no options", func(t *testing.T) {
		t.Parallel()

		res, _ := sanitizeMongoURI(baseURI + "/?")
		assert.Equal(t, baseURI+"/?", res)

		res, _ = sanitizeMongoURI(baseURI + "/")
		assert.Equal(t, baseURI+"/", res)

		res, _ = sanitizeMongoURI(baseURI)
		assert.Equal(t, baseURI, res)
	})
}
