package mdb //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsSystemCollection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{name: "system.views", want: true},
		{name: "system.profile", want: true},
		{name: "system.buckets.weather", want: true},
		{name: "system.", want: true},
		{name: "system", want: false},
		{name: "systematic", want: false},
		{name: "users", want: false},
		{name: "my.system.views", want: false},
		{name: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isSystemCollection(tt.name))
		})
	}
}

func TestIsInternalDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{name: "admin", want: true},
		{name: "config", want: true},
		{name: "local", want: true},
		{name: "Admin", want: false},
		{name: "admin2", want: false},
		{name: "localhost", want: false},
		{name: "percona_clustersync_mongodb", want: false},
		{name: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, isInternalDatabase(tt.name))
		})
	}
}
