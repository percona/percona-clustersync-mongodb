package catalog //nolint:testpackage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexCatalogEntryUnsuccessfulType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		entry indexCatalogEntry
		want  IndexUnsuccessfulType
	}{
		{name: "failed", entry: indexCatalogEntry{Failed: true}, want: IndexFailed},
		{name: "incomplete", entry: indexCatalogEntry{Incomplete: true}, want: IndexIncomplete},
		{name: "inconsistent", entry: indexCatalogEntry{Inconsistent: true}, want: IndexInconsistent},
		{name: "successful", entry: indexCatalogEntry{}, want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, tt.entry.unsuccessfulType())
		})
	}
}
