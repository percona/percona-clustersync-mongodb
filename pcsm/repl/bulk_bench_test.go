package repl //nolint:testpackage

import (
	"strconv"
	"strings"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func BenchmarkCollectUpdateOpsWithPipeline_LargeMixed(b *testing.B) {
	event := makeMixedBenchmarkUpdateEvent(220, 100, 12_000)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = collectUpdateOpsWithPipeline(event)
	}
}

func BenchmarkCollectUpdateOpsWithPipeline_NormalMixed(b *testing.B) {
	event := makeMixedBenchmarkUpdateEvent(20, 20, 200)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = collectUpdateOpsWithPipeline(event)
	}
}

func makeMixedBenchmarkUpdateEvent(arrayUpdates, nonArrayUpdates int, valueSize int) *UpdateEvent {
	updatedFields := make(bson.D, 0, arrayUpdates+nonArrayUpdates)
	for i := range arrayUpdates {
		updatedFields = append(updatedFields, bson.E{Key: "arr." + strconv.Itoa(i), Value: i})
	}

	val := strings.Repeat("x", valueSize)
	for i := range nonArrayUpdates {
		updatedFields = append(updatedFields, bson.E{Key: "meta.f_" + strconv.Itoa(i), Value: val})
	}

	return &UpdateEvent{
		UpdateDescription: UpdateDescription{
			TruncatedArrays: []struct {
				Field   string `bson:"field"`
				NewSize int32  `bson:"newSize"`
			}{
				{Field: "arr", NewSize: 20_000},
			},
			UpdatedFields: updatedFields,
			RemovedFields: []string{"legacy.field"},
		},
	}
}
