package clone

import (
	"runtime"
	"testing"
)

func TestCopyManagerOptionsApplyDefaults_AutoWorkersAreBounded(t *testing.T) {
	t.Parallel()

	opts := CopyManagerOptions{}
	opts.applyDefaults()

	wantRead := max(runtime.NumCPU()/4, 1)
	wantInsert := min(max(runtime.NumCPU(), 2), 16)

	if opts.NumReadWorkers != wantRead {
		t.Fatalf("NumReadWorkers = %d, want %d", opts.NumReadWorkers, wantRead)
	}

	if opts.NumInsertWorkers != wantInsert {
		t.Fatalf("NumInsertWorkers = %d, want %d", opts.NumInsertWorkers, wantInsert)
	}
}

func TestCopyManagerOptionsApplyDefaults_ExplicitWorkersArePreserved(t *testing.T) {
	t.Parallel()

	opts := CopyManagerOptions{NumReadWorkers: 3, NumInsertWorkers: 24}
	opts.applyDefaults()

	if opts.NumReadWorkers != 3 {
		t.Fatalf("NumReadWorkers = %d, want 3", opts.NumReadWorkers)
	}

	if opts.NumInsertWorkers != 24 {
		t.Fatalf("NumInsertWorkers = %d, want 24", opts.NumInsertWorkers)
	}
}
