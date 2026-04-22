package repl //nolint:testpackage

import "testing"

func TestOptionsApplyDefaults_FollowUpOverflowAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		input  string
		expect string
	}{
		{name: "empty defaults to fail", input: "", expect: followUpOverflowActionFail},
		{name: "warn accepted", input: "warn", expect: followUpOverflowActionWarn},
		{name: "case-insensitive", input: "WARN", expect: followUpOverflowActionWarn},
		{name: "invalid falls back to fail", input: "drop", expect: followUpOverflowActionFail},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			opts := Options{FollowUpOverflowAction: tt.input}
			opts.applyDefaults()

			if opts.FollowUpOverflowAction != tt.expect {
				t.Fatalf("FollowUpOverflowAction = %q, want %q", opts.FollowUpOverflowAction, tt.expect)
			}
		})
	}
}
