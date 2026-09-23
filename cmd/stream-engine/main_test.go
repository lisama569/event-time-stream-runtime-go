package main

import (
	"bytes"
	"context"
	"path/filepath"
	"strings"
	"testing"
)

// TestDemoCrashAndRecovery drives the actual main entrypoint through cold start
// -> simulated crash (no trailing checkpoint) -> restart, and verifies the
// restarted run emits the full deterministic result set with zero duplicates.
func TestDemoCrashAndRecovery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cp")

	out1 := &bytes.Buffer{}
	code := runWithWriter(context.Background(),
		[]string{"demo", "--dir", dir, "--crash-after", "4", "--checkpoint-every", "5ms"}, out1)
	if code != 0 {
		t.Fatalf("crash-run exit=%d", code)
	}
	if !strings.Contains(out1.String(), "SIMULATED CRASH") {
		t.Fatalf("missing simulated-crash marker:\n%s", out1.String())
	}
	if strings.Contains(out1.String(), "RESULT") {
		t.Fatalf("no window result should be published after only 4 envelopes")
	}

	out2 := &bytes.Buffer{}
	code = runWithWriter(context.Background(),
		[]string{"demo", "--restart", "--dir", dir, "--checkpoint-every", "5ms"}, out2)
	if code != 0 {
		t.Fatalf("restart exit=%d", code)
	}
	body := out2.String()
	// Deterministic expected aggregates from the demo narrative.
	for _, want := range []string{
		"RESULT key=k1 window=[1790157600000,1790157610000) count=3 sum=7",
		"RESULT key=k2 window=[1790157600000,1790157610000) count=1 sum=7",
		"RESULT key=k1 window=[1790157610000,1790157620000) count=3 sum=10",
		"RESULT key=k2 window=[1790157610000,1790157620000) count=1 sum=20",
		"RESULT key=k3 window=[1790157630000,1790157640000) count=1 sum=3",
		"reason=late_dropped",
		"reason=malformed",
		"reason=bad_time",
		"reason=out_of_span",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("restart output missing %q\nfull:\n%s", want, body)
		}
	}
	// Each result line must appear exactly once (no duplicates after recovery).
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, "RESULT") {
			continue
		}
		if c := strings.Count(body, line); c != 1 {
			t.Fatalf("result published %d times after recovery: %s", c, line)
		}
	}

	// Third run: everything checkpointed and Pending cleared -> no RESULTS.
	out3 := &bytes.Buffer{}
	_ = runWithWriter(context.Background(),
		[]string{"demo", "--restart", "--dir", dir}, out3)
	if strings.Contains(out3.String(), "RESULT") {
		t.Fatalf("idle restart must not republish:\n%s", out3.String())
	}
}
