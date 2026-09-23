package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/event-time-stream-runtime-go/internal/checkpoint"
	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
	"github.com/lisama569/event-time-stream-runtime-go/internal/window"
)

// idemSink dedupes published results by (key,start), modelling an idempotent
// external sink, and records every delivery attempt.
type idemSink struct {
	mu        sync.Mutex
	delivered []window.Result
	attempts  []window.Result
}

func (s *idemSink) Publish(_ context.Context, r window.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts = append(s.attempts, r)
	for _, d := range s.delivered {
		if d.Key == r.Key && d.WindowStart == r.WindowStart {
			return nil // idempotent no-op
		}
	}
	s.delivered = append(s.delivered, r)
	return nil
}

func (s *idemSink) Delivered() []window.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := append([]window.Result(nil), s.delivered...)
	sortResultsTest(out)
	return out
}

func (s *idemSink) AttemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.attempts)
}

func sortResultsTest(r []window.Result) {
	sort.Slice(r, func(i, j int) bool {
		if r[i].WindowStart != r[j].WindowStart {
			return r[i].WindowStart < r[j].WindowStart
		}
		return r[i].Key < r[j].Key
	})
}

func testCfg() Config {
	c := DefaultConfig()
	c.EngineID = "test-engine"
	c.WindowSize = 10 * time.Second
	c.AllowedLateness = 0
	c.OutOfOrderBound = 0
	c.CheckpointInterval = 25 * time.Millisecond
	return c
}

func line(id, key string, tsSec, val int64) string {
	const base = int64(1790157600000)
	return fmt.Sprintf(`{"id":%q,"key":%q,"ts":%d,"value":%d}`, id, key, base+tsSec*1000, val)
}

func runEngine(t *testing.T, cfg Config, dir string, streams map[string][]string, sink Sink, hook CrashHook) Stats {
	t.Helper()
	cp, err := checkpoint.NewStore(dir, cfg.EngineID)
	if err != nil {
		t.Fatal(err)
	}
	eng, err := New(cfg, cfg.LineSources(streams), cp, sink, nil)
	if err != nil {
		t.Fatal(err)
	}
	if hook != nil {
		eng.WithCrashHook(hook)
	}
	st, err := eng.Run(context.Background())
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	return st
}

func TestEndToEndWindowingDeterministic(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	streams := map[string][]string{
		"s": {
			line("a1", "k1", 1, 1),
			line("a3", "k1", 3, 2),
			line("a2", "k1", 2, 4), // out of order in [0,10)
			line("b1", "k2", 5, 7),
			line("a4", "k1", 12, 8),
			line("a6", "k1", 11, 1), // out of order in [10,20)
			line("a7", "k1", 14, 1),
		},
	}
	sink := &idemSink{}
	st := runEngine(t, cfg, dir, streams, sink, nil)

	if st.Processed != 7 || st.LateDropped != 0 {
		t.Fatalf("stats: %+v", st)
	}
	got := sink.Delivered()
	want := []window.Result{
		{Key: "k1", WindowStart: baseMs(0), WindowEnd: baseMs(10), Count: 3, Sum: 7},
		{Key: "k2", WindowStart: baseMs(0), WindowEnd: baseMs(10), Count: 1, Sum: 7},
		{Key: "k1", WindowStart: baseMs(10), WindowEnd: baseMs(20), Count: 3, Sum: 10},
	}
	assertResults(t, got, want)
}

func baseMs(sec int64) int64 {
	return int64(1790157600000) + sec*1000
}

func assertResults(t *testing.T, got, want []window.Result) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d results %+v, want %d %+v", len(got), got, len(want), want)
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Key != w.Key || g.WindowStart != w.WindowStart || g.WindowEnd != w.WindowEnd ||
			g.Count != w.Count || g.Sum != w.Sum {
			t.Errorf("result[%d] = {key=%s [%d,%d) count=%d sum=%d}, want {key=%s [%d,%d) count=%d sum=%d}",
				i, g.Key, g.WindowStart, g.WindowEnd, g.Count, g.Sum,
				w.Key, w.WindowStart, w.WindowEnd, w.Count, w.Sum)
		}
	}
}

func TestLateAndRejectionClassification(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.MinEventTime = time.UnixMilli(baseMs(-60))
	cfg.MaxEventTime = time.UnixMilli(baseMs(3600))
	var rejects []event.Rejected
	var rejMu sync.Mutex

	cp, _ := checkpoint.NewStore(dir, cfg.EngineID)
	streams := map[string][]string{"s": {
		line("a1", "k1", 1, 1),
		line("a2", "k1", 12, 2),
		line("jump", "k1", 40, 1),   // watermark jumps to 40
		line("late1", "k1", 9, 999), // [0,10) finalized -> late_dropped
		line("late2", "k1", 19, 999),
		`{"id":"bad","key":"k1","ts":"nope","value":1}`,
		`{"id":"span","key":"k1","ts":99999999999999,"value":1}`,
		`{broken`,
	}}
	eng, err := New(cfg, cfg.LineSources(streams), cp, &idemSink{}, func(r event.Rejected) {
		rejMu.Lock()
		rejects = append(rejects, r)
		rejMu.Unlock()
	})
	if err != nil {
		t.Fatal(err)
	}
	st, err := eng.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.LateDropped != 2 {
		t.Fatalf("late_dropped=%d want 2", st.LateDropped)
	}
	kinds := map[event.RejectReason]int{}
	for _, r := range rejects {
		kinds[r.Reason]++
	}
	if kinds["late_dropped"] != 2 || kinds[event.RejectBadTime] != 1 ||
		kinds[event.RejectOutOfSpan] != 1 || kinds[event.RejectMalformed] != 1 {
		t.Fatalf("reject classification mismatch: %+v %+v", kinds, rejects)
	}
	// explainability: every late rejection carries event time, watermark and basis
	for _, r := range rejects {
		if r.Reason == "late_dropped" {
			if !contains(r.Detail, "eventTime=") || !contains(r.Detail, "watermark=") ||
				!contains(r.Detail, "basis=") {
				t.Fatalf("late rejection not explainable: %s", r.Detail)
			}
		}
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

func TestDuplicateRedeliveryAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.CheckpointInterval = 30 * time.Millisecond
	all := []string{
		line("a1", "k1", 1, 1),
		line("a2", "k1", 2, 2),
		line("a3", "k1", 3, 3),
		line("a4", "k1", 12, 4),
		line("a5", "k1", 13, 5),
		line("a6", "k1", 22, 6),
	}
	sink := &idemSink{}

	// run 1: stop (simulated crash, no trailing cp) after 3 envelopes
	cfg1 := cfg
	cfg1.MaxEvents = 3
	st1 := runEngine(t, cfg1, dir, map[string][]string{"s": all}, sink, nil)
	if st1.Processed != 3 {
		t.Fatalf("run1 processed=%d", st1.Processed)
	}
	// run 2: restart, replay from last checkpoint; windows must not double-count
	st2 := runEngine(t, cfg, dir, map[string][]string{"s": all}, sink, nil)
	if st2.Processed < 3 {
		t.Fatalf("run2 processed=%d, expected replay + tail", st2.Processed)
	}
	got := sink.Delivered()
	want := []window.Result{
		{Key: "k1", WindowStart: baseMs(0), WindowEnd: baseMs(10), Count: 3, Sum: 6},
		{Key: "k1", WindowStart: baseMs(10), WindowEnd: baseMs(20), Count: 2, Sum: 9},
		{Key: "k1", WindowStart: baseMs(20), WindowEnd: baseMs(30), Count: 1, Sum: 6},
	}
	assertResults(t, got, want)

	// run 3: another full restart with no new input -> identical, no new publishes
	st3 := runEngine(t, cfg, dir, map[string][]string{"s": all}, sink, nil)
	if st3.Published != 0 {
		t.Fatalf("idle restart published=%d, want 0", st3.Published)
	}
}

// TestRejectCorruptCheckpointOnStartup: a bad/version-mismatched checkpoint
// must refuse recovery with a distinguishable cause; no results published.
func TestRejectCorruptCheckpointOnStartup(t *testing.T) {
	cases := []struct {
		name     string
		mutate   func(b []byte) []byte
		wantKind string
	}{
		{"bad_magic", func(b []byte) []byte { b[0] = 'X'; return b }, "bad_magic"},
		{"bad_crc", func(b []byte) []byte { b[len(b)-1] ^= 0xFF; return b }, "bad_crc"},
		{"bad_version", func(b []byte) []byte { b[4] = 231; b[5] = 3; return b }, "bad_version"},
		{"truncated", func(b []byte) []byte { return b[:9] }, "bad_payload"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := testCfg()
			// create a valid checkpoint by running once
			runEngine(t, cfg, dir,
				map[string][]string{"s": {line("a1", "k1", 1, 1)}},
				&idemSink{}, nil)

			path := dir + "/checkpoint.bin"
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, tc.mutate(b), 0o600); err != nil {
				t.Fatal(err)
			}

			cp, _ := checkpoint.NewStore(dir, cfg.EngineID)
			eng, err := New(cfg, cfg.LineSources(
				map[string][]string{"s": {line("a1", "k1", 1, 1)}}),
				cp, &idemSink{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = eng.Run(context.Background())
			if err == nil {
				t.Fatal("expected recovery refusal, got nil error")
			}
			var ce *checkpoint.CorruptionError
			if !errors.As(err, &ce) {
				t.Fatalf("want CorruptionError, got %v", err)
			}
			if ce.Kind != tc.wantKind {
				t.Fatalf("checkpoint version=%d kind=%s want %s basis=%s",
					ce.Version, ce.Kind, tc.wantKind, ce.Detail)
			}
		})
	}
}

// TestCrashAfterCheckpointBeforePublish simulates crash after a checkpoint is
// durable but before its pending results reach the sink: restart must publish
// them exactly once (idempotent sink), and later windows must be unaffected.
func TestCrashAfterCheckpointBeforePublish(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	cfg.CheckpointInterval = 15 * time.Millisecond
	all := []string{
		line("a1", "k1", 1, 1),
		line("a2", "k1", 2, 2),
		line("a3", "k1", 12, 3),
		line("a4", "k1", 22, 4),
	}
	sink := &idemSink{}

	crashed := false
	hook := func(seq int64, pending []window.Result) {
		if !crashed && len(pending) > 0 {
			crashed = true
			panic("simulated crash after durable checkpoint, before publish")
		}
	}

	// run 1: panic in the hook simulates process death
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("expected simulated crash panic")
			}
		}()
		runEngine(t, cfg, dir, map[string][]string{"s": all}, sink, hook)
	}()
	if sink.AttemptCount() != 0 {
		t.Fatalf("nothing should be published before crash, attempts=%d", sink.AttemptCount())
	}

	// run 2: recovery replays durable pending (idempotent publish) + the tail
	st := runEngine(t, cfg, dir, map[string][]string{"s": all}, sink, nil)
	_ = st
	got := sink.Delivered()
	want := []window.Result{
		{Key: "k1", WindowStart: baseMs(0), WindowEnd: baseMs(10), Count: 2, Sum: 3},
		{Key: "k1", WindowStart: baseMs(10), WindowEnd: baseMs(20), Count: 1, Sum: 3},
		{Key: "k1", WindowStart: baseMs(20), WindowEnd: baseMs(30), Count: 1, Sum: 4},
	}
	assertResults(t, got, want)
}

// TestConcurrentIngestionResultSetInvariant fans many sources in concurrently
// and asserts the final result SET is identical across repeated runs, even
// though interleaving (and thus emission order/counts in one no-checkpoint
// scenario) varies. Single-key inputs would be order-sensitive, so each
// source writes its own key, making the aggregate set scheduling-independent.
func TestConcurrentIngestionResultSetInvariant(t *testing.T) {
	cfg := testCfg()
	cfg.CheckpointInterval = 5 * time.Millisecond
	const sources = 8
	const perSource = 25

	buildStreams := func() map[string][]string {
		m := map[string][]string{}
		for i := 0; i < sources; i++ {
			key := fmt.Sprintf("k%d", i)
			lines := make([]string, perSource)
			for j := 0; j < perSource; j++ {
				lines[j] = line(fmt.Sprintf("e-%d-%d", i, j), key, int64(j), 1)
			}
			m[fmt.Sprintf("src%d", i)] = lines
		}
		return m
	}

	runOnce := func() []window.Result {
		dir := t.TempDir()
		sink := &idemSink{}
		st := runEngine(t, cfg, dir, buildStreams(), sink, nil)
		if st.Rejected != 0 || st.LateDropped != 0 {
			t.Fatalf("unexpected rejects/late: %+v", st)
		}
		return sink.Delivered()
	}

	first := runOnce()
	for iter := 0; iter < 5; iter++ {
		got := runOnce()
		if len(got) != len(first) {
			t.Fatalf("iter %d result count %d != %d", iter, len(got), len(first))
		}
		// each source spans windows [0,10),[10,20),[20,30): counts 10/10/5
		for i, r := range got {
			w := first[i]
			if r.Key != w.Key || r.WindowStart != w.WindowStart || r.Count != w.Count || r.Sum != w.Sum {
				t.Fatalf("iter %d result[%d]=%+v want %+v (eventTime=%d basis=concurrent-interleave)",
					iter, i, r, w, r.WindowStart)
			}
		}
	}
	// spot-check the 10/10/5 distribution for source 0
	var c0 []window.Result
	for _, r := range first {
		if r.Key == "k0" {
			c0 = append(c0, r)
		}
	}
	if len(c0) != 3 || c0[0].Count != 10 || c0[1].Count != 10 || c0[2].Count != 5 {
		t.Fatalf("k0 distribution: %+v", c0)
	}
}

// TestRejectIncompatibleEngineID: a checkpoint written by a different engine
// identity is an incompatible checkpoint and must be refused with details.
func TestRejectIncompatibleEngineID(t *testing.T) {
	dir := t.TempDir()
	cfg := testCfg()
	runEngine(t, cfg, dir,
		map[string][]string{"s": {line("a1", "k1", 1, 1)}}, &idemSink{}, nil)

	other := testCfg()
	other.EngineID = "a-totally-different-engine"
	cp, _ := checkpoint.NewStore(dir, other.EngineID)
	eng, err := New(other, other.LineSources(
		map[string][]string{"s": {line("a1", "k1", 1, 1)}}),
		cp, &idemSink{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = eng.Run(context.Background())
	var ce *checkpoint.CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want CorruptionError for engine mismatch, got %v", err)
	}
	if ce.Version != checkpoint.CurrentVersion {
		t.Fatalf("checkpoint version=%d want %d", ce.Version, checkpoint.CurrentVersion)
	}
	if !contains(ce.Detail, "engine id mismatch") {
		t.Fatalf("rejection must explain basis: %s", ce.Detail)
	}
}
