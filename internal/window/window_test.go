package window

import (
	"testing"

	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
)

const size = int64(10_000)
const lateness = int64(5_000)

func ev(id, src, key string, ts, val, seq int64) event.Event {
	return event.Event{ID: id, SourceID: src, Key: key, Timestamp: ts, Value: val, Seq: seq}
}

func TestOutOfOrderWithinWindow(t *testing.T) {
	s := NewStore(size, lateness)
	wm := NegativeInfinityForTest()
	// events arrive out of timestamp order but inside the same window
	if r := s.Apply(ev("a", "s1", "k", 12_000, 1, 2), wm); r != ApplyAccepted {
		t.Fatalf("apply: %d", r)
	}
	if r := s.Apply(ev("b", "s1", "k", 11_000, 2, 3), wm); r != ApplyAccepted {
		t.Fatalf("apply ooo: %d", r)
	}
	st := s.Snapshot()
	if len(st.Windows) != 1 || st.Windows[0].Count != 2 || st.Windows[0].Sum != 3 {
		t.Fatalf("unexpected state: %+v", st.Windows)
	}
}

func NegativeInfinityForTest() int64 { return -1 << 62 }

func TestLateAfterCloseImmutable(t *testing.T) {
	s := NewStore(size, lateness)
	s.Apply(ev("a", "s1", "k", 1_000, 1, 0), NegativeInfinityForTest())
	// watermark at exactly window end closes it
	s.Advance(10_000)
	r := s.Apply(ev("late", "s1", "k", 9_000, 999, 1), 10_000)
	if r != ApplyLateDropped {
		t.Fatalf("closed-window apply = %d, want LateDropped", r)
	}
	// even within allowed lateness a closed window is immutable
	st := s.Snapshot()
	if st.Windows[0].Count != 1 || st.Windows[0].Sum != 1 {
		t.Fatalf("closed window mutated: %+v", st.Windows[0])
	}
}

func TestAllowedLatenessFinalizeBoundary(t *testing.T) {
	s := NewStore(size, lateness)
	s.Apply(ev("a", "s1", "k", 1_000, 1, 0), NegativeInfinityForTest())
	// An out-of-order event arrives while the watermark (lag-adjusted by the
	// engine) is still 2s behind real input: window open, event applies.
	s.Advance(8_000)
	if r := s.Apply(ev("b", "s1", "k", 2_000, 4, 1), 8_000); r != ApplyAccepted {
		t.Fatalf("late-but-open window apply = %d, want accepted", r)
	}
	// Once the watermark passes the window end, new late events are immutable.
	s.Advance(10_000)
	if r := s.Apply(ev("z", "s1", "k", 3_000, 9, 2), 10_000); r != ApplyLateDropped {
		t.Fatalf("post-close apply = %d, want LateDropped", r)
	}
	// finalize at end+allowedLateness; aggregate reflects only a,b
	out := s.PurgeOlder(15_000)
	if len(out) != 1 || out[0].Count != 2 || out[0].Sum != 5 {
		t.Fatalf("finalize: %+v", out)
	}
	// after purge any straggler is dropped as well
	if r := s.Apply(ev("c", "s1", "k", 4_000, 9, 3), 15_000); r != ApplyLateDropped {
		t.Fatalf("post-purge: %d", r)
	}
}

func TestDedupBySeqAndID(t *testing.T) {
	s := NewStore(size, lateness)
	wm := NegativeInfinityForTest()
	if r := s.Apply(ev("e1", "s1", "k", 1_000, 5, 7), wm); r != ApplyAccepted {
		t.Fatal(r)
	}
	// replay with same source position
	if r := s.Apply(ev("e1", "s1", "k", 1_000, 5, 7), wm); r != ApplyDuplicate {
		t.Fatalf("seq dup: %d", r)
	}
	// redelivery with a new position but the same business id
	if r := s.Apply(ev("e1", "s1", "k", 1_000, 5, 8), wm); r != ApplyDuplicate {
		t.Fatalf("id dup: %d", r)
	}
	st := s.Snapshot()
	if st.Windows[0].Count != 1 || st.Windows[0].Sum != 5 {
		t.Fatalf("dedup failed: %+v", st.Windows[0])
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	s := NewStore(size, lateness)
	wm := NegativeInfinityForTest()
	s.Apply(ev("e1", "s1", "k", 1_000, 5, 7), wm)
	s.Advance(10_000)
	snap := s.Snapshot()

	s2 := NewStore(size, lateness)
	s2.Restore(snap)
	// duplicate must remain suppressed after restore
	if r := s2.Apply(ev("e1", "s1", "k", 1_000, 5, 7), 10_000); r != ApplyDuplicate {
		t.Fatalf("post-restore dup: %d", r)
	}
	// late event still rejected
	if r := s2.Apply(ev("x", "s1", "k", 9_000, 1, 8), 10_000); r != ApplyLateDropped {
		t.Fatalf("post-restore late: %d", r)
	}
}

func TestTumblingWindowStart(t *testing.T) {
	cases := map[int64]int64{0: 0, 1: 0, 9999: 0, 10000: 10000, -1: -10000, -10000: -10000}
	for in, want := range cases {
		if got := WindowStart(in, size); got != want {
			t.Errorf("WindowStart(%d)=%d want %d", in, got, want)
		}
	}
}
