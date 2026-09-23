package watermark

import "testing"

func TestStartsAtNegInf(t *testing.T) {
	tr := NewTracker(1000)
	tr.Init("a")
	tr.Init("b")
	if w := tr.Watermark(); w != NegativeInfinity {
		t.Fatalf("initial watermark = %d, want -inf", w)
	}
	tr.Observe("a", 5000)
	if w := tr.Watermark(); w != NegativeInfinity {
		t.Fatalf("watermark with silent source b = %d, want pinned -inf", w)
	}
	tr.Observe("b", 9000)
	if w := tr.Watermark(); w != 4000 { // min(5000-1000, 9000-1000)
		t.Fatalf("watermark = %d, want 4000", w)
	}
}

func TestMonotonicAndLag(t *testing.T) {
	tr := NewTracker(2000)
	tr.Init("a")
	tr.Observe("a", 10_000)
	if w := tr.Watermark(); w != 8000 {
		t.Fatalf("wm=%d want 8000", w)
	}
	tr.Observe("a", 9_000) // older event must not lower the max
	if w := tr.Watermark(); w != 8000 {
		t.Fatalf("wm regressed to %d", w)
	}
}

func TestIdleExcludedButProducedCounts(t *testing.T) {
	tr := NewTracker(0)
	tr.Init("a")
	tr.Init("b")
	tr.Observe("a", 1000)
	tr.Observe("b", 5000)
	if w := tr.Watermark(); w != 1000 {
		t.Fatalf("wm=%d want 1000", w)
	}
	tr.MarkIdle("a")
	if w := tr.Watermark(); w != 5000 {
		t.Fatalf("idle a should be excluded, wm=%d want 5000", w)
	}
	tr.Observe("a", 2000) // active again
	if w := tr.Watermark(); w != 2000 {
		t.Fatalf("reactivated a wm=%d want 2000", w)
	}
}

func TestSnapshotRestore(t *testing.T) {
	tr := NewTracker(0)
	tr.Init("a")
	tr.Observe("a", 7777)
	snap := tr.Snapshot()
	tr2 := NewTracker(0)
	tr2.Restore(snap)
	if w := tr2.Watermark(); w != 7777 {
		t.Fatalf("restored wm=%d want 7777", w)
	}
}
