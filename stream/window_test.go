package stream

import (
	"testing"
	"time"
)

func iso(t *testing.T, s string) time.Time {
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}

func ev(id, key, ts string, v float64) *Event {
	tm, _ := time.Parse(time.RFC3339, ts)
	return &Event{ID: id, Key: key, Timestamp: tm, Value: v, Source: "s", Offset: 0}
}

func TestWatermarkMinAcrossSources(t *testing.T) {
	w := NewWatermarkTracker()
	w.Register("a")
	w.Register("b")
	if g := w.Global(); !g.IsZero() {
		t.Fatalf("initial global must be zero, got %v", g)
	}
	w.Observe("a", iso(t, "2026-01-01T00:00:30Z"))
	w.Observe("b", iso(t, "2026-01-01T00:00:10Z"))
	if g := w.Global(); !g.Equal(iso(t, "2026-01-01T00:00:10Z")) {
		t.Fatalf("global = min: got %v", g)
	}
	w.Observe("b", iso(t, "2026-01-01T00:00:20Z"))
	if g := w.Global(); !g.Equal(iso(t, "2026-01-01T00:00:20Z")) {
		t.Fatalf("after advance got %v", g)
	}
	// 水位单调：旧事件不能拉回水位。
	w.Observe("b", iso(t, "2026-01-01T00:00:01Z"))
	if g := w.Global(); !g.Equal(iso(t, "2026-01-01T00:00:20Z")) {
		t.Fatalf("watermark regressed to %v", g)
	}
	// 慢源 EOF 后退出最小值计算。
	w.Finish("b")
	if g := w.Global(); !g.Equal(iso(t, "2026-01-01T00:00:30Z")) {
		t.Fatalf("after finish b, got %v", g)
	}
}

func TestWindowOutOfOrderWithinLateness(t *testing.T) {
	s := NewWindowStore(time.Minute, 10*time.Second)
	// 先到晚事件，再到早事件：必须落在同一窗口并正确聚合。
	s.Add(ev("late", "a", "2026-01-01T00:00:59Z", 9), iso(t, "2026-01-01T00:00:59Z"))
	s.Add(ev("early", "a", "2026-01-01T00:00:01Z", 1), iso(t, "2026-01-01T00:00:59Z"))
	id := WindowID{Key: "a", Start: iso(t, "2026-01-01T00:00:00Z")}
	// 水位 00:01:09 < 视界 00:01:10：窗口不可关。
	if got := s.Closeable(iso(t, "2026-01-01T00:01:09Z")); len(got) != 0 {
		t.Fatalf("window must stay open until end+lateness, got %v", got)
	}
	got := s.Closeable(iso(t, "2026-01-01T00:01:10Z"))
	if len(got) != 1 || got[0] != id {
		t.Fatalf("expected window closeable at horizon, got %v", got)
	}
	r, ok := s.Close(id, iso(t, "2026-01-01T00:01:10Z"),
		iso(t, "2026-01-01T00:01:10Z"), 1)
	if !ok {
		t.Fatal("close failed")
	}
	if r.Count != 2 || r.Sum != 10 || r.Min != 1 || r.Max != 9 {
		t.Fatalf("aggregation wrong: %+v", r)
	}
	if r.FirstEventID != "early" {
		t.Fatalf("first event must be order-independent deterministic, got %q", r.FirstEventID)
	}
	// 同一窗口只能关一次。
	if _, ok := s.Close(id, iso(t, "2026-01-01T00:01:10Z"),
		iso(t, "2026-01-01T00:01:10Z"), 1); ok {
		t.Fatal("double close must fail")
	}
}

func TestWindowLateAfterCloseIsImmutable(t *testing.T) {
	s := NewWindowStore(time.Minute, 10*time.Second)
	lowWM := iso(t, "2026-01-01T00:00:05Z")
	s.Add(ev("e1", "a", "2026-01-01T00:00:05Z", 1), lowWM)
	// 水位推进到关闭视界 00:01:10：窗口最终关闭。
	wm := iso(t, "2026-01-01T00:01:10Z")
	id := WindowID{Key: "a", Start: iso(t, "2026-01-01T00:00:00Z")}
	ids := s.Closeable(wm)
	if len(ids) != 1 {
		t.Fatalf("expected one closeable, got %d (eventTime=%s watermark=%s)",
			len(ids), "2026-01-01T00:00:05Z", wm.Format(time.RFC3339))
	}
	r, _ := s.Close(ids[0], wm, wm, 1)
	before := *r

	// 水位关闭后的迟到事件：不得修改结果。
	accepted, dropped, dup := s.Add(ev("late1", "a", "2026-01-01T00:00:09Z", 100), wm)
	if accepted || dup || !dropped {
		t.Fatalf("post-close event must be dropped, got accepted=%v dropped=%v dup=%v "+
			"(eventTime=%s watermark=%s window=[%s,%s))",
			accepted, dropped, dup,
			"2026-01-01T00:00:09Z", wm.Format(time.RFC3339),
			id.Start.Format(time.RFC3339), id.Start.Add(time.Minute).Format(time.RFC3339))
	}
	r2, ok := s.Close(id, wm, wm, 2)
	if ok || r2 != nil {
		t.Fatal("closed window must not be re-closeable")
	}
	if seq, emitted := s.Emitted(id); !emitted || seq != 1 {
		t.Fatalf("emitted marker wrong: seq=%d emitted=%v", seq, emitted)
	}
	if got := s.EmittedResults(); len(got) != 1 || got[0].Sum != before.Sum || got[0].Count != before.Count {
		t.Fatalf("closed result mutated: %+v", got)
	}
}

func TestDuplicateEventIdIdempotent(t *testing.T) {
	s := NewWindowStore(time.Minute, 10*time.Second)
	wm := iso(t, "2026-01-01T00:00:30Z")
	if a, d, du := s.Add(ev("x", "a", "2026-01-01T00:00:01Z", 5), wm); !a || d || du {
		t.Fatal("first add must accept")
	}
	// 完全重复投递（重放/重试）：幂等忽略。
	if a, d, du := s.Add(ev("x", "a", "2026-01-01T00:00:01Z", 5), wm); a || d || !du {
		t.Fatalf("duplicate must be flagged, got accepted=%v dropped=%v dup=%v", a, d, du)
	}
	// 不同键复用同一 id：不同窗口互不影响（id 去重以窗口为界）。
	if a, _, du := s.Add(ev("x", "b", "2026-01-01T00:00:01Z", 5), wm); !a || du {
		t.Fatal("same id on different key must accept")
	}
	id := WindowID{Key: "a", Start: iso(t, "2026-01-01T00:00:00Z")}
	r, _ := s.Close(id, endOfInputWatermark, endOfInputWatermark, 1)
	if r.Count != 1 || r.Sum != 5 {
		t.Fatalf("duplicate changed the aggregate: %+v", r)
	}
}

func TestDeterministicCloseOrder(t *testing.T) {
	s := NewWindowStore(time.Minute, 10*time.Second)
	// 低水位时加入全部事件（不会被判迟到），再在 +inf 下统一关闭。
	s.Add(ev("1", "b", "2026-01-01T00:01:30Z", 1), time.Time{})
	s.Add(ev("2", "a", "2026-01-01T00:00:30Z", 1), time.Time{})
	s.Add(ev("3", "a", "2026-01-01T00:01:30Z", 1), time.Time{})
	s.Add(ev("4", "b", "2026-01-01T00:00:30Z", 1), time.Time{})
	ids := s.Closeable(endOfInputWatermark)
	want := []WindowID{
		{Key: "a", Start: iso(t, "2026-01-01T00:00:00Z")},
		{Key: "b", Start: iso(t, "2026-01-01T00:00:00Z")},
		{Key: "a", Start: iso(t, "2026-01-01T00:01:00Z")},
		{Key: "b", Start: iso(t, "2026-01-01T00:01:00Z")},
	}
	if len(ids) != len(want) {
		t.Fatalf("got %v", ids)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("position %d: want %+v got %+v (deterministic order required)",
				i, want[i], ids[i])
		}
	}
}
