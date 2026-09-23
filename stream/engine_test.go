package stream

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"
)

func collectErrors() (*[]*ClassifiedError, ErrorHandler) {
	var mu sync.Mutex
	var errs []*ClassifiedError
	h := func(ce *ClassifiedError) {
		mu.Lock()
		defer mu.Unlock()
		cp := *ce
		errs = append(errs, &cp)
	}
	return &errs, h
}

func kinds(errs *[]*ClassifiedError) map[ErrorKind]int {
	out := map[ErrorKind]int{}
	for _, e := range *errs {
		out[e.Kind]++
	}
	return out
}

func resultMap(rs []*WindowResult) map[string]*WindowResult {
	m := map[string]*WindowResult{}
	for _, r := range rs {
		m[fmt.Sprintf("%s@%s", r.Key, r.WindowStart.Format("15:04:05"))] = r
	}
	return m
}

// TestEngineClassifications 在“同一源内严格有序”的输入上确定性地覆盖
// 全部异常分类：MALFORMED / INVALID_TIMESTAMP / DUPLICATE /
// OUT_OF_RANGE / LATE_DROPPED。失败时错误对象自带事件时间与水位判据。
func TestEngineClassifications(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 5 * time.Second
	cfg.SourceQueueSize = 4
	cfg.MaxOutofOrder = 2 * time.Minute

	// 全部记录放在同一个源：源内顺序确定，水位推进与分类判定因此完全确定。
	lines := []string{
		"e1,a,2026-01-01T00:00:01Z,1", // 接受
		"e3,a,2026-01-01T00:00:20Z,3", // 乱序仍接受
		"e2,a,2026-01-01T00:00:10Z,2", // 乱序仍接受
		"bad,line",                    // MALFORMED（字段数不对）
		"e4,a,not-a-time,4",           // INVALID_TIMESTAMP
		"e1,a,2026-01-01T00:00:01Z,1", // DUPLICATE（重复投递）
		"e9,a,2026-01-01T00:05:00Z,9", // OUT_OF_RANGE（超前当前水位 4m40s > 2m）
		"e5,a,2026-01-01T00:01:20Z,5", // 水位 -> 00:01:20，越过 [00:00,00:01) 视界 00:01:05
		"e6,a,2026-01-01T00:00:50Z,6", // LATE_DROPPED（窗口已关，不得修改）
	}
	errs, h := collectErrors()
	sink := NewMemorySink()
	eng := NewEngine(cfg, h)
	if err := eng.Run(context.Background(),
		[]Source{NewSliceSource("s", lines)}, sink); err != nil {
		t.Fatal(err)
	}
	km := kinds(errs)
	want := map[ErrorKind]int{
		KindMalformed: 1, KindInvalidTimestamp: 1, KindDuplicate: 1,
		KindOutOfRange: 1, KindLateDropped: 1,
	}
	for k, n := range want {
		if km[k] != n {
			for _, e := range *errs {
				t.Logf("classified %s: eventTime=%s watermark=%s reason=%s",
					e.Kind, formatTime(e.EventTime), formatTime(e.Watermark), e.Reason)
			}
			t.Fatalf("kind %s: want %d got %d", k, n, km[k])
		}
	}
	// 迟到事件 e6 不得修改 a@00:00（仅 e1,e2,e3：count=3 sum=6）。
	m := resultMap(sink.Snapshot())
	r := m["a@00:00:00"]
	if r == nil || r.Count != 3 || r.Sum != 6 {
		t.Fatalf("closed result mutated by late event: %+v", r)
	}
	if st := eng.Report(); st.Accepted != 4 {
		t.Fatalf("accepted want 4 (e1,e2,e3,e5), got %d: %+v", st.Accepted, st)
	}
}

// TestEngineConcurrentDeterminism：多源并发 + 乱序在多次运行下产出
// 唯一确定的窗口结果；慢源不阻塞整体吞吐。
func TestEngineConcurrentDeterminism(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 10 * time.Second
	cfg.SourceQueueSize = 8

	srcA := []string{
		"e1,a,2026-01-01T00:00:01Z,1",
		"e3,a,2026-01-01T00:00:20Z,3",
		"e2,a,2026-01-01T00:00:10Z,2",
		"e4,a,2026-01-01T00:00:59Z,4",
	}
	srcB := []string{
		"e10,b,2026-01-01T00:00:02Z,10",
		"e11,b,2026-01-01T00:00:40Z,20",
	}
	srcC := []string{ // 慢源
		"e20,c,2026-01-01T00:00:03Z,7",
		"e21,c,2026-01-01T00:00:33Z,8",
	}

	sig := func(rs []*WindowResult) []string {
		m := resultMap(rs)
		keys := make([]string, 0, len(m))
		out := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			r := m[k]
			out = append(out, fmt.Sprintf("%s=%d/%v/%v/%v/%s", k,
				r.Count, r.Sum, r.Min, r.Max, r.FirstEventID))
		}
		return out
	}

	var baseline []string
	for attempt := 0; attempt < 8; attempt++ {
		sink := NewMemorySink()
		eng := NewEngine(cfg, nil)
		if err := eng.Run(context.Background(), []Source{
			NewSliceSource("A", srcA),
			NewSliceSource("B", srcB),
			NewSliceSource("C", srcC).WithDelay(time.Millisecond),
		}, sink); err != nil {
			t.Fatal(err)
		}
		got := sig(sink.Snapshot())
		if attempt == 0 {
			baseline = got
			continue
		}
		if len(got) != len(baseline) {
			t.Fatalf("attempt %d: %v != %v", attempt, got, baseline)
		}
		for i := range got {
			if got[i] != baseline[i] {
				t.Fatalf("attempt %d nondeterministic:\n got %v\nwant %v",
					attempt, got, baseline)
			}
		}
	}
	wantContains := map[string]string{
		"a@00:00:00": "a@00:00:00=4/10/1/4/e1", // 乱序 e2/e3 正确归窗
		"b@00:00:00": "b@00:00:00=2/30/10/20/e10",
		"c@00:00:00": "c@00:00:00=2/15/7/8/e20", // 慢源结果完整
	}
	for _, w := range wantContains {
		found := false
		for _, s := range baseline {
			if s == w {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing expected %s; got %v", w, baseline)
		}
	}
}

// TestEngineBackpressureDoesNotBlockOtherSources：有界队列反压快源，
// 慢源有独立队列仍被公平调度，不死锁、不静默丢事件。
func TestEngineBackpressureDoesNotBlockOtherSources(t *testing.T) {
	cfg := DefaultConfig()
	cfg.SourceQueueSize = 2
	cfg.CheckpointEvery = 1 << 30

	fast := make([]string, 200)
	for i := range fast {
		fast[i] = fmt.Sprintf("f%d,a,2026-01-01T00:00:%02dZ,1", i, i%60)
	}
	slow := []string{
		"s1,b,2026-01-01T00:00:01Z,1",
		"s2,b,2026-01-01T00:00:30Z,1",
	}
	sink := NewMemorySink()
	eng := NewEngine(cfg, nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := eng.Run(context.Background(), []Source{
			NewSliceSource("fast", fast),
			NewSliceSource("slow", slow).WithDelay(5 * time.Millisecond),
		}, sink); err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("deadlock: backpressure starved the slow source")
	}
	if st := eng.Report(); st.Accepted != 202 {
		t.Fatalf("backpressure lost events: accepted=%d want 202", st.Accepted)
	}
}
