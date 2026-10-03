package stream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestStatusSnapshotDuringRun：不改配置、不中断摄入，运行中并发读取
// 状态摘要；读到的字段满足一致性不变量，运行结束后与 Report 对齐。
func TestStatusSnapshotDuringRun(t *testing.T) {
	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 5 * time.Second
	cfg.SourceQueueSize = 8

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(key string, n int) []string {
		lines := make([]string, 0, n)
		for i := 0; i < n; i++ {
			// 事件时间逐半分钟推进（每窗 2 条），单调避免非预期迟到。
			ts := base.Add(time.Duration(i) * 30 * time.Second)
			lines = append(lines, fmt.Sprintf("%s-%d,%s,%s,1.5",
				key, i, key, ts.Format(time.RFC3339Nano)))
		}
		return lines
	}
	slowLines := mk("slow", 60) // 3ms/条，保证有充足的“运行中”窗口可观测
	fastLines := mk("fast", 60)
	total := int64(len(slowLines) + len(fastLines))

	eng := NewEngine(cfg, nil)
	// 运行前读取：零值快照，不应 panic。
	pre := eng.Status()
	if pre.Ingested != 0 || len(pre.Sources) != 0 {
		t.Fatalf("status before Run should be empty, got %+v", pre)
	}

	sink := NewMemorySink()
	done := make(chan error, 1)
	go func() {
		done <- eng.Run(context.Background(), []Source{
			NewSliceSource("slow", slowLines).WithDelay(3 * time.Millisecond),
			NewSliceSource("fast", fastLines),
		}, sink)
	}()

	var sawMidRun, sawActive bool
	maxBacklog := 0
	samples := 0
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			goto drained
		default:
		}
		s := eng.Status()
		if len(s.Sources) == 0 {
			continue // Run 尚未装上状态镜像，等下一个样本
		}
		samples++
		// 一致性不变量：每个样本都必须满足。
		if len(s.Sources) != 2 {
			t.Fatalf("want 2 sources, got %+v", s.Sources)
		}
		if s.Sources[0].Name != "fast" || s.Sources[1].Name != "slow" {
			t.Fatalf("sources not sorted by name: %+v", s.Sources)
		}
		var offsets int64
		for _, ss := range s.Sources {
			if ss.Backlog < 0 || ss.Backlog > ss.QueueCap {
				t.Fatalf("backlog %d out of bounds (cap %d)", ss.Backlog, ss.QueueCap)
			}
			if ss.Backlog > maxBacklog {
				maxBacklog = ss.Backlog
			}
			offsets += ss.Offset
		}
		// 摘要是无锁读取：单条在途记录可造成最多每源 1 条的计数偏斜。
		if d := offsets - s.Ingested; d < -2 || d > 2 {
			t.Fatalf("ingested %d vs sum of source offsets %d: skew %d exceeds in-flight bound",
				s.Ingested, offsets, d)
		}
		if s.Accepted+s.Malformed+s.InvalidTS+s.OutOfRange+s.LateDropped+s.Duplicates > s.Ingested {
			t.Fatalf("classified counts exceed ingested: %+v", s)
		}
		if s.ActiveWindows > 0 {
			sawActive = true
		}
		if s.Ingested > 0 && s.Ingested < total {
			sawMidRun = true
		}
		time.Sleep(100 * time.Microsecond)
	}
drained:
	if !sawMidRun {
		t.Fatal("never observed a mid-run snapshot (status not readable during ingestion?)")
	}
	if !sawActive {
		t.Fatal("never observed active windows during run")
	}
	t.Logf("during run: %d samples, observed active windows and backlog (max %d/%d)",
		samples, maxBacklog, cfg.SourceQueueSize)

	// 运行结束后：摘要与 Report 对齐，窗口全部终结，源全部 EOF。
	final := eng.Status()
	st := eng.Report()
	if final.Ingested != st.Ingested || final.Accepted != st.Accepted ||
		final.LateDropped != st.LateDropped || final.Duplicates != st.Duplicates {
		t.Fatalf("status counters diverge from report: status=%+v report=%+v", final, st)
	}
	if final.Ingested != total {
		t.Fatalf("final ingested %d, want %d", final.Ingested, total)
	}
	if final.ActiveWindows != 0 {
		t.Fatalf("active windows after EOF: %d", final.ActiveWindows)
	}
	if got := len(sink.Snapshot()); final.WindowsClosed != int64(got) {
		t.Fatalf("windows closed %d != emitted results %d", final.WindowsClosed, got)
	}
	for _, ss := range final.Sources {
		if !ss.Finished {
			t.Fatalf("source %s not finished after Run", ss.Name)
		}
		if ss.Offset != 60 {
			t.Fatalf("source %s offset %d, want 60", ss.Name, ss.Offset)
		}
		if ss.Backlog != 0 {
			t.Fatalf("source %s backlog %d after EOF", ss.Name, ss.Backlog)
		}
	}
	t.Logf("PASS: final status consistent — ingested=%d accepted=%d closed=%d retained=%d sources=%+v",
		final.Ingested, final.Accepted, final.WindowsClosed, final.EmittedRetained,
		final.Sources)
}
