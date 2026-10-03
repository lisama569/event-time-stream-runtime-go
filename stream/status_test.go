package stream

import (
	"context"
	"testing"
	"time"
)

// slowSink 包装一个 Sink，在每次 Emit 前睡眠，模拟慢消费者以制造积压。
type slowSink struct {
	inner Sink
	delay time.Duration
}

func (s *slowSink) Emit(r *WindowResult) error {
	time.Sleep(s.delay)
	return s.inner.Emit(r)
}

func (s *slowSink) Close() error { return s.inner.Close() }

// TestStatusBeforeAndAfterRun：未运行时 Status 为零值；运行结束后
// 返回最终值且与 Report 一致。
func TestStatusBeforeAndAfterRun(t *testing.T) {
	eng := NewEngine(DefaultConfig(), nil)
	st := eng.Status()
	if st.Running || st.Ingested != 0 || st.Sources != nil {
		t.Fatalf("fresh engine must report zero status, got %+v", st)
	}
	t.Logf("scenario fresh-engine -> zero status, running=%v: PASS", st.Running)

	lines := []string{
		"a1,a,2026-01-01T00:00:05Z,1",
		"a2,a,2026-01-01T00:00:30Z,2",
		"a3,a,2026-01-01T00:10:00Z,3", // 推进水位，关闭 [00:00,00:01)
		"a4,a,2026-01-01T00:00:10Z,9", // 迟到：窗口已关闭 -> LATE_DROPPED
		"garbage-line",                // -> MALFORMED
	}
	sink := NewMemorySink()
	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 0
	if err := eng.Run(context.Background(),
		[]Source{NewSliceSource("s1", lines)}, sink); err != nil {
		t.Fatal(err)
	}

	st = eng.Status()
	rep := eng.Report()
	if st.Running {
		t.Fatal("status must not be running after Run returned")
	}
	if st.Ingested != rep.Ingested || st.Accepted != rep.Accepted ||
		st.Malformed != rep.Malformed || st.LateDropped != rep.LateDropped {
		t.Fatalf("status counters diverge from report: status=%+v report=%+v", st, rep)
	}
	if st.Malformed != 1 || st.LateDropped != 1 {
		t.Fatalf("want malformed=1 late_dropped=1, got %+v", st)
	}
	if st.WindowsEmitted != int64(len(sink.Snapshot())) {
		t.Fatalf("windows_emitted=%d but sink has %d", st.WindowsEmitted, len(sink.Snapshot()))
	}
	if st.ActiveWindows != 0 {
		t.Fatalf("all windows must be closed after EOI, active=%d", st.ActiveWindows)
	}
	if len(st.Sources) != 1 || !st.Sources[0].Finished || st.Sources[0].Offset != int64(len(lines)) {
		t.Fatalf("source status wrong: %+v", st.Sources)
	}
	t.Logf("scenario after-run -> running=%v ingested=%d accepted=%d malformed=%d late=%d emitted=%d src=%+v: PASS",
		st.Running, st.Ingested, st.Accepted, st.Malformed, st.LateDropped,
		st.WindowsEmitted, st.Sources[0])
}

// TestStatusDuringRun：摄入进行中并发读取状态摘要——不停止摄入、不改配置，
// 能观察到运行标志、活跃窗口、各源积压与计数推进。
func TestStatusDuringRun(t *testing.T) {
	const n = 300
	lines := genRotatingLines(n, 4) // 300 个 1 秒窗口，4 个键轮转
	cfg := DefaultConfig()
	cfg.WindowSize = time.Second
	cfg.AllowedLateness = 0
	cfg.SourceQueueSize = 4
	cfg.CheckpointPath = ""

	// 快源 + 慢汇：汇在状态协程中执行，慢下来后摄取队列必然积压，
	// 从而能在运行中观察到各源积压情况。
	src := NewSliceSource("fast", lines)
	sink := &slowSink{inner: NewMemorySink(), delay: 500 * time.Microsecond}
	eng := NewEngine(cfg, nil)

	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background(), []Source{src}, sink) }()

	var sawRunning, sawBacklog, sawActive bool
	var maxQueued, samples int
	var lastIngested int64 = -1
	monotone := true
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			goto finished
		default:
			st := eng.Status()
			samples++
			if st.Running {
				sawRunning = true
			}
			if st.ActiveWindows > 0 {
				sawActive = true
			}
			if lastIngested >= 0 && st.Ingested < lastIngested {
				monotone = false
			}
			lastIngested = st.Ingested
			for _, s := range st.Sources {
				if s.Queued < 0 || s.Queued > s.Capacity {
					t.Fatalf("queued %d out of capacity %d", s.Queued, s.Capacity)
				}
				if s.Queued > 0 {
					sawBacklog = true
				}
				if s.Queued > maxQueued {
					maxQueued = s.Queued
				}
			}
			time.Sleep(time.Millisecond)
		}
	}
finished:
	if !sawRunning {
		t.Fatal("never observed running status during ingestion")
	}
	if !sawActive {
		t.Fatal("never observed active windows during ingestion")
	}
	if !monotone {
		t.Fatal("ingested counter must be monotone across samples")
	}
	if !sawBacklog {
		t.Fatal("never observed source backlog with fast source + slow sink")
	}
	st := eng.Status()
	t.Logf("scenario mid-run-sampling -> samples=%d running=%v activeWindowsSeen=%v "+
		"backlogSeen=%v maxQueued=%d/%d finalIngested=%d: PASS",
		samples, sawRunning, sawActive, sawBacklog, maxQueued,
		cfg.SourceQueueSize, st.Ingested)
	if st.Running || st.Ingested != int64(n) {
		t.Fatalf("final status wrong: %+v", st)
	}
}
