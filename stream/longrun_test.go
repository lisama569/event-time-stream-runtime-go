package stream

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"
)

// 长跑用例的固定形状：窗口 1 分钟、4 个键、每键每窗 2 条事件，
// 事件时间逐分钟推进（共 totalWindows 个分钟窗）。
const (
	lrKeys      = 4
	lrPerKey    = 2
	lrWindow    = time.Minute
	lrLateness  = 5 * time.Second
	lrRetention = 2 * time.Minute // 已发布结果的事件时间保留范围
	lrBoundOpen = 32              // 活跃窗口数上界（结构保证 ~8，留足余量）
	lrBoundEmit = 64              // 保留已发布结果上界（结构保证 ~20）
)

func lrConfig(ckptPath string) Config {
	cfg := DefaultConfig()
	cfg.WindowSize = lrWindow
	cfg.AllowedLateness = lrLateness
	cfg.EmittedRetention = lrRetention
	cfg.SourceQueueSize = 16
	cfg.CheckpointEvery = 50
	cfg.CheckpointPath = ckptPath
	return cfg
}

// lrLines 生成 totalWindows 个分钟窗的事件（事件时间单调推进）。
func lrLines(totalWindows int) []string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lines := make([]string, 0, totalWindows*lrKeys*lrPerKey)
	for w := 0; w < totalWindows; w++ {
		for k := 0; k < lrKeys; k++ {
			for j := 0; j < lrPerKey; j++ {
				ts := base.Add(time.Duration(w)*time.Minute +
					time.Duration(j)*time.Second + time.Duration(k)*time.Millisecond)
				lines = append(lines, fmt.Sprintf("w%dk%dj%d,key%d,%s,%s",
					w, k, j, k, ts.Format(time.RFC3339Nano),
					[]string{"1.5", "2.5"}[j]))
			}
		}
	}
	return lines
}

// lrRunOnce 跑一档规模，轮询 Status 记录状态体积峰值，返回最终快照与检查点大小。
func lrRunOnce(t *testing.T, totalWindows int, ckptPath string) (StatusSnapshot, int64, *MemorySink) {
	t.Helper()
	cfg := lrConfig(ckptPath)
	sink := NewMemorySink()
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		t.Errorf("unexpected classified error: %s", ce)
	})

	var maxActive, maxRetained atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				s := eng.Status()
				for {
					cur := maxActive.Load()
					if s.ActiveWindows <= cur || maxActive.CompareAndSwap(cur, s.ActiveWindows) {
						break
					}
				}
				for {
					cur := maxRetained.Load()
					if s.EmittedRetained <= cur || maxRetained.CompareAndSwap(cur, s.EmittedRetained) {
						break
					}
				}
			}
		}
	}()

	src := NewSliceSource("longrun", lrLines(totalWindows))
	if err := eng.Run(context.Background(), []Source{src}, sink); err != nil {
		t.Fatal(err)
	}
	close(stop)

	final := eng.Status()
	if maxActive.Load() > lrBoundOpen {
		t.Fatalf("windows=%d: active windows peaked at %d, exceeds bound %d (state grows with input?)",
			totalWindows, maxActive.Load(), lrBoundOpen)
	}
	if maxRetained.Load() > lrBoundEmit {
		t.Fatalf("windows=%d: retained emitted peaked at %d, exceeds bound %d (state grows with input?)",
			totalWindows, maxRetained.Load(), lrBoundEmit)
	}
	fi, err := os.Stat(ckptPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("windows=%d: sampled max active=%d max retained_emitted=%d final checkpoint=%d bytes",
		totalWindows, maxActive.Load(), maxRetained.Load(), fi.Size())
	return final, fi.Size(), sink
}

// TestLongRunBoundedState：两档差别很大（10 倍）的累计窗口总量下，
// 运行期状态体积（活跃窗口数、保留的已发布结果数）与落盘检查点体积
// 都不随总量增长，且结果不丢不重。
func TestLongRunBoundedState(t *testing.T) {
	dir := t.TempDir()

	const small, large = 300, 3000 // 累计窗口总量差 10 倍

	finalS, sizeS, sinkS := lrRunOnce(t, small, dir+"/ck-small.json")
	finalL, sizeL, sinkL := lrRunOnce(t, large, dir+"/ck-large.json")

	// 正确性：结果不丢（每个键每窗恰好一条结果）不重（MemorySink 已校验）。
	for name, sink := range map[string]*MemorySink{"small": sinkS, "large": sinkL} {
		n := small
		if name == "large" {
			n = large
		}
		want := n * lrKeys
		rs := sink.Snapshot()
		if len(rs) != want {
			t.Fatalf("%s: got %d results, want %d (lost or duplicated windows)",
				name, len(rs), want)
		}
		if len(sink.Duplicates) > 0 {
			t.Fatalf("%s: duplicate emissions: %v", name, sink.Duplicates)
		}
		for _, r := range rs {
			if r.Count != lrPerKey || r.Sum != 4.0 {
				t.Fatalf("%s: bad aggregate %s@%s count=%d sum=%v",
					name, r.Key, r.WindowStart, r.Count, r.Sum)
			}
		}
	}

	// 终结语义：两档都关闭了全部窗口，运行结束后活跃窗口归零。
	if finalS.WindowsClosed != int64(small*lrKeys) ||
		finalL.WindowsClosed != int64(large*lrKeys) {
		t.Fatalf("windows closed mismatch: small=%d large=%d",
			finalS.WindowsClosed, finalL.WindowsClosed)
	}
	if finalS.ActiveWindows != 0 || finalL.ActiveWindows != 0 {
		t.Fatalf("active windows after EOF: small=%d large=%d",
			finalS.ActiveWindows, finalL.ActiveWindows)
	}

	// 有界性：10 倍总量，检查点体积不得随之增长（允许 JSON 字节级抖动）。
	if sizeL > sizeS*2 || sizeS > sizeL*2 {
		t.Fatalf("checkpoint size grows with total windows: %d windows -> %d bytes, %d windows -> %d bytes",
			small, sizeS, large, sizeL)
	}
	// 保留的已发布结果尾部相近（只取决于保留范围，不取决于总量）。
	if finalS.EmittedRetained != finalL.EmittedRetained {
		t.Fatalf("retained tail should be scale-independent: small=%d large=%d",
			finalS.EmittedRetained, finalL.EmittedRetained)
	}
	t.Logf("PASS: 10x more windows (%d vs %d), checkpoint %d vs %d bytes, retained tail %d vs %d — state bounded",
		large, small, sizeL, sizeS, finalL.EmittedRetained, finalS.EmittedRetained)
}

// lrWriteFile 把事件写入本地文件并返回可恢复的文件源（支持 SeekOffset）。
func lrWriteFile(t *testing.T, path string, totalWindows int) *FileSource {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lrLines(totalWindows) {
		if _, err := f.WriteString(line + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileSource(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	return src
}

// TestLongRunRecoveryWithRetention：保留策略下崩溃恢复仍然不重不漏，
// 且 emit_seq 跨恢复单调（已发布记录被修剪后靠检查点中的 NextEmitSeq）。
func TestLongRunRecoveryWithRetention(t *testing.T) {
	dir := t.TempDir()
	ck := dir + "/ck.json"
	cfg := lrConfig(ck)

	// 阶段 1：跑 300 个窗口。
	sink1 := NewMemorySink()
	eng1 := NewEngine(cfg, nil)
	if err := eng1.Run(context.Background(),
		[]Source{lrWriteFile(t, dir+"/in.csv", 300)}, sink1); err != nil {
		t.Fatal(err)
	}
	phase1 := sink1.Snapshot()
	if len(phase1) != 300*lrKeys {
		t.Fatalf("phase1: got %d results, want %d", len(phase1), 300*lrKeys)
	}
	var maxSeq1 int64
	for _, r := range phase1 {
		if r.EmitSeq > maxSeq1 {
			maxSeq1 = r.EmitSeq
		}
	}

	// 阶段 2：同一检查点重启，源文件增长到 350 个窗口。
	// 恢复时只重放保留尾部（而非全部 1200 条历史结果）。
	sink2 := NewMemorySink()
	eng2 := NewEngine(cfg, nil)
	if err := eng2.Run(context.Background(),
		[]Source{lrWriteFile(t, dir+"/in.csv", 350)}, sink2); err != nil {
		t.Fatal(err)
	}
	phase2 := sink2.Snapshot()

	replayed := 0
	minNewSeq := int64(1<<62 - 1)
	for _, r := range phase2 {
		if r.EmitSeq <= maxSeq1 {
			replayed++ // 重放的保留尾部：沿用原 emit_seq
		} else if r.EmitSeq < minNewSeq {
			minNewSeq = r.EmitSeq
		}
	}
	retained := eng1.Status().EmittedRetained
	if int64(replayed) != retained {
		t.Fatalf("phase2 replayed %d results, want retained tail %d (bounded replay)",
			replayed, retained)
	}
	if replayed >= len(phase1) {
		t.Fatalf("replay not bounded: replayed %d of %d historical results", replayed, len(phase1))
	}
	// 新结果的 emit_seq 必须全部大于阶段 1 的最大序号（修剪后仍单调）。
	newCount := 0
	for _, r := range phase2 {
		if r.EmitSeq > maxSeq1 {
			newCount++
		}
	}
	if newCount > 0 && minNewSeq <= maxSeq1 {
		t.Fatalf("emit_seq regressed after pruning: min new %d <= phase1 max %d",
			minNewSeq, maxSeq1)
	}

	// 不重不漏：阶段 1 ∪ 阶段 2 按窗口去重后 == 一次性跑 350 窗的参照。
	refSink := NewMemorySink()
	refCfg := lrConfig("")
	refEng := NewEngine(refCfg, nil)
	if err := refEng.Run(context.Background(),
		[]Source{NewSliceSource("longrun", lrLines(350))}, refSink); err != nil {
		t.Fatal(err)
	}
	type winKey struct {
		key   string
		start int64
	}
	union := make(map[winKey]string)
	put := func(rs []*WindowResult) {
		for _, r := range rs {
			union[winKey{r.Key, r.WindowStart.UnixNano()}] =
				fmt.Sprintf("%d|%x|%x|%x|%s", r.Count,
					math.Float64bits(r.Sum), math.Float64bits(r.Min), math.Float64bits(r.Max), r.FirstEventID)
		}
	}
	put(phase1)
	put(phase2)
	if len(union) != 350*lrKeys {
		t.Fatalf("union covers %d windows, want %d (loss or unexpected extra)",
			len(union), 350*lrKeys)
	}
	for _, r := range refSink.Snapshot() {
		k := winKey{r.Key, r.WindowStart.UnixNano()}
		want := fmt.Sprintf("%d|%x|%x|%x|%s", r.Count,
			math.Float64bits(r.Sum), math.Float64bits(r.Min), math.Float64bits(r.Max), r.FirstEventID)
		if union[k] != want {
			t.Fatalf("window %s@%s mismatch after recovery: got %s want %s",
				r.Key, r.WindowStart, union[k], want)
		}
	}
	t.Logf("PASS: recovery with retention replayed only bounded tail (%d of %d), "+
		"emit_seq monotone (phase1 max=%d < min new=%d), union == reference (%d windows)",
		replayed, len(phase1), maxSeq1, minNewSeq, len(union))
}
