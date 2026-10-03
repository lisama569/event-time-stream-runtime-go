package stream

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// genRotatingLines 生成 n 条事件：时间逐秒递增，键在 keys 个键间轮转，
// 因此每秒产生一个新窗口，总窗口数 = n（高基数、持续增长的场景）。
func genRotatingLines(n, keys int) []string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		ts := base.Add(time.Duration(i) * time.Second).Format(time.RFC3339)
		lines = append(lines, "e"+itoa(i)+",k"+itoa(i%keys)+","+ts+",1")
	}
	return lines
}

// boundedCfg 返回长跑场景配置：1 秒窗口、不容迟到、保留范围 5 秒、高频检查点。
func boundedCfg(ckpt string) Config {
	cfg := DefaultConfig()
	cfg.WindowSize = time.Second
	cfg.AllowedLateness = 0
	cfg.EmittedRetention = 5 * time.Second
	cfg.CheckpointEvery = 50
	cfg.CheckpointPath = ckpt
	cfg.SourceQueueSize = 8
	return cfg
}

// runLongRun 跑一次长跑并周期采样 Status，返回结果数、运行期间观测到的
// 最大活跃窗口数/最大保留已发布数，以及最终检查点文件大小。
func runLongRun(t *testing.T, nWindows, keys int, dir string) (emitted int,
	maxActive, maxRetained int64, ckptSize int64) {
	t.Helper()
	cfg := boundedCfg(filepath.Join(dir, "ckpt.json"))
	lines := genRotatingLines(nWindows, keys)
	src := NewSliceSource("s", lines)
	sink := NewMemorySink()
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		t.Errorf("unexpected classified error: %s", ce)
	})

	done := make(chan error, 1)
	go func() { done <- eng.Run(context.Background(), []Source{src}, sink) }()

	// 运行中周期采样状态摘要，记录状态体积高水位。
	for {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			st := eng.Status()
			if st.ActiveWindows != 0 {
				t.Errorf("after EOI all windows must be closed, active=%d", st.ActiveWindows)
			}
			goto finished
		default:
			st := eng.Status()
			if st.ActiveWindows > maxActive {
				maxActive = st.ActiveWindows
			}
			if st.RetainedEmitted > maxRetained {
				maxRetained = st.RetainedEmitted
			}
			time.Sleep(time.Millisecond)
		}
	}
finished:
	if dups := sink.Duplicates; len(dups) != 0 {
		t.Fatalf("duplicate emits: %v", dups)
	}
	fi, err := os.Stat(filepath.Join(dir, "ckpt.json"))
	if err != nil {
		t.Fatal(err)
	}
	return len(sink.Snapshot()), maxActive, maxRetained, fi.Size()
}

// TestLongRunStateBounded：两档差别很大的窗口总量（2k vs 20k 个窗口），
// 验证内存状态与检查点体积只跟随活跃范围/保留范围，不随累计窗口数增长。
func TestLongRunStateBounded(t *testing.T) {
	const keys = 64
	emittedA, activeA, retainedA, sizeA := runLongRun(t, 500, keys, t.TempDir())
	emittedB, activeB, retainedB, sizeB := runLongRun(t, 5_000, keys, t.TempDir())

	t.Logf("scenario A: windows=%d emitted=%d maxActive=%d maxRetained=%d ckptBytes=%d",
		500, emittedA, activeA, retainedA, sizeA)
	t.Logf("scenario B: windows=%d emitted=%d maxActive=%d maxRetained=%d ckptBytes=%d",
		5_000, emittedB, activeB, retainedB, sizeB)

	// 结果不丢不重：10 倍窗口总量 -> 10 倍结果数。
	if emittedA != 500 || emittedB != 5_000 {
		t.Fatalf("emitted mismatch: A=%d want 500, B=%d want 5000", emittedA, emittedB)
	}

	// 状态体积有界：只与配置（队列容量/键基数/保留范围）相关，与累计窗口数无关。
	// 活跃窗口上界 ~ 摄取队列容量（源 EOF 时残余记录排空期间短暂并存）
	// 与键基数；保留已发布上界 ~ 保留范围(5s)/窗口(1s) × 键数。
	const activeBound = 8 + 2 + keys
	const retainedBound = (5 + 2) * keys
	if activeA > activeBound || activeB > activeBound {
		t.Fatalf("active windows unbounded: A=%d B=%d bound=%d", activeA, activeB, activeBound)
	}
	if retainedA > retainedBound || retainedB > retainedBound {
		t.Fatalf("retained emitted unbounded: A=%d B=%d bound=%d", retainedA, retainedB, retainedBound)
	}

	// 检查点体积不随总量增长：B 的窗口总量是 A 的 10 倍，
	// 落盘进度体积必须保持在同一量级（允许 2 倍余量覆盖序号位数等）。
	if sizeB > sizeA*2 {
		t.Fatalf("checkpoint size grows with total windows: A=%dB B=%dB", sizeA, sizeB)
	}
	t.Logf("conclusion: 10x windows -> ckpt size ratio %.2fx (<=2x), state bounded by active set + retention: PASS",
		float64(sizeB)/float64(sizeA))
}

// TestRestartWithPrunedEmittedKeepsOutput：保留范围=0 时检查点会遗忘
// 已发布记录；重启后输出文件必须既不丢历史结果也不重复（CommittedFileSink
// 追加+幂等去重），且检查点体积不随重启累积的窗口数增长。
func TestRestartWithPrunedEmittedKeepsOutput(t *testing.T) {
	dir := t.TempDir()
	ck := filepath.Join(dir, "ckpt.json")
	out := filepath.Join(dir, "out.jsonl")
	const keys = 10

	cfg := boundedCfg(ck)
	cfg.EmittedRetention = 0 // 关闭视界一过即遗忘，最激进的有界配置
	cfg.DeterministicClosedAt = true

	lines := genRotatingLines(400, keys)
	inPath := filepath.Join(dir, "in.csv")

	// 阶段 1：处理前 200 条（200 个窗口）。用文件源以支持按偏移恢复。
	writeLines := func(n int) {
		content := strings.Join(lines[:n], "\n") + "\n"
		if err := os.WriteFile(inPath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	openFile := func(t *testing.T) *FileSource {
		src, err := NewFileSource(inPath)
		if err != nil {
			t.Fatal(err)
		}
		return src
	}

	writeLines(200)
	src1 := openFile(t)
	defer src1.Close()
	sink1, err := NewCommittedFileSink(out)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := NewEngine(cfg, nil)
	if err := eng1.Run(context.Background(), []Source{src1}, sink1); err != nil {
		t.Fatal(err)
	}
	_ = sink1.Close()
	phase1Bytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	size1 := fileSize(t, ck)
	t.Logf("phase1: emitted=200 ckptBytes=%d outBytes=%d", size1, len(phase1Bytes))

	// 阶段 2：同一检查点重启，输入增长到 400 条，输出文件不截断。
	writeLines(400)
	src2 := openFile(t)
	defer src2.Close()
	sink2, err := NewCommittedFileSink(out)
	if err != nil {
		t.Fatal(err)
	}
	eng2 := NewEngine(cfg, nil)
	if err := eng2.Run(context.Background(), []Source{src2}, sink2); err != nil {
		t.Fatal(err)
	}
	_ = sink2.Close()
	size2 := fileSize(t, ck)

	finalBytes, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	outLines := strings.Split(strings.TrimRight(string(finalBytes), "\n"), "\n")
	if len(outLines) != 400 {
		t.Fatalf("output must keep full history without loss: got %d lines want 400", len(outLines))
	}
	// 阶段 1 已写入的内容逐字节保留（重放被幂等跳过，没有重写/重复）。
	if !strings.HasPrefix(string(finalBytes), string(phase1Bytes)) {
		t.Fatal("phase1 output bytes must be preserved verbatim after restart")
	}
	// 无重复窗口。
	seen := make(map[string]int)
	for _, l := range outLines {
		var r WindowResult
		if err := r.UnmarshalJSON([]byte(l)); err != nil {
			t.Fatalf("bad output line: %v", err)
		}
		seen[resultKey(&r)]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("window %s appears %d times in output", k, n)
		}
	}
	if size2 > size1*2 {
		t.Fatalf("checkpoint grows across restarts: phase1=%dB phase2=%dB", size1, size2)
	}
	t.Logf("conclusion: restart with retention=0 -> out 400/400 lines, no loss, no dup, ckpt %dB->%dB: PASS",
		size1, size2)
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
