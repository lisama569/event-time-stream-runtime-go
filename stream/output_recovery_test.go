package stream

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"
)

// 本文件覆盖“输出文件 + 检查点 + 有限保留范围”组合下的重启恢复语义：
//   - 重启（含追加数据、多次重启）后输出文件必须保留全部历史结果，不丢不重；
//   - 写入过程中崩溃留下残缺尾行时，恢复后完整行原样保留、半行被裁掉；
//   - 跨重启续算的窗口聚合值（含小数）与一次性运行逐比特一致。

// runFileSinkPhase 用提交式文件汇跑一个阶段到 EOF，返回引擎（可取统计）。
func runFileSinkPhase(t *testing.T, cfg Config, src Source, outPath string) *Engine {
	t.Helper()
	sink, err := NewCommittedFileSink(outPath)
	if err != nil {
		t.Fatal(err)
	}
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		t.Errorf("unexpected classified error: %s", ce)
	})
	if err := eng.Run(context.Background(), []Source{src}, sink); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	return eng
}

// readOutputFile 读取输出文件并逐行解析；任何无法解析的行都视为
// 残缺写入污染，直接判失败。
func readOutputFile(t *testing.T, path string) []*WindowResult {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := strings.TrimRight(string(data), "\n")
	if body == "" {
		return nil
	}
	lines := strings.Split(body, "\n")
	out := make([]*WindowResult, 0, len(lines))
	for i, ln := range lines {
		r := new(WindowResult)
		if err := json.Unmarshal([]byte(ln), r); err != nil {
			t.Fatalf("output line %d is not a complete valid result "+
				"(partial write polluted output?): %v", i+1, err)
		}
		out = append(out, r)
	}
	return out
}

// runMemoryReference 无检查点一次性运行，给出参照结果。
func runMemoryReference(t *testing.T, cfg Config, lines []string) *MemorySink {
	t.Helper()
	cfg.CheckpointPath = ""
	sink := NewMemorySink()
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		t.Errorf("reference run error: %s", ce)
	})
	if err := eng.Run(context.Background(),
		[]Source{NewSliceSource("longrun", lines)}, sink); err != nil {
		t.Fatal(err)
	}
	return sink
}

// assertOutputMatchesReference 校验输出文件：行数正确、无重复窗口、
// emit_seq 严格递增（全局发射序列的一段完整前缀），且逐窗口聚合值
// （count/sum/min/max 按比特、首事件）与一次性运行参照完全一致。
func assertOutputMatchesReference(t *testing.T, got []*WindowResult,
	ref *MemorySink, wantWindows int) {
	t.Helper()
	if len(got) != wantWindows {
		t.Fatalf("output has %d lines, want %d (lost or extra results)",
			len(got), wantWindows)
	}
	type agg struct {
		count         int64
		sum, min, max uint64
		first         string
	}
	pack := func(r *WindowResult) agg {
		return agg{r.Count, math.Float64bits(r.Sum), math.Float64bits(r.Min),
			math.Float64bits(r.Max), r.FirstEventID}
	}
	refMap := make(map[WindowID]agg, wantWindows)
	for _, r := range ref.Snapshot() {
		refMap[WindowID{Key: r.Key, Start: r.WindowStart}] = pack(r)
	}
	if len(refMap) != wantWindows {
		t.Fatalf("reference itself has %d unique windows, want %d",
			len(refMap), wantWindows)
	}
	seen := make(map[WindowID]struct{}, wantWindows)
	var prevSeq int64
	for _, r := range got {
		id := WindowID{Key: r.Key, Start: r.WindowStart}
		if _, dup := seen[id]; dup {
			t.Fatalf("window %s@%s appears twice in output file", r.Key, r.WindowStart)
		}
		seen[id] = struct{}{}
		if r.EmitSeq <= prevSeq {
			t.Fatalf("emit_seq not strictly increasing in output: ...%d then %d",
				prevSeq, r.EmitSeq)
		}
		prevSeq = r.EmitSeq
		want, ok := refMap[id]
		if !ok {
			t.Fatalf("output contains window %s@%s absent from reference",
				r.Key, r.WindowStart)
		}
		if g := pack(r); g != want {
			t.Fatalf("window %s@%s aggregate mismatch after recovery: "+
				"got %+v want %+v", r.Key, r.WindowStart, g, want)
		}
	}
}

// TestOutputRestartRetainsAllResults：两档差 10 倍的窗口总量（300/3000）、
// 有限保留范围下，跑一部分 -> 重启（追加数据）-> 再重启（输入不变），
// 输出文件必须始终包含全部已产出结果，与一次性运行逐窗口一致，不丢不重。
func TestOutputRestartRetainsAllResults(t *testing.T) {
	sizes := map[int]int64{} // totalWindows -> 最终检查点字节数（有界性观测）
	for _, total := range []int{300, 3000} {
		total := total
		t.Run(fmt.Sprintf("windows=%d", total), func(t *testing.T) {
			dir := t.TempDir()
			ck := dir + "/ck.json"
			out := dir + "/out.jsonl"
			in := dir + "/in.csv"
			cfg := lrConfig(ck)

			// 阶段 1：先跑前 3/5 的窗口到 EOF。
			cut := total * 3 / 5
			eng1 := runFileSinkPhase(t, cfg, lrWriteFile(t, in, cut), out)
			phase1 := readOutputFile(t, out)
			if len(phase1) != cut*lrKeys {
				t.Fatalf("phase1: output has %d lines, want %d",
					len(phase1), cut*lrKeys)
			}
			retained := eng1.Status().EmittedRetained
			if int64(len(phase1)) <= retained {
				t.Fatalf("phase1: output lines %d should far exceed retained tail %d "+
					"(retention must be a small fraction of history)", len(phase1), retained)
			}
			t.Logf("phase1: %d windows emitted to file, checkpoint retains only tail of %d",
				len(phase1), retained)

			// 阶段 2：输入追加到全部窗口，同一检查点+同一输出文件重启。
			runFileSinkPhase(t, cfg, lrWriteFile(t, in, total), out)
			phase2 := readOutputFile(t, out)
			if len(phase2) != total*lrKeys {
				t.Fatalf("phase2: output has %d lines, want %d "+
					"(history beyond retention must survive restart)",
					len(phase2), total*lrKeys)
			}

			// 阶段 3：输入不变的纯重启，输出必须一字节不多一字节不少。
			runFileSinkPhase(t, cfg, lrWriteFile(t, in, total), out)
			phase3 := readOutputFile(t, out)
			if len(phase3) != len(phase2) {
				t.Fatalf("phase3 (idempotent restart): %d lines, want %d (rewrote history?)",
					len(phase3), len(phase2))
			}

			// 与一次性运行逐窗口一致、不丢不重、emit_seq 严格递增。
			ref := runMemoryReference(t, cfg, lrLines(total))
			assertOutputMatchesReference(t, phase3, ref, total*lrKeys)

			fi, err := os.Stat(ck)
			if err != nil {
				t.Fatal(err)
			}
			sizes[total] = fi.Size()
			t.Logf("PASS windows=%d: 3 phases (partial -> appended -> idle restart), "+
				"output %d lines == one-shot reference, no loss/dup; final checkpoint=%d bytes",
				total, len(phase3), fi.Size())
		})
	}
	// 有界性：总量差 10 倍，检查点体积不得随之增长。
	if sizes[3000] > sizes[300]*2 || sizes[300] > sizes[3000]*2 {
		t.Fatalf("checkpoint size grows with total windows: 300 -> %dB, 3000 -> %dB",
			sizes[300], sizes[3000])
	}
	t.Logf("PASS boundedness: checkpoint 300w=%dB vs 3000w=%dB (scale-independent)",
		sizes[300], sizes[3000])
}

// TestOutputCrashMidWritePartialLine：模拟“写结果过程中崩掉、最后一行
// 只写了一半”。两种形态：尾部多出一截不完整的 JSON；最后一行完整结果被
// 拦腰截断。重启恢复后：完整行原样保留、半行被裁掉不污染后续输出、
// 被截断的窗口由检查点重放恰好补齐一次，最终与一次性运行一致。
func TestOutputCrashMidWritePartialLine(t *testing.T) {
	const total = 120

	setup := func(t *testing.T) (dir, ck, out, in string, cfg Config) {
		t.Helper()
		dir = t.TempDir()
		ck, out, in = dir+"/ck.json", dir+"/out.jsonl", dir+"/in.csv"
		cfg = lrConfig(ck)
		// 阶段 1：完整跑完全部窗口，输出文件与检查点都就绪。
		runFileSinkPhase(t, cfg, lrWriteFile(t, in, total), out)
		return dir, ck, out, in, cfg
	}

	t.Run("trailing garbage partial line", func(t *testing.T) {
		_, _, out, in, cfg := setup(t)
		before := readOutputFile(t, out)
		// 崩溃瞬间：下一条结果只写了一半（无换行、JSON 不完整）。
		f, err := os.OpenFile(out, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(`{"key":"key3","window_start":"2026-01-01T01:5`); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}

		// 重启（输入不变）：半行必须被裁掉，且不得因此重写任何已落盘结果。
		runFileSinkPhase(t, cfg, lrWriteFile(t, in, total), out)
		after := readOutputFile(t, out)
		if len(after) != len(before) {
			t.Fatalf("after recovery: %d lines, want %d (garbage survived or history rewritten)",
				len(after), len(before))
		}
		ref := runMemoryReference(t, cfg, lrLines(total))
		assertOutputMatchesReference(t, after, ref, total*lrKeys)
		t.Logf("PASS trailing-garbage: partial line discarded, %d complete lines "+
			"preserved as-is, output == one-shot reference", len(after))
	})

	t.Run("last result line cut in half", func(t *testing.T) {
		_, _, out, in, cfg := setup(t)
		before := readOutputFile(t, out)
		cut := before[len(before)-1] // 被拦腰截断的那条结果（emit_seq 最大）

		fi, err := os.Stat(out)
		if err != nil {
			t.Fatal(err)
		}
		// 崩溃瞬间：最后一行写到一半（截掉 41 字节，JSON 不再完整）。
		if err := os.Truncate(out, fi.Size()-41); err != nil {
			t.Fatal(err)
		}

		// 重启（输入不变）：被截断的窗口在检查点保留尾部中，
		// 重放时必须恰好补写一次，其余完整行原样保留。
		runFileSinkPhase(t, cfg, lrWriteFile(t, in, total), out)
		after := readOutputFile(t, out)
		if len(after) != len(before) {
			t.Fatalf("after recovery: %d lines, want %d (cut window lost or duplicated)",
				len(after), len(before))
		}
		last := after[len(after)-1]
		if last.Key != cut.Key || !last.WindowStart.Equal(cut.WindowStart) ||
			last.EmitSeq != cut.EmitSeq {
			t.Fatalf("rewritten tail mismatch: got %s@%s seq=%d, want %s@%s seq=%d",
				last.Key, last.WindowStart, last.EmitSeq, cut.Key, cut.WindowStart, cut.EmitSeq)
		}
		ref := runMemoryReference(t, cfg, lrLines(total))
		assertOutputMatchesReference(t, after, ref, total*lrKeys)
		t.Logf("PASS half-written-line: window %s@%s (emit_seq=%d) rewritten exactly once "+
			"from checkpoint replay, %d complete lines preserved, output == one-shot reference",
			cut.Key, cut.WindowStart, cut.EmitSeq, len(before)-1)
	})
}

// writeLinesFile 把给定事件行写入文件并返回可恢复的文件源。
func writeLinesFile(t *testing.T, path string, lines []string) *FileSource {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range lines {
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

// decimalLines 生成带非精确二进制小数值（0.1/0.2/0.3）的单调事件流，
// 用于验证跨重启续算的浮点聚合末位不漂移。
func decimalLines(totalWindows int) []string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	vals := []string{"0.1", "0.2", "0.3"}
	lines := make([]string, 0, totalWindows*3*3)
	for w := 0; w < totalWindows; w++ {
		for k := 0; k < 3; k++ {
			for j := 0; j < 3; j++ {
				ts := base.Add(time.Duration(w)*time.Minute +
					time.Duration(j)*time.Second + time.Duration(k)*time.Millisecond)
				lines = append(lines, fmt.Sprintf("d%dk%dj%d,key%d,%s,%s",
					w, k, j, k, ts.Format(time.RFC3339Nano), vals[j]))
			}
		}
	}
	return lines
}

// TestOutputRestartDecimalDeterminism：在处理中途“崩溃”（取消上下文），
// 让一批窗口带着部分事件进入检查点、重启后继续接收同一窗口的剩余事件。
// 跨重启续算出的每个窗口聚合值（含小数 Sum 的比特位）必须与从零一次性
// 运行完全一致，且输出不丢不重。
func TestOutputRestartDecimalDeterminism(t *testing.T) {
	const total = 600
	lines := decimalLines(total)
	totalEvents := int64(len(lines))

	dir := t.TempDir()
	ck := dir + "/ck.json"
	out := dir + "/out.jsonl"
	in := dir + "/in.csv"
	cfg := lrConfig(ck)
	cfg.CheckpointEvery = 20 // 高频检查点：崩溃现场附近必有含打开窗口的快照

	// 阶段 1：跑到约 30% 事件时取消上下文，模拟崩溃（窗口半开进检查点）。
	src := writeLinesFile(t, in, lines)
	sink1, err := NewCommittedFileSink(out)
	if err != nil {
		t.Fatal(err)
	}
	eng1 := NewEngine(cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if eng1.Status().Ingested >= totalEvents*3/10 {
					cancel()
					return
				}
			}
		}
	}()
	runErr := eng1.Run(ctx, []Source{src}, sink1)
	close(stop)
	_ = sink1.Close()
	if runErr == nil {
		t.Fatal("phase1 should have been cancelled mid-run, not completed")
	}
	ing1 := eng1.Report().Ingested
	if ing1 <= 0 || ing1 >= totalEvents {
		t.Fatalf("phase1 ingested %d of %d events: not a mid-run crash", ing1, totalEvents)
	}
	ckpt, err := LoadCheckpoint(ck, cfg)
	if err != nil {
		t.Fatalf("phase1 checkpoint invalid: %v", err)
	}
	if len(ckpt.Windows) == 0 {
		t.Fatalf("phase1 checkpoint has no open windows; " +
			"test cannot exercise cross-restart accumulation")
	}
	t.Logf("phase1: crashed after %d/%d events, checkpoint holds %d open windows "+
		"with partial decimal values", ing1, totalEvents, len(ckpt.Windows))

	// 阶段 2：同一输入重启，续算到 EOF。
	runFileSinkPhase(t, cfg, writeLinesFile(t, in, lines), out)
	got := readOutputFile(t, out)

	// 参照：同一批输入从零一次性运行。
	ref := runMemoryReference(t, cfg, lines)
	assertOutputMatchesReference(t, got, ref, total*3)
	t.Logf("PASS: %d windows resumed across crash, decimal aggregates bit-identical "+
		"to one-shot run (sum/min/max compared by Float64bits), no loss/dup", len(got))
}
