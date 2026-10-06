package stream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

// orLines 生成 totalWindows 个分钟窗的事件，每窗 lrKeys 个键、每键 2 条，
// 值带多位小数（1.1/2.2 这类二进制不可精确表示的值），用于跨重启续算时
// 检验 Sum 不存在末位漂移（按比特对比）。
func orLines(totalWindows int) []string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	lines := make([]string, 0, totalWindows*lrKeys*lrPerKey)
	for w := 0; w < totalWindows; w++ {
		for k := 0; k < lrKeys; k++ {
			for j := 0; j < lrPerKey; j++ {
				ts := base.Add(time.Duration(w)*time.Minute +
					time.Duration(j)*time.Second + time.Duration(k)*time.Millisecond)
				lines = append(lines, fmt.Sprintf("w%dk%dj%d,key%d,%s,%s",
					w, k, j, k, ts.Format(time.RFC3339Nano),
					[]string{"1.1", "2.2"}[j]))
			}
		}
	}
	return lines
}

// orWriteFile 重写/追加写输入文件。
func orWriteFile(t *testing.T, path string, totalWindows int) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range orLines(totalWindows) {
		if _, err := f.WriteString(line + "\n"); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// orRunFile 用 CommittedFileSink 跑一档输入到完成（模拟一次进程生命周期）。
func orRunFile(t *testing.T, cfg Config, inPath, outPath string) {
	t.Helper()
	src, err := NewFileSource(inPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	sink, err := NewCommittedFileSink(outPath)
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		t.Errorf("unexpected classified error: %s", ce)
	})
	if err := eng.Run(context.Background(), []Source{src}, sink); err != nil {
		t.Fatal(err)
	}
}

// orReadOutput 读取输出文件，按窗口去重校验后返回 windowKey -> 规范签名。
type orWinKey struct {
	key   string
	start int64
}

func orReadOutput(t *testing.T, path string) (map[orWinKey]string, int64) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	out := make(map[orWinKey]string)
	seqs := make(map[int64]struct{})
	var maxSeq int64
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		var r WindowResult
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("output line not valid json %q: %v", sc.Text(), err)
		}
		k := orWinKey{r.Key, r.WindowStart.UnixNano()}
		if _, dup := out[k]; dup {
			t.Fatalf("window %s@%s appears twice in output file", r.Key, r.WindowStart)
		}
		if _, dup := seqs[r.EmitSeq]; dup {
			t.Fatalf("emit_seq %d repeated in output file", r.EmitSeq)
		}
		seqs[r.EmitSeq] = struct{}{}
		if r.EmitSeq > maxSeq {
			maxSeq = r.EmitSeq
		}
		out[k] = fmt.Sprintf("%d|%x|%x|%x|%s", r.Count,
			math.Float64bits(r.Sum), math.Float64bits(r.Min),
			math.Float64bits(r.Max), r.FirstEventID)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out, maxSeq
}

// TestOutputSurvivesRestartWithFiniteRetention：两档差十倍的窗口总量（300/3000）
// 下，有限保留范围 + CommittedFileSink，经过多次重启（输入不变/追加数据），
// 输出文件与一次性运行逐窗口一致（含小数 Sum 比特），不丢不重、seq 连续。
func TestOutputSurvivesRestartWithFiniteRetention(t *testing.T) {
	for _, total := range []int{300, 3000} {
		t.Run(fmt.Sprintf("windows=%d", total), func(t *testing.T) {
			dir := t.TempDir()
			ck := dir + "/ck.json"
			in := dir + "/in.csv"
			out := dir + "/out.jsonl"
			cfg := lrConfig(ck) // retention=2min，对 300/3000 窗都会大量修剪

			// 阶段 1：从零跑到 total 窗。
			orWriteFile(t, in, total)
			orRunFile(t, cfg, in, out)
			got1, max1 := orReadOutput(t, out)
			if len(got1) != total*lrKeys {
				t.Fatalf("phase1: %d windows in file, want %d", len(got1), total*lrKeys)
			}

			// 阶段 2：输入不变，纯重启（重放只应有界尾部）。
			orRunFile(t, cfg, in, out)
			got2, max2 := orReadOutput(t, out)
			if len(got2) != total*lrKeys {
				t.Fatalf("phase2 restart no-append: %d windows in file, want %d",
					len(got2), total*lrKeys)
			}
			if max2 != max1 {
				t.Fatalf("phase2 max emit_seq changed: %d -> %d", max1, max2)
			}

			// 阶段 3：追加 total/10 个新窗口后再重启两次。
			extra := total / 10
			orWriteFile(t, in, total+extra)
			orRunFile(t, cfg, in, out)
			orRunFile(t, cfg, in, out) // 再来一次纯重启
			got3, max3 := orReadOutput(t, out)
			if len(got3) != (total+extra)*lrKeys {
				t.Fatalf("phase3 after append+2 restarts: %d windows, want %d",
					len(got3), (total+extra)*lrKeys)
			}
			if max3 != int64((total+extra)*lrKeys) {
				t.Fatalf("phase3 max emit_seq=%d, want %d (seq gap across prunes?)",
					max3, (total+extra)*lrKeys)
			}

			// 参照：无检查点、无保留地一次性跑 total+extra 窗。
			refSink := NewMemorySink()
			refCfg := lrConfig("")
			refEng := NewEngine(refCfg, nil)
			if err := refEng.Run(context.Background(),
				[]Source{NewSliceSource("longrun", orLines(total+extra))}, refSink); err != nil {
				t.Fatal(err)
			}
			keys := make([]orWinKey, 0, len(refSink.Snapshot()))
			for _, r := range refSink.Snapshot() {
				k := orWinKey{r.Key, r.WindowStart.UnixNano()}
				keys = append(keys, k)
				want := fmt.Sprintf("%d|%x|%x|%x|%s", r.Count,
					math.Float64bits(r.Sum), math.Float64bits(r.Min),
					math.Float64bits(r.Max), r.FirstEventID)
				if got := got3[k]; got != want {
					t.Fatalf("window %s@%s after restarts: got %s want %s",
						r.Key, r.WindowStart, got, want)
				}
			}
			sort.Slice(keys, func(i, j int) bool {
				if keys[i].start != keys[j].start {
					return keys[i].start < keys[j].start
				}
				return keys[i].key < keys[j].key
			})
			if len(keys) != len(got3) {
				t.Fatalf("reference covers %d windows, output has %d", len(keys), len(got3))
			}
			t.Logf("PASS windows=%d retention=%s: %d windows survive %d restarts (one with +%d appended), "+
				"per-window count/sum(bits)/min(bits)/max(bits)/first equal to one-shot run, max emit_seq=%d",
				total, lrRetention, len(got3), 3, extra, max3)
		})
	}
}

// crashSink 包装 CommittedFileSink：当第一条 emit_seq > crashAfterSeq 的
// 结果（即重启后的第一条“新”结果）到来时，先把该结果行的前半截（无换行）
// 直接追加进输出文件，再返回错误中止 Run —— 精确模拟“检查点已提交、结果
// 写到一半进程崩溃”。
type crashSink struct {
	inner         *CommittedFileSink
	outPath       string
	crashAfterSeq int64
	crashed       bool
}

func (c *crashSink) PrepareResume(fromCheckpoint bool) error {
	return c.inner.PrepareResume(fromCheckpoint)
}

func (c *crashSink) DurableSeq() int64 { return c.inner.DurableSeq() }

func (c *crashSink) Emit(r *WindowResult) error {
	if !c.crashed && r.EmitSeq > c.crashAfterSeq {
		c.crashed = true
		data, err := json.Marshal(r)
		if err != nil {
			return err
		}
		f, err := os.OpenFile(c.outPath, os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return err
		}
		half := append(append([]byte{}, data...), '\n')
		half = half[:len(half)/2] // 半行，不含结尾换行
		if _, err := f.Write(half); err != nil {
			f.Close()
			return err
		}
		f.Close()
		return fmt.Errorf("simulated crash while emitting seq %d", r.EmitSeq)
	}
	return c.inner.Emit(r)
}

func (c *crashSink) Close() error { return c.inner.Close() }

var _ Sink = (*crashSink)(nil)

// orCrashRun 用故障注入汇跑一次；预期在第一条新结果写入时返回错误。
func orCrashRun(t *testing.T, cfg Config, inPath, outPath string, crashAfterSeq int64) {
	t.Helper()
	src, err := NewFileSource(inPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	inner, err := NewCommittedFileSink(outPath)
	if err != nil {
		t.Fatal(err)
	}
	cs := &crashSink{inner: inner, outPath: outPath, crashAfterSeq: crashAfterSeq}
	eng := NewEngine(cfg, nil)
	err = eng.Run(context.Background(), []Source{src}, cs)
	_ = inner.Close()
	if err == nil {
		t.Fatal("expected simulated crash error, got nil")
	}
	if !cs.crashed {
		t.Fatalf("crash sink never triggered; run err=%v", err)
	}
	t.Logf("injected crash at seq %d: %v", crashAfterSeq+1, err)
}

// TestOutputTornLastLineRecovery：检查点已提交、结果行写到一半时崩溃
// （末行残缺、无换行收尾），重启恢复必须保留此前所有完整行，剥掉残缺
// 半行，被截断的那个窗口由检查点重放恰好补写一次；之后追加数据再重启
// 仍然逐窗口一致。
func TestOutputTornLastLineRecovery(t *testing.T) {
	dir := t.TempDir()
	ck := dir + "/ck.json"
	in := dir + "/in.csv"
	out := dir + "/out.jsonl"
	cfg := lrConfig(ck)

	// 阶段 1：完整跑 260 窗（输出 seq 1..N 全部完整）。
	const phase1Windows = 260
	orWriteFile(t, in, phase1Windows)
	orRunFile(t, cfg, in, out)
	good, maxSeq := orReadOutput(t, out)
	if len(good) != phase1Windows*lrKeys {
		t.Fatalf("phase1: %d windows, want %d", len(good), phase1Windows*lrKeys)
	}
	if maxSeq != int64(phase1Windows*lrKeys) {
		t.Fatalf("phase1 max seq=%d", maxSeq)
	}

	// 阶段 2：输入追加到 300 窗，在第一条新结果（seq N+1）写一半时“崩溃”。
	orWriteFile(t, in, 300)
	orCrashRun(t, cfg, in, out, maxSeq)

	// 此时输出文件末尾是半行：确认它确实存在且无换行。
	raw, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if raw[len(raw)-1] == '\n' {
		t.Fatal("expected torn last line without trailing newline")
	}

	// 阶段 3：相同输入重启恢复——完整行原样保留，半行剥除，seq N+1 的窗口
	// 由检查点重放恰好补写一次。
	orRunFile(t, cfg, in, out)
	got3, max3 := orReadOutput(t, out)
	if len(got3) != 300*lrKeys {
		t.Fatalf("after torn-line restart: %d windows, want %d", len(got3), 300*lrKeys)
	}
	if max3 != int64(300*lrKeys) {
		t.Fatalf("after torn-line restart max emit_seq=%d, want %d (dup or gap)",
			max3, 300*lrKeys)
	}
	for k, v := range good {
		if got3[k] != v {
			t.Fatalf("pre-crash window %v changed: got %s want %s", k, got3[k], v)
		}
	}

	// 阶段 4：再追加 40 窗并重启两次：残缺窗口不漏不重，整体与一次性运行一致。
	orWriteFile(t, in, 340)
	orRunFile(t, cfg, in, out)
	orRunFile(t, cfg, in, out)
	got4, max4 := orReadOutput(t, out)
	if len(got4) != 340*lrKeys {
		t.Fatalf("after append: %d windows, want %d", len(got4), 340*lrKeys)
	}
	if max4 != int64(340*lrKeys) {
		t.Fatalf("after append max emit_seq=%d, want %d", max4, 340*lrKeys)
	}
	refSink := NewMemorySink()
	refCfg := lrConfig("")
	refEng := NewEngine(refCfg, nil)
	if err := refEng.Run(context.Background(),
		[]Source{NewSliceSource("longrun", orLines(340))}, refSink); err != nil {
		t.Fatal(err)
	}
	for _, r := range refSink.Snapshot() {
		k := orWinKey{r.Key, r.WindowStart.UnixNano()}
		want := fmt.Sprintf("%d|%x|%x|%x|%s", r.Count,
			math.Float64bits(r.Sum), math.Float64bits(r.Min),
			math.Float64bits(r.Max), r.FirstEventID)
		if got4[k] != want {
			t.Fatalf("window %s@%s mismatch: got %s want %s",
				r.Key, r.WindowStart, got4[k], want)
		}
	}

	// 无换行收尾的行即使恰好可解析也不能信任：删掉最后一行的换行再重启。
	raw, err = os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if raw[len(raw)-1] != '\n' {
		t.Fatal("expected clean trailing newline before parseable-tail case")
	}
	if err := os.WriteFile(out, raw[:len(raw)-1], 0o644); err != nil {
		t.Fatal(err)
	}
	orRunFile(t, cfg, in, out) // 相同输入重启：该窗口重放补写一次
	got5, max5 := orReadOutput(t, out)
	if len(got5) != 340*lrKeys || max5 != int64(340*lrKeys) {
		t.Fatalf("parseable un-newlined tail mishandled: windows=%d maxSeq=%d, want %d/%d",
			len(got5), max5, 340*lrKeys, 340*lrKeys)
	}
	t.Logf("PASS: crash mid-write at seq %d left a torn line; %d earlier complete lines kept intact, "+
		"truncated window replayed once; append to %d windows then 2 restarts match one-shot run; "+
		"parseable un-newlined tail also not trusted",
		maxSeq+1, maxSeq, 340*lrKeys)
}

// TestOutputRejectsCorruptCompleteLines：恢复对账时，若输出文件中存在无法
// 解析的完整行、或 emit_seq 不连续（文件被外部截断/混入其他运行），引擎
// 必须返回错误拒绝启动，避免基于错误的“已持久化前缀”静默跳过结果造成丢失。
func TestOutputRejectsCorruptCompleteLines(t *testing.T) {
	cases := []struct {
		name    string
		damage  func(t *testing.T, lines [][]byte) []byte
		wantErr string
	}{
		{
			name: "corrupt middle line",
			damage: func(t *testing.T, lines [][]byte) []byte {
				cp := append([]byte{}, lines[2]...)
				for i := range cp {
					if cp[i] == ':' {
						cp[i] = 'X' // 破坏 JSON 结构
						break
					}
				}
				lines[2] = cp
				var out []byte
				for _, l := range lines {
					out = append(out, l...)
				}
				return out
			},
			wantErr: "corrupt complete line",
		},
		{
			name: "gap in emit_seq",
			damage: func(t *testing.T, lines [][]byte) []byte {
				// 删掉中间一个完整行（seq 出现断档）。
				var out []byte
				for i, l := range lines {
					if i == 2 {
						continue
					}
					out = append(out, l...)
				}
				return out
			},
			wantErr: "not contiguous",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ck := dir + "/ck.json"
			in := dir + "/in.csv"
			out := dir + "/out.jsonl"
			cfg := lrConfig(ck)

			orWriteFile(t, in, 30)
			orRunFile(t, cfg, in, out)

			raw, err := os.ReadFile(out)
			if err != nil {
				t.Fatal(err)
			}
			var lines [][]byte
			start := 0
			for i := 0; i < len(raw); i++ {
				if raw[i] == '\n' {
					lines = append(lines, append([]byte{}, raw[start:i+1]...))
					start = i + 1
				}
			}
			if len(lines) < 4 {
				t.Fatalf("need >=4 lines, got %d", len(lines))
			}
			if err := os.WriteFile(out, tc.damage(t, lines), 0o644); err != nil {
				t.Fatal(err)
			}

			src, err := NewFileSource(in)
			if err != nil {
				t.Fatal(err)
			}
			defer src.Close()
			sink, err := NewCommittedFileSink(out)
			if err != nil {
				t.Fatal(err)
			}
			eng := NewEngine(cfg, nil)
			err = eng.Run(context.Background(), []Source{src}, sink)
			sink.Close()
			if err == nil {
				t.Fatal("expected engine to reject damaged output file, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not mention %q", err.Error(), tc.wantErr)
			}
			t.Logf("decision: damaged output rejected: %v", err)
		})
	}
}

// TestOutputRestartStateStaysBounded：追加式文件汇下多次重启，内存/检查点
// 状态仍只与保留尾部相称（不随累计窗口总量增长），而输出文件保留全部历史。
// 用“边追加重启”的方式把累计窗口从 small 推到 large：任意时刻检查点体积
// 都与一次性跑 small 档相当，最终输出行数 = large 档全部窗口。
func TestOutputRestartStateStaysBounded(t *testing.T) {
	dir := t.TempDir()
	ck := dir + "/ck.json"
	in := dir + "/in.csv"
	out := dir + "/out.jsonl"
	cfg := lrConfig(ck)

	const small, large = 300, 3000
	orWriteFile(t, in, small)
	orRunFile(t, cfg, in, out)
	fiSmall, err := os.Stat(ck)
	if err != nil {
		t.Fatal(err)
	}

	var maxCkpt int64 = fiSmall.Size()
	for n := small + 300; n <= large; n += 300 {
		orWriteFile(t, in, n)
		orRunFile(t, cfg, in, out)
		fi, err := os.Stat(ck)
		if err != nil {
			t.Fatal(err)
		}
		if fi.Size() > maxCkpt {
			maxCkpt = fi.Size()
		}
	}

	got, maxSeq := orReadOutput(t, out)
	if len(got) != large*lrKeys {
		t.Fatalf("output covers %d windows, want %d", len(got), large*lrKeys)
	}
	if maxSeq != int64(large*lrKeys) {
		t.Fatalf("max emit_seq=%d, want %d", maxSeq, large*lrKeys)
	}
	// 一次性跑 large 档的检查点体积作为参照：边重启累积到 large，
	// 检查点峰值不得显著超过一次跑到 large 的有界体积。
	orWriteFile(t, dir+"/in-ref.csv", large)
	refCfg := lrConfig(dir + "/ck-ref.json")
	orRunFile(t, refCfg, dir+"/in-ref.csv", dir+"/out-ref.jsonl")
	fiLarge, err := os.Stat(dir + "/ck-ref.json")
	if err != nil {
		t.Fatal(err)
	}
	if maxCkpt > fiLarge.Size()*2 {
		t.Fatalf("checkpoint grew across restarts: max %d bytes vs one-shot large %d bytes",
			maxCkpt, fiLarge.Size())
	}
	outFi, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("PASS: %d incremental restarts from %d to %d windows: max checkpoint=%d bytes "+
		"(one-shot large=%d bytes), output file=%d bytes holding all %d results exactly once",
		(large-small)/300, small, large, maxCkpt, fiLarge.Size(), outFi.Size(), len(got))
}
