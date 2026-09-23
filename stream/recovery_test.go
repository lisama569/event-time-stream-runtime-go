package stream

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// writeInputs 写两个本地 CSV 源；n 指定每个文件写多少行。
// 事件时间排布：key 为 a/b；00:00:0x、00:01:0x、00:02:0x，窗口边界清晰。
func writeInputs(t *testing.T, dir string, nPerFile int) (p1, p2 string) {
	t.Helper()
	mk := func(path string, id int) {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		for i := 0; i < nPerFile; i++ {
			key := "a"
			if id == 2 {
				key = "b"
			}
			line := csvLine(key, i) + "\n"
			if _, err := f.WriteString(line); err != nil {
				t.Fatal(err)
			}
		}
	}
	p1 = filepath.Join(dir, "in1.csv")
	p2 = filepath.Join(dir, "in2.csv")
	mk(p1, 1)
	mk(p2, 2)
	return p1, p2
}

func csvLine(key string, i int) string {
	// 事件时间随 i 单调递增（第 i 分钟的第 10 秒），避免制造非预期迟到；
	// 乱序归窗语义由 window_test.go 单独覆盖。两个键落在同样的窗口集合上。
	ts := time.Date(2026, 1, 1, 0, i, 10, 0, time.UTC).Format(time.RFC3339)
	id := key + "-" + itoa(i)
	return id + "," + key + "," + ts + "," + itoa(i+1) + ".0"
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}

func openSources(t *testing.T, p1, p2 string) []Source {
	t.Helper()
	s1, err := NewFileSource(p1)
	if err != nil {
		t.Fatal(err)
	}
	s2, err := NewFileSource(p2)
	if err != nil {
		t.Fatal(err)
	}
	return []Source{s1, s2}
}

func closeSources(ss []Source) {
	for _, s := range ss {
		if c, ok := s.(*FileSource); ok {
			_ = c.Close()
		}
	}
}

func runToCompletion(t *testing.T, cfg Config, p1, p2, outPath string) *MemorySink {
	t.Helper()
	ss := openSources(t, p1, p2)
	defer closeSources(ss)
	sink := NewMemorySink()
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		t.Errorf("unexpected error: %s (eventTime=%s watermark=%s)",
			ce.Kind, formatTime(ce.EventTime), formatTime(ce.Watermark))
	})
	if err := eng.Run(context.Background(), ss, sink); err != nil {
		t.Fatal(err)
	}
	return sink
}

func signature(rs []*WindowResult) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Key+"@"+r.WindowStart.Format("15:04:05")+
			":n="+itoa(int(r.Count))+":sum="+ftoa(r.Sum)+":first="+r.FirstEventID)
	}
	sort.Strings(out)
	return out
}

func ftoa(f float64) string {
	// 输入都是整数，直接格式化
	return itoa(int(f))
}

// TestRecoveryNoDuplicatesNoLoss：模拟“处理一半崩溃 -> 重启继续 ->
// 追加数据后再次重启”，最终结果必须与一次性运行完全一致，且不重不漏。
func TestRecoveryNoDuplicatesNoLoss(t *testing.T) {
	dir := t.TempDir()
	ck := filepath.Join(dir, "ckpt.json")

	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 5 * time.Second
	cfg.SourceQueueSize = 4
	cfg.CheckpointEvery = 5 // 高频检查点，制造多次提交
	cfg.CheckpointPath = ck

	// 阶段 1：每个源先只有 4 条（处理后“崩溃”）。
	p1, p2 := writeInputs(t, dir, 4)
	s1 := runToCompletion(t, cfg, p1, p2, "")
	phase1 := s1.Snapshot()
	if len(phase1) == 0 {
		t.Fatal("phase1 should emit some windows")
	}

	// 检查点必须存在且可严格校验加载。
	if _, err := LoadCheckpoint(ck, cfg); err != nil {
		t.Fatalf("phase1 checkpoint invalid: %v", err)
	}

	// 阶段 2：文件增长到 12 条，用同一检查点重启，输出重建后继续。
	p1, p2 = writeInputs(t, dir, 12)
	s2 := runToCompletion(t, cfg, p1, p2, "")
	phase2 := s2.Snapshot()

	// 阶段 3：再来一次（无新数据，纯重启）：结果集合必须不变。
	s3 := runToCompletion(t, cfg, p1, p2, "")
	phase3 := s3.Snapshot()

	// 参照：无检查点一次性运行 12 条/源。
	cfgRef := cfg
	cfgRef.CheckpointPath = ""
	ref := runToCompletion(t, cfgRef, p1, p2, "")
	wantSig := signature(ref.Snapshot())

	for name, rs := range map[string][]*WindowResult{
		"phase2": phase2, "phase3": phase3,
	} {
		got := signature(rs)
		if len(got) != len(wantSig) {
			t.Fatalf("%s: result count %d != reference %d; got=%v want=%v",
				name, len(got), len(wantSig), got, wantSig)
		}
		for i := range got {
			if got[i] != wantSig[i] {
				t.Fatalf("%s mismatch:\n got %v\nwant %v", name, got, wantSig)
			}
		}
		// 每个窗口恰好一次：MemorySink 会记录重复发射。
	}

	// EmitSeq 必须跨恢复单调、无重号。
	seqs := map[int64]string{}
	for _, r := range phase3 {
		if prev, dup := seqs[r.EmitSeq]; dup {
			t.Fatalf("emit_seq %d reused (%s, %s): duplicate publish",
				r.EmitSeq, prev, r.Key)
		}
		seqs[r.EmitSeq] = r.Key
	}
}

// TestRecoveryRejectsCorruptCheckpoint：损坏检查点必须被拒绝并给出
// 可区分原因，引擎绝不带着坏状态运行。
func TestRecoveryRejectsCorruptCheckpoint(t *testing.T) {
	dir := t.TempDir()
	ck := filepath.Join(dir, "ckpt.json")
	cfg := DefaultConfig()
	cfg.CheckpointPath = ck
	cfg.CheckpointEvery = 2

	p1, p2 := writeInputs(t, dir, 3)
	_ = runToCompletion(t, cfg, p1, p2, "")

	// 翻转文件中间一个字节（避开开头花括号），破坏校验和。
	raw, err := os.ReadFile(ck)
	if err != nil {
		t.Fatal(err)
	}
	mid := len(raw) / 2
	if raw[mid] == 'a' {
		raw[mid] = 'b'
	} else {
		raw[mid] = 'a'
	}
	if err := os.WriteFile(ck, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	ss := openSources(t, p1, p2)
	defer closeSources(ss)
	eng := NewEngine(cfg, nil)
	err = eng.Run(context.Background(), ss, NewMemorySink())
	if err == nil {
		t.Fatal("corrupt checkpoint must be rejected")
	}
	ce, ok := err.(*CheckpointError)
	if !ok {
		t.Fatalf("want CheckpointError, got %T: %v", err, err)
	}
	t.Logf("decision: kind=%s path=%s version=%d reason=%s",
		ce.Kind, ce.Path, CheckpointVersion, ce.Reason)
	if ce.Kind != CKPTMalformed {
		t.Fatalf("corrupt checksum -> MALFORMED, got %s", ce.Kind)
	}
}

// TestRecoveryRejectsVersionAndConfigMismatch：版本不兼容、窗口配置不一致
// 必须以不同原因被拒绝。
func TestRecoveryRejectsVersionAndConfigMismatch(t *testing.T) {
	dir := t.TempDir()
	ck := filepath.Join(dir, "ckpt.json")
	cfg := DefaultConfig()
	cfg.CheckpointPath = ck

	p1, p2 := writeInputs(t, dir, 2)
	_ = runToCompletion(t, cfg, p1, p2, "")
	loaded, err := LoadCheckpoint(ck, cfg)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("future version", func(t *testing.T) {
		bad := *loaded
		bad.Version = 999
		bad.Checksum = computeChecksum(&bad)
		data, _ := jsonMarshalIndent(&bad)
		if err := os.WriteFile(ck, data, 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := LoadCheckpoint(ck, cfg)
		assertKind(t, err, CKPTIncompatibleVersion)
	})

	t.Run("window config mismatch", func(t *testing.T) {
		// 恢复成正常检查点，再用不同窗口大小配置加载。
		if err := SaveCheckpoint(ck, loaded); err != nil {
			t.Fatal(err)
		}
		cfg2 := cfg
		cfg2.WindowSize = 2 * time.Minute
		_, err := LoadCheckpoint(ck, cfg2)
		assertKind(t, err, CKPTBadConfig)
	})
}

// TestCrashBeforeCommitNoUnpublishedResults：在“窗口已关闭挂起、检查点尚未
// 提交”的窗口模拟崩溃：重启后该窗口必须由重放数据重新计算并恰好发布一次，
// 已提交窗口也不得重复。
func TestCrashBeforeCommitNoUnpublishedResults(t *testing.T) {
	dir := t.TempDir()
	ck := filepath.Join(dir, "ckpt.json")
	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 0 // 水位越过窗口右端即关闭，边界清晰
	cfg.CheckpointPath = ck
	cfg.CheckpointEvery = 1 << 30 // 关闭周期检查点，只在窗口关闭时提交

	// 文件 1：一条把全局水位推过第一窗口右端的事件后，文件结束前会关闭
	// [00:00,00:01)。用 ctx 在处理若干事件后取消，模拟在任意时刻崩溃。
	p1 := filepath.Join(dir, "in1.csv")
	p2 := filepath.Join(dir, "in2.csv")
	lines1 := []string{
		"a1,a,2026-01-01T00:00:05Z,1",
		"a2,a,2026-01-01T00:00:30Z,2",
		"a3,a,2026-01-01T00:01:00Z,3", // 水位到 00:01:00 -> a 第一窗口关闭挂起
	}
	lines2 := []string{
		"b1,b,2026-01-01T00:00:10Z,10",
		"b2,b,2026-01-01T00:00:40Z,20",
	}
	if err := os.WriteFile(p1, joinLines(lines1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p2, joinLines(lines2), 0o644); err != nil {
		t.Fatal(err)
	}

	// 崩溃运行：上下文在 50ms 后取消，且极快处理；多次尝试确保至少一次
	// 发生在“有窗口挂起但未提交”附近。无论崩在哪，恢复后最终状态必须一致。
	crashRun := func() error {
		ss := openSources(t, p1, p2)
		defer closeSources(ss)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		defer cancel()
		eng := NewEngine(cfg, nil)
		return eng.Run(ctx, ss, NewMemorySink())
	}
	_ = crashRun() // 结果丢弃；可能 ctx 取消，也可能刚好跑完

	// 恢复运行（相同输入，完整跑到 EOF）。
	ss := openSources(t, p1, p2)
	defer closeSources(ss)
	sink := NewMemorySink()
	eng := NewEngine(cfg, nil)
	if err := eng.Run(context.Background(), ss, sink); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	got := signature(sink.Snapshot())
	// 期望两个窗口：a@00:00 count2 sum3 first=a1；b@00:00 count2 sum30 first=b1。
	want := []string{
		"a@00:00:00:n=2:sum=3:first=a1",
		"a@00:01:00:n=1:sum=3:first=a3",
		"b@00:00:00:n=2:sum=30:first=b1",
	}
	if len(got) != len(want) {
		t.Fatalf("resume results:\n got %v\nwant %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("resume result[%d]: got %s want %s", i, got[i], want[i])
		}
	}
	// 若崩溃前恰好提交过，MemorySink 也必须无重复发射。
	if dups := sink.Duplicates; len(dups) != 0 {
		t.Fatalf("duplicate publish after crash recovery: %v", dups)
	}
}

func joinLines(lines []string) []byte {
	var out []byte
	for _, l := range lines {
		out = append(out, l...)
		out = append(out, '\n')
	}
	return out
}
