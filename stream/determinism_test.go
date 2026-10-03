package stream

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// detEvent 是确定性测试的一条逻辑事件。
type detEvent struct {
	id    string
	key   string
	min   int // 第几分钟（窗口）
	sec   int // 窗口内秒
	value string
}

// detEvents 构造一批逻辑事件：多键、多窗口、带小数值，
// 其中包含一组“float64 顺序敏感”的值（1e16, 1, -1e16）：
// 按 (1e16 + 1) + -1e16 顺序累加得 0，按 1e16 + (1 + -1e16) 得 1；
// 精确累加器必须无论顺序都得到 1。
func detEvents() []detEvent {
	var evs []detEvent
	id := 0
	mk := func(key string, min, sec int, v string) {
		evs = append(evs, detEvent{id: "d" + itoa(id), key: key, min: min, sec: sec, value: v})
		id++
	}
	keys := []string{"a", "b", "c"}
	vals := []string{"0.1", "0.2", "0.3", "0.7", "1.5", "2.25"}
	for m := 0; m < 10; m++ {
		for ki, k := range keys {
			for vi, v := range vals {
				mk(k, m, (ki*7+vi*11)%60, v)
			}
		}
	}
	// 顺序敏感三元组：全部落在 key=a 的第 0 窗口。
	mk("a", 0, 1, "1e16")
	mk("a", 0, 2, "1")
	mk("a", 0, 3, "-1e16")
	return evs
}

func (e detEvent) line() string {
	ts := time.Date(2026, 1, 1, 0, e.min, e.sec, 0, time.UTC).Format(time.RFC3339)
	return e.id + "," + e.key + "," + ts + "," + e.value
}

// detCfg 返回确定性测试配置：超大允许迟到保证任何到达顺序都不会
// 误伤迟到丢弃（逻辑输入集合恒定）；确定性 ClosedAt 使输出可逐字节对比。
func detCfg() Config {
	cfg := DefaultConfig()
	cfg.WindowSize = time.Minute
	cfg.AllowedLateness = 24 * time.Hour
	cfg.DeterministicClosedAt = true
	cfg.CheckpointPath = ""
	return cfg
}

// runDeterministic 把事件按给定方式分配到 nSrc 个源并运行，返回 JSONL 输出字节。
func runDeterministic(t *testing.T, evs []detEvent, nSrc int, shuffleSeed int64,
	duplicate bool) []byte {
	t.Helper()
	lines := make([]string, 0, len(evs)*2)
	for _, e := range evs {
		lines = append(lines, e.line())
		if duplicate {
			lines = append(lines, e.line()) // 重复投递：必须被幂等忽略
		}
	}
	rnd := rand.New(rand.NewSource(shuffleSeed))
	rnd.Shuffle(len(lines), func(i, j int) { lines[i], lines[j] = lines[j], lines[i] })

	var sources []Source
	chunks := make([][]string, nSrc)
	for i, l := range lines {
		chunks[i%nSrc] = append(chunks[i%nSrc], l)
	}
	for i, c := range chunks {
		sources = append(sources, NewSliceSource(fmt.Sprintf("src-%d", i), c))
	}

	var buf bytes.Buffer
	sink := NewJSONSink(&buf)
	eng := NewEngine(detCfg(), nil)
	if err := eng.Run(context.Background(), sources, sink); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// TestDeterministicResultsRegardlessOfOrder：同一批逻辑输入，无论
// 到达顺序如何打乱、来自几个源、是否重复投递、重复运行多少次，
// 输出必须逐字节一致；小数累加不得因顺序产生细微差别。
func TestDeterministicResultsRegardlessOfOrder(t *testing.T) {
	evs := detEvents()
	t.Logf("scenario: logical events=%d, keys=3, windows=10/key, decimals + order-sensitive triplet",
		len(evs))

	ref := runDeterministic(t, evs, 1, 1, false)
	runs := []struct {
		name      string
		nSrc      int
		seed      int64
		duplicate bool
	}{
		{"shuffled-3-sources", 3, 42, false},
		{"shuffled-2-sources-other-seed", 2, 7, false},
		{"duplicated-delivery-4-sources", 4, 99, true},
		{"single-source-reordered", 1, 1234, false},
	}
	for _, r := range runs {
		got := runDeterministic(t, evs, r.nSrc, r.seed, r.duplicate)
		if !bytes.Equal(got, ref) {
			t.Fatalf("%s: output differs from reference\n--- ref ---\n%s\n--- got ---\n%s",
				r.name, ref, got)
		}
		t.Logf("scenario %-32s -> byte-identical to reference (%d bytes): PASS",
			r.name, len(got))
	}

	// 重复运行参照本身，也必须逐字节一致。
	again := runDeterministic(t, evs, 1, 1, false)
	if !bytes.Equal(again, ref) {
		t.Fatal("re-running the same input must be byte-identical")
	}
	t.Logf("scenario %-32s -> byte-identical: PASS", "rerun-reference")

	// 顺序敏感三元组：精确和必须是 1（float64 顺序累加会得到 0 或 1 依赖顺序）。
	sink := NewMemorySink()
	eng := NewEngine(detCfg(), nil)
	src := NewSliceSource("s", []string{
		"x1,a,2026-01-01T00:00:01Z,1e16",
		"x2,a,2026-01-01T00:00:02Z,1",
		"x3,a,2026-01-01T00:00:03Z,-1e16",
	})
	if err := eng.Run(context.Background(), []Source{src}, sink); err != nil {
		t.Fatal(err)
	}
	rs := sink.Snapshot()
	if len(rs) != 1 {
		t.Fatalf("want 1 window, got %d", len(rs))
	}
	if rs[0].Sum != 1 {
		t.Fatalf("order-sensitive sum: got %v want exactly 1 (1e16+1-1e16)", rs[0].Sum)
	}
	t.Logf("conclusion: order-sensitive 1e16+1-1e16 sum = %v (exact, order-independent): PASS",
		rs[0].Sum)
}
