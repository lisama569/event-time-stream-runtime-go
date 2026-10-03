package stream

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// TestDeterministicSumOrderIndependent 直接验证求和原语：
// 同一多重集合任意打乱，结果逐比特一致；且对已知用例给出精确值。
func TestDeterministicSumOrderIndependent(t *testing.T) {
	// 1e16 处 ulp=2：朴素顺序累加对顺序敏感（见下），是有效的试金石。
	vals := []float64{1e16, 1, 1, -1e16, 0.1, 0.2, 0.3, -0.1, -0.2, -0.3}

	// 先确认朴素累加确实随顺序变化（否则本用例没有区分度）。
	naive := func(vs []float64) float64 {
		var s float64
		for _, v := range vs {
			s += v
		}
		return s
	}
	forward := naive(vals)
	backward := naive([]float64{-0.3, -0.2, -0.1, 0.3, 0.2, 0.1, -1e16, 1, 1, 1e16})
	if math.Float64bits(forward) == math.Float64bits(backward) {
		t.Fatalf("naive sum unexpectedly order-independent on this fixture; "+
			"test would be vacuous (forward=%v backward=%v)", forward, backward)
	}
	t.Logf("naive sum differs by order: forward=%v backward=%v", forward, backward)

	want := math.Float64bits(deterministicSum(vals))
	rng := rand.New(rand.NewSource(42))
	for trial := 0; trial < 200; trial++ {
		shuffled := append([]float64(nil), vals...)
		rng.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		got := math.Float64bits(deterministicSum(shuffled))
		if got != want {
			t.Fatalf("trial %d: deterministicSum depends on order: bits %x != %x (vals=%v)",
				trial, got, want, shuffled)
		}
	}
	t.Logf("deterministicSum stable across 200 shuffles: bits=%x value=%v",
		want, deterministicSum(vals))

	// 精确性：[-1e16, 1, 1, 1e16] 的数学和恰为 2，补偿求和应达到。
	if got := deterministicSum([]float64{1e16, 1, 1, -1e16}); got != 2 {
		t.Fatalf("compensated sum should be exact here: got %v want 2", got)
	}
}

// detCanonical 把结果集合转成与运行细节无关的确定签名：
// 不含 ClosedAt（墙钟）与 Watermark（取决于到达交错），
// 含全部聚合值（Sum/Min/Max 按比特）与 EmitSeq（关闭顺序确定）。
func detCanonical(rs []*WindowResult) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, fmt.Sprintf("%s@%d|n=%d|sum=%x|min=%x|max=%x|first=%s|seq=%d",
			r.Key, r.WindowStart.UnixNano(), r.Count,
			math.Float64bits(r.Sum), math.Float64bits(r.Min), math.Float64bits(r.Max),
			r.FirstEventID, r.EmitSeq))
	}
	sort.Strings(out)
	return out
}

// detEventSet 生成一批带小数值的逻辑事件：8 键 × 6 窗口 × 3 事件。
// 小数值刻意取 0.1/0.2/0.3 这类二进制不可精确表示的数。
func detEventSet() []string {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	decimals := []string{"0.1", "0.2", "0.3", "0.7", "1.1", "2.9"}
	var lines []string
	for w := 0; w < 6; w++ {
		for k := 0; k < 8; k++ {
			for j := 0; j < 3; j++ {
				ts := base.Add(time.Duration(w)*time.Minute +
					time.Duration(j)*time.Second + time.Duration(k)*100*time.Millisecond)
				id := fmt.Sprintf("w%dk%dj%d", w, k, j)
				v := decimals[(w+k+j)%len(decimals)]
				lines = append(lines, fmt.Sprintf("%s,key%d,%s,%s",
					id, k, ts.Format(time.RFC3339Nano), v))
			}
		}
	}
	return lines
}

func runDetConfig(t *testing.T, cfg Config, sources []Source) []string {
	t.Helper()
	sink := NewMemorySink()
	eng := NewEngine(cfg, func(ce *ClassifiedError) {
		// 迟到视界（30m）覆盖全部乱序跨度；注入的重复投递会被幂等
		// 吸收（DUPLICATE 属预期），其余任何分类异常都说明语义被破坏。
		if ce.Kind != KindDuplicate {
			t.Errorf("unexpected classified error: %s", ce)
		}
	})
	if err := eng.Run(context.Background(), sources, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.Duplicates) > 0 {
		t.Fatalf("duplicate emissions: %v", sink.Duplicates)
	}
	return detCanonical(sink.Snapshot())
}

// TestEngineDeterministicAcrossInterleavings：同一批逻辑输入，
// 无论单源顺序、多源任意交错、是否带重复投递、是否启用检查点，
// 每个窗口的聚合结果（含小数值 Sum 的比特级取值）都必须完全一致。
func TestEngineDeterministicAcrossInterleavings(t *testing.T) {
	events := detEventSet()
	t.Logf("logical input: %d events across 8 keys x 6 windows", len(events))

	baseCfg := func() Config {
		cfg := DefaultConfig()
		cfg.WindowSize = time.Minute
		// 迟到视界大于全部事件跨度：任何交错下都不会误判迟到，
		// 从而“同一逻辑输入”在各运行中被完整接受。
		cfg.AllowedLateness = 30 * time.Minute
		cfg.SourceQueueSize = 4 // 小队列制造真实反压与交错
		return cfg
	}

	// 配置 1：单源、按生成顺序。
	ref := runDetConfig(t, baseCfg(), []Source{NewSliceSource("solo", events)})
	t.Logf("config 1 (single source, in order): %d window results", len(ref))

	// 配置 2/3：两种不同 shuffle 拆到 3 个源，并注入重复投递。
	for run := 0; run < 2; run++ {
		rng := rand.New(rand.NewSource(int64(1000 + run)))
		shuffled := append([]string(nil), events...)
		rng.Shuffle(len(shuffled), func(i, j int) {
			shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
		})
		// 重复投递：随机复制 1/4 的记录（同 id 同内容），幂等去重应吸收。
		withDups := append([]string(nil), shuffled...)
		for i := 0; i < len(shuffled)/4; i++ {
			withDups = append(withDups, shuffled[rng.Intn(len(shuffled))])
		}
		rng.Shuffle(len(withDups), func(i, j int) {
			withDups[i], withDups[j] = withDups[j], withDups[i]
		})
		var srcs []Source
		for s := 0; s < 3; s++ {
			var part []string
			for i, line := range withDups {
				if i%3 == s {
					part = append(part, line)
				}
			}
			srcs = append(srcs, NewSliceSource(fmt.Sprintf("s%d", s), part))
		}
		got := runDetConfig(t, baseCfg(), srcs)
		if len(got) != len(ref) {
			t.Fatalf("run %d: result count %d != %d", run, len(got), len(ref))
		}
		for i := range got {
			if got[i] != ref[i] {
				t.Fatalf("run %d mismatch at %d:\n got %s\nwant %s", run, i, got[i], ref[i])
			}
		}
		t.Logf("config %d (3 sources, shuffled, with duplicates): identical to reference", run+2)
	}

	// 配置 4：启用检查点（值随快照落盘/恢复后结果仍一致）。
	dir := t.TempDir()
	cfgCk := baseCfg()
	cfgCk.CheckpointPath = dir + "/ck.json"
	cfgCk.CheckpointEvery = 7
	rng := rand.New(rand.NewSource(7777))
	shuffled := append([]string(nil), events...)
	rng.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	got := runDetConfig(t, cfgCk, []Source{
		NewSliceSource("c0", shuffled[:len(shuffled)/2]),
		NewSliceSource("c1", shuffled[len(shuffled)/2:]),
	})
	for i := range got {
		if got[i] != ref[i] {
			t.Fatalf("checkpointed run mismatch at %d:\n got %s\nwant %s", i, got[i], ref[i])
		}
	}
	t.Logf("config 4 (checkpoint every 7 events): identical to reference")
	t.Logf("PASS: %d window results byte-identical across all arrival orders", len(ref))
}
