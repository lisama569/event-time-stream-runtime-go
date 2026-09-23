package stream

import "time"

// WatermarkTracker 为每个源独立维护事件时间水位，并给出全局水位。
//
// 水位定义：某源水位 = 该源“已观测到”的最大事件时间。
// 全局水位 = 所有未结束源水位的最小值（多源并行时取最保守者，
// 保证慢源不会拖垮正确性：快源可以先关窗的前提是慢源也已观测到该时间）。
// 已正常结束（EOF）的源退出最小值计算。
type WatermarkTracker struct {
	perSource map[string]time.Time
	finished  map[string]bool
}

func NewWatermarkTracker() *WatermarkTracker {
	return &WatermarkTracker{
		perSource: make(map[string]time.Time),
		finished:  make(map[string]bool),
	}
}

// Register 在运行前登记所有已知源，初始水位为零值（视为 -inf）。
func (w *WatermarkTracker) Register(name string) {
	if _, ok := w.perSource[name]; !ok {
		w.perSource[name] = time.Time{}
	}
}

// Observe 观测一个事件时间，单调推进该源水位。
// 返回推进后的全局水位与本次是否推进了该源水位。
func (w *WatermarkTracker) Observe(source string, t time.Time) (global time.Time, advanced bool) {
	cur := w.perSource[source]
	if t.After(cur) {
		w.perSource[source] = t.UTC()
		advanced = true
	}
	return w.Global(), advanced
}

// Finish 标记某源已正常结束，退出全局最小值计算。
func (w *WatermarkTracker) Finish(source string) {
	w.finished[source] = true
}

// Restore 在恢复时载入某源水位下界；随后观测只会取 max。
func (w *WatermarkTracker) Restore(source string, t time.Time) {
	w.Register(source)
	if t.After(w.perSource[source]) {
		w.perSource[source] = t.UTC()
	}
}

// Global 返回当前全局水位。任何活跃源尚无观测时返回零值（引擎按 -inf 处理）。
func (w *WatermarkTracker) Global() time.Time {
	var min time.Time
	first := true
	for src, wm := range w.perSource {
		if w.finished[src] {
			continue
		}
		if first || wm.Before(min) {
			min = wm
			first = false
		}
	}
	return min
}

// PerSource 返回各源水位快照（副本）。
func (w *WatermarkTracker) PerSource() map[string]time.Time {
	out := make(map[string]time.Time, len(w.perSource))
	for k, v := range w.perSource {
		out[k] = v
	}
	return out
}
