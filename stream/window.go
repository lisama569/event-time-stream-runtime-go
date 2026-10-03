package stream

import (
	"sort"
	"time"
)

// openWindow 是一个尚未关闭的键控窗口的有界状态。
type openWindow struct {
	key            string
	start, end     time.Time
	count          int64
	sum            exactSum // 精确和：顺序无关，任意到达顺序结果逐位一致
	min, max       float64
	firstEventID   string
	firstEventTime time.Time
	seenIDs        map[string]struct{}
}

// emittedEntry 记录已发布窗口，用于恢复后“不重”与向新汇侧重放。
type emittedEntry struct {
	R WindowResult
}

// WindowStore 保存有界窗口状态：
//   - open：打开中的窗口（数量受水位推进与允许的未来范围上界约束）；
//   - emitted：已发布窗口（仅保留 关闭视界+保留范围 以内的尾部；
//     全局水位越过 windowEnd+lateness+retention 后，该窗口的任何后续事件
//     都必然被水位视界判为迟到，因此保留记录可以安全遗忘——状态体积只与
//     活跃窗口、迟到视界和配置的保留范围相关，不随累计窗口数膨胀）。
type WindowStore struct {
	windowSize time.Duration
	lateness   time.Duration
	retention  time.Duration // 已发布结果在关闭视界之后的保留范围
	open       map[WindowID]*openWindow
	emitted    map[WindowID]emittedEntry
	emittedOrd []WindowID // 按发射顺序
	nextSeq    int64      // 下一个发射序号（持久化，遗忘已发布记录后仍单调）
}

func NewWindowStore(windowSize, allowedLateness time.Duration) *WindowStore {
	return &WindowStore{
		windowSize: windowSize,
		lateness:   allowedLateness,
		open:       make(map[WindowID]*openWindow),
		emitted:    make(map[WindowID]emittedEntry),
		nextSeq:    1,
	}
}

// SetEmittedRetention 设置已发布结果在关闭视界之后的保留范围。
func (s *WindowStore) SetEmittedRetention(d time.Duration) {
	if d < 0 {
		d = 0
	}
	s.retention = d
}

// closedHorizon 返回窗口的关闭视界：全局水位到达 end+lateness 时窗口最终关闭。
func windowClosedHorizon(end time.Time, lateness time.Duration) time.Time {
	return end.Add(lateness)
}

// Add 把事件加入对应窗口。
//   - accepted：被窗口接受（含迟到范围内的乱序事件）；
//   - dropped：窗口已被水位关闭（或按视界必然已关闭），事件迟到，不得修改结果；
//   - dup：同一事件 ID 在打开窗口内重复投递，被幂等忽略。
func (s *WindowStore) Add(e *Event, watermark time.Time) (accepted, dropped, dup bool) {
	start := WindowStart(e.Timestamp, s.windowSize)
	end := start.Add(s.windowSize)
	id := WindowID{Key: e.Key, Start: start}

	if _, emitted := s.emitted[id]; emitted {
		return false, true, false
	}
	if !watermark.IsZero() && !watermark.Before(windowClosedHorizon(end, s.lateness)) {
		// 水位已越过关闭视界：窗口不可能再被修改。
		return false, true, false
	}

	w := s.open[id]
	if w == nil {
		w = &openWindow{
			key: e.Key, start: start, end: end,
			sum: newExactSum(),
			min: e.Value, max: e.Value,
			firstEventID: e.ID, firstEventTime: e.Timestamp,
			seenIDs: make(map[string]struct{}),
		}
		s.open[id] = w
	} else {
		if _, ok := w.seenIDs[e.ID]; ok {
			return false, false, true
		}
	}

	w.seenIDs[e.ID] = struct{}{}
	w.count++
	w.sum.Add(e.Value)
	if e.Value < w.min {
		w.min = e.Value
	}
	if e.Value > w.max {
		w.max = e.Value
	}
	// 确定性的“首事件”：取 (事件时间, 事件ID) 最小者，与处理顺序无关，
	// 从而乱序、重放、并发下结果唯一确定。
	if e.Timestamp.Before(w.firstEventTime) ||
		(e.Timestamp.Equal(w.firstEventTime) && e.ID < w.firstEventID) {
		w.firstEventTime = e.Timestamp
		w.firstEventID = e.ID
	}
	return true, false, false
}

// Closeable 返回在给定全局水位下可以最终关闭并发布的窗口，
// 按 (窗口起点, 键) 排序，保证发射顺序跨运行确定。
func (s *WindowStore) Closeable(globalWM time.Time) []WindowID {
	if globalWM.IsZero() {
		return nil
	}
	var out []WindowID
	for id, w := range s.open {
		if !globalWM.Before(windowClosedHorizon(w.end, s.lateness)) {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Start.Equal(out[j].Start) {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// Close 取出并移除某窗口，生成确定聚合结果并登记为已发布。
// 对同一窗口只成功一次（重复调用返回 false）。
func (s *WindowStore) Close(id WindowID, wm, closedAt time.Time, seq int64) (*WindowResult, bool) {
	w := s.open[id]
	if w == nil {
		if _, ok := s.emitted[id]; ok {
			return nil, false
		}
		return nil, false
	}
	delete(s.open, id)
	r := &WindowResult{
		Key:          w.key,
		WindowStart:  w.start,
		WindowEnd:    w.end,
		Count:        w.count,
		Sum:          w.sum.Float(),
		Min:          w.min,
		Max:          w.max,
		FirstEventID: w.firstEventID,
		ClosedAt:     closedAt.UTC(),
		Watermark:    wm.UTC(),
		EmitSeq:      seq,
	}
	s.emitted[id] = emittedEntry{R: *r}
	s.emittedOrd = append(s.emittedOrd, id)
	if seq >= s.nextSeq {
		s.nextSeq = seq + 1 // 发射序号随关闭单调推进，并随快照持久化
	}
	return r, true
}

// Emitted 返回某已发布窗口的发射序号；未发布返回 false。
func (s *WindowStore) Emitted(id WindowID) (int64, bool) {
	if en, ok := s.emitted[id]; ok {
		return en.R.EmitSeq, true
	}
	return 0, false
}

// EmittedResults 按发射顺序返回所有已保留的已发布结果（恢复重放用）。
func (s *WindowStore) EmittedResults() []WindowResult {
	out := make([]WindowResult, 0, len(s.emittedOrd))
	for _, id := range s.emittedOrd {
		out = append(out, s.emitted[id].R)
	}
	return out
}

// OpenCount 返回当前打开中的窗口数（运行状态摘要用）。
func (s *WindowStore) OpenCount() int { return len(s.open) }

// EmittedCount 返回仍在保留范围内的已发布窗口数（运行状态摘要用）。
func (s *WindowStore) EmittedCount() int { return len(s.emitted) }

// NextSeq 给出恢复后下一个发射序号。序号随快照持久化，
// 即使旧的已发布记录已按保留范围被遗忘，序号也不会回退重用。
func (s *WindowStore) NextSeq() int64 {
	return s.nextSeq
}

// forgetHorizon 返回已发布记录的遗忘视界：全局水位越过
// windowEnd+lateness+retention 的已发布窗口可以被安全遗忘。
func (s *WindowStore) forgetHorizon(end time.Time) time.Time {
	return end.Add(s.lateness + s.retention)
}

// PruneEmitted 遗忘遗忘视界已越过 before 的已发布记录，返回遗忘条数。
// before 为零值时不做任何事。迟到判定不依赖这些记录（水位视界已保证
// 任何该窗口事件必然被判迟到），因此遗忘不改变迟到语义。
func (s *WindowStore) PruneEmitted(before time.Time) int {
	if before.IsZero() {
		return 0
	}
	pruned := 0
	kept := s.emittedOrd[:0]
	for _, id := range s.emittedOrd {
		en := s.emitted[id]
		if !before.Before(s.forgetHorizon(en.R.WindowEnd)) {
			delete(s.emitted, id)
			pruned++
			continue
		}
		kept = append(kept, id)
	}
	s.emittedOrd = kept
	return pruned
}

// windowSnapshot 是窗口状态的可序列化快照。
type windowSnapshot struct {
	Open    []windowState
	Emitted []emittedRecord
	NextSeq int64
}

// windowState 是 openWindow 的序列化载体。
type windowState struct {
	Key            string    `json:"key"`
	Start          time.Time `json:"start"`
	End            time.Time `json:"end"`
	Count          int64     `json:"count"`
	Sum            float64   `json:"sum"`
	SumExact       string    `json:"sum_exact,omitempty"` // 精确和（有理数文本），恢复后累加仍顺序无关
	Min            float64   `json:"min"`
	Max            float64   `json:"max"`
	FirstEventID   string    `json:"first_event_id"`
	FirstEventTime time.Time `json:"first_event_time"`
	SeenIDs        []string  `json:"seen_ids"`
}

// emittedRecord 是已发布结果的序列化载体。
type emittedRecord struct {
	R WindowResult `json:"result"`
}

// Snapshot 导出状态；pruneBefore 给定水位时，遗忘遗忘视界
// （windowEnd+lateness+retention）已越过该水位的已发布窗口。
func (s *WindowStore) Snapshot(pruneBefore time.Time) windowSnapshot {
	snap := windowSnapshot{
		Open:    make([]windowState, 0, len(s.open)),
		Emitted: make([]emittedRecord, 0, len(s.emittedOrd)),
		NextSeq: s.nextSeq,
	}
	for _, w := range s.open {
		ids := make([]string, 0, len(w.seenIDs))
		for id := range w.seenIDs {
			ids = append(ids, id)
		}
		sort.Strings(ids) // 序列化确定性
		snap.Open = append(snap.Open, windowState{
			Key: w.key, Start: w.start, End: w.end,
			Count: w.count, Sum: w.sum.Float(), SumExact: w.sum.String(),
			Min: w.min, Max: w.max,
			FirstEventID: w.firstEventID, FirstEventTime: w.firstEventTime,
			SeenIDs: ids,
		})
	}
	s.PruneEmitted(pruneBefore) // 顺带收紧内存中的已发布尾部
	for _, id := range s.emittedOrd {
		snap.Emitted = append(snap.Emitted, emittedRecord{R: s.emitted[id].R})
	}
	sort.Slice(snap.Open, func(i, j int) bool {
		if !snap.Open[i].Start.Equal(snap.Open[j].Start) {
			return snap.Open[i].Start.Before(snap.Open[j].Start)
		}
		return snap.Open[i].Key < snap.Open[j].Key
	})
	return snap
}

// Restore 从快照重建全部状态。
func (s *WindowStore) Restore(snap windowSnapshot) {
	s.open = make(map[WindowID]*openWindow, len(snap.Open))
	for _, st := range snap.Open {
		seen := make(map[string]struct{}, len(st.SeenIDs))
		for _, id := range st.SeenIDs {
			seen[id] = struct{}{}
		}
		sum := newExactSum()
		if st.SumExact != "" {
			if !sum.setString(st.SumExact) {
				// 快照由本包写出且经校验和保护，非法即编程错误。
				panic("stream: invalid exact sum in snapshot: " + st.SumExact)
			}
		} else {
			sum.Add(st.Sum) // 兼容无精确和字段的旧快照
		}
		s.open[WindowID{Key: st.Key, Start: st.Start}] = &openWindow{
			key: st.Key, start: st.Start, end: st.End,
			count: st.Count, sum: sum, min: st.Min, max: st.Max,
			firstEventID: st.FirstEventID, firstEventTime: st.FirstEventTime,
			seenIDs: seen,
		}
	}
	s.emitted = make(map[WindowID]emittedEntry, len(snap.Emitted))
	s.emittedOrd = s.emittedOrd[:0]
	var maxSeq int64
	for _, er := range snap.Emitted {
		id := WindowID{Key: er.R.Key, Start: er.R.WindowStart}
		s.emitted[id] = emittedEntry{R: er.R}
		s.emittedOrd = append(s.emittedOrd, id)
		if er.R.EmitSeq > maxSeq {
			maxSeq = er.R.EmitSeq
		}
	}
	// 恢复发射序号：优先取快照中的持久化值；旧快照没有该字段时
	// 退化为已保留记录中的最大值+1（序号绝不回退重用）。
	s.nextSeq = snap.NextSeq
	if s.nextSeq < maxSeq+1 {
		s.nextSeq = maxSeq + 1
	}
	if s.nextSeq <= 0 {
		s.nextSeq = 1
	}
}

// WindowStart 按窗口大小对事件时间向下取整（UTC）。
func WindowStart(t time.Time, size time.Duration) time.Time {
	return t.UTC().Truncate(size)
}
