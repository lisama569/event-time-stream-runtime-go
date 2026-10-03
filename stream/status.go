package stream

import (
	"sync/atomic"
	"time"
)

// SourceStatus 是单个输入源的运行中状态摘要。
type SourceStatus struct {
	Name      string    `json:"name"`
	Offset    int64     `json:"offset"`   // 已消费记录条数
	Queued    int       `json:"queued"`   // 摄取队列中积压的记录数
	Capacity  int       `json:"capacity"` // 队列容量（背压上界）
	Watermark time.Time `json:"watermark"`
	Finished  bool      `json:"finished"` // 是否已 EOF
}

// Status 是引擎运行中状态的一次性摘要（只读快照，可安全并发读取）。
// 用于体检：判断状态是否有界（窗口数是否只跟随活跃范围）、
// 定位积压（各源队列水位）与异常（分类计数）。
type Status struct {
	Running         bool           `json:"running"`
	GlobalWatermark time.Time      `json:"global_watermark"`
	ActiveWindows   int64          `json:"active_windows"`   // 打开中的窗口数
	RetainedEmitted int64          `json:"retained_emitted"` // 仍在保留范围内的已发布窗口数
	WindowsEmitted  int64          `json:"windows_emitted"`  // 累计已发布窗口数（单调）
	Ingested        int64          `json:"ingested"`
	Accepted        int64          `json:"accepted"`
	Malformed       int64          `json:"malformed"`
	InvalidTS       int64          `json:"invalid_ts"`
	OutOfRange      int64          `json:"out_of_range"`
	LateDropped     int64          `json:"late_dropped"`
	Duplicates      int64          `json:"duplicates"`
	Checkpoints     int64          `json:"checkpoints"`
	Sources         []SourceStatus `json:"sources"`
}

// sourceLive 是单个源的运行中原子状态（状态协程写，Status 读者并发读）。
type sourceLive struct {
	name     string
	ch       chan ingestionItem // 读 len 即积压量，并发安全
	offset   atomic.Int64
	wmNano   atomic.Int64 // 事件时间水位（UnixNano）；0 表示尚无观测
	finished atomic.Bool
}

// engineLive 是一次运行的可观测原子状态集合。
// 全部字段为原子量或只读引用：状态协程无锁更新，Status 无锁读取，
// 因此运行中体检不需要停止摄入，也基本不影响吞吐。
type engineLive struct {
	running         atomic.Bool
	globalWMNano    atomic.Int64
	activeWindows   atomic.Int64
	retainedEmitted atomic.Int64
	windowsEmitted  atomic.Int64
	ingested        atomic.Int64
	accepted        atomic.Int64
	malformed       atomic.Int64
	invalidTS       atomic.Int64
	outOfRange      atomic.Int64
	lateDropped     atomic.Int64
	duplicates      atomic.Int64
	checkpoints     atomic.Int64
	sources         []sourceLive // 运行期间只读
	srcIdx          map[string]int
}

func nanoToTime(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n).UTC()
}

func timeToNano(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UnixNano()
}

// Status 返回当前运行状态摘要。可在运行中随时调用：只读原子量与队列
// 长度，不需要停止摄入、不改动配置、基本不影响吞吐。运行结束后调用
// 返回最后一次运行的最终值（Running=false）；从未运行时返回零值。
func (e *Engine) Status() Status {
	lv := e.live.Load()
	if lv == nil {
		return Status{}
	}
	st := Status{
		Running:         lv.running.Load(),
		GlobalWatermark: nanoToTime(lv.globalWMNano.Load()),
		ActiveWindows:   lv.activeWindows.Load(),
		RetainedEmitted: lv.retainedEmitted.Load(),
		WindowsEmitted:  lv.windowsEmitted.Load(),
		Ingested:        lv.ingested.Load(),
		Accepted:        lv.accepted.Load(),
		Malformed:       lv.malformed.Load(),
		InvalidTS:       lv.invalidTS.Load(),
		OutOfRange:      lv.outOfRange.Load(),
		LateDropped:     lv.lateDropped.Load(),
		Duplicates:      lv.duplicates.Load(),
		Checkpoints:     lv.checkpoints.Load(),
		Sources:         make([]SourceStatus, 0, len(lv.sources)),
	}
	for i := range lv.sources {
		s := &lv.sources[i]
		st.Sources = append(st.Sources, SourceStatus{
			Name:      s.name,
			Offset:    s.offset.Load(),
			Queued:    len(s.ch),
			Capacity:  cap(s.ch),
			Watermark: nanoToTime(s.wmNano.Load()),
			Finished:  s.finished.Load(),
		})
	}
	return st
}
