package stream

import (
	"math"
	"sort"
	"sync/atomic"
	"time"
)

// StatusSnapshot 是引擎运行状态在某一时刻的一致性摘要，
// 可在摄入进行中从任意 goroutine 并发读取（不改配置、不中断摄入、
// 只读原子量与队列长度，对吞吐的影响可忽略）。
// 用它判断状态是否有界、定位异常与积压。
//
// 读取是无锁的：各计数器并非事务式同时落笔，单条在途记录可能造成
// 计数间不超过“每源 1 条”的瞬时偏斜；趋势与量级不受影响。
type StatusSnapshot struct {
	// 窗口状态体积（有界性的直接证据）。
	ActiveWindows   int64 // 当前打开中的窗口数
	WindowsClosed   int64 // 累计已最终关闭（终结）的窗口数
	EmittedRetained int64 // 当前仍保留在状态中的已发布结果数（受保留策略约束）

	// 事件处理计数（与 Report 同源）。
	Ingested    int64
	Accepted    int64
	Malformed   int64
	InvalidTS   int64
	OutOfRange  int64
	LateDropped int64
	Duplicates  int64
	Checkpoints int64

	// GlobalWatermark 为当前全局水位；零值表示尚无观测。
	GlobalWatermark time.Time
	// Sources 为各输入源进度与积压，按源名排序（输出确定）。
	Sources []SourceStatus
}

// SourceStatus 是单个输入源的运行状态。
type SourceStatus struct {
	Name      string    // 源名
	Offset    int64     // 已消费记录条数（含坏记录/空行）
	Backlog   int       // 当前在该源有界队列中等待处理的记录数
	QueueCap  int       // 队列容量（Backlog 持续贴近 QueueCap 说明该源被反压）
	Finished  bool      // 是否已 EOF
	Watermark time.Time // 该源当前水位；零值表示尚无观测
}

// noWMSentinel 是 liveStatus 中“尚无水位观测”的原子编码。
const noWMSentinel = math.MinInt64

// liveStatus 是运行期状态的原子镜像：仅状态 goroutine 写入，
// 任意 goroutine 可通过 Status 读取，无需锁、不阻塞摄入。
type liveStatus struct {
	activeWindows   atomic.Int64
	windowsClosed   atomic.Int64
	emittedRetained atomic.Int64

	ingested    atomic.Int64
	accepted    atomic.Int64
	malformed   atomic.Int64
	invalidTS   atomic.Int64
	outOfRange  atomic.Int64
	lateDropped atomic.Int64
	duplicates  atomic.Int64
	checkpoints atomic.Int64

	globalWM atomic.Int64 // unixnano；noWMSentinel 表示无观测

	order   []string
	sources map[string]*sourceLive
}

// sourceLive 是单源运行状态的原子镜像。
type sourceLive struct {
	queue    chan ingestionItem // 只读引用，用于 len/cap 观测积压
	queueCap int
	offset   atomic.Int64
	finished atomic.Bool
	wm       atomic.Int64 // unixnano；noWMSentinel 表示无观测
}

func newLiveStatus(order []string, queues map[string]chan ingestionItem,
	offsets map[string]int64) *liveStatus {
	l := &liveStatus{
		order:   append([]string(nil), order...),
		sources: make(map[string]*sourceLive, len(order)),
	}
	l.globalWM.Store(noWMSentinel)
	for _, name := range l.order {
		sl := &sourceLive{queue: queues[name], queueCap: cap(queues[name])}
		sl.offset.Store(offsets[name])
		sl.wm.Store(noWMSentinel)
		l.sources[name] = sl
	}
	return l
}

func encodeWM(t time.Time) int64 {
	if t.IsZero() {
		return noWMSentinel
	}
	return t.UTC().UnixNano()
}

func decodeWM(v int64) time.Time {
	if v == noWMSentinel {
		return time.Time{}
	}
	return time.Unix(0, v).UTC()
}

// Status 返回当前运行状态摘要。运行前（返回零值快照）/运行中/运行后
// 均可调用；运行中调用不阻塞摄入、不改任何配置。
func (e *Engine) Status() StatusSnapshot {
	live := e.live.Load()
	if live == nil {
		return StatusSnapshot{}
	}
	snap := StatusSnapshot{
		ActiveWindows:   live.activeWindows.Load(),
		WindowsClosed:   live.windowsClosed.Load(),
		EmittedRetained: live.emittedRetained.Load(),
		Ingested:        live.ingested.Load(),
		Accepted:        live.accepted.Load(),
		Malformed:       live.malformed.Load(),
		InvalidTS:       live.invalidTS.Load(),
		OutOfRange:      live.outOfRange.Load(),
		LateDropped:     live.lateDropped.Load(),
		Duplicates:      live.duplicates.Load(),
		Checkpoints:     live.checkpoints.Load(),
		GlobalWatermark: decodeWM(live.globalWM.Load()),
	}
	names := append([]string(nil), live.order...)
	sort.Strings(names) // 输出顺序确定，便于对比与日志
	for _, name := range names {
		sl := live.sources[name]
		backlog := 0
		if sl.queue != nil {
			backlog = len(sl.queue)
		}
		snap.Sources = append(snap.Sources, SourceStatus{
			Name:      name,
			Offset:    sl.offset.Load(),
			Backlog:   backlog,
			QueueCap:  sl.queueCap,
			Finished:  sl.finished.Load(),
			Watermark: decodeWM(sl.wm.Load()),
		})
	}
	return snap
}
