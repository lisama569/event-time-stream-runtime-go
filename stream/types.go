package stream

import (
	"encoding/json"
	"time"
)

// Event 表示一条经过解析与校验、进入运行时的有效事件。
type Event struct {
	ID        string
	Key       string
	Timestamp time.Time
	Value     float64
	Source    string
	Offset    int64
}

// WindowResult 是某个键、某个窗口的最终聚合结果。
// 结果一经发布即不可变；同一窗口在任何情况下只发布一次。
type WindowResult struct {
	Key          string    `json:"key"`
	WindowStart  time.Time `json:"window_start"`
	WindowEnd    time.Time `json:"window_end"`
	Count        int64     `json:"count"`
	Sum          float64   `json:"sum"`
	Min          float64   `json:"min"`
	Max          float64   `json:"max"`
	FirstEventID string    `json:"first_event_id"`
	ClosedAt     time.Time `json:"closed_at"`
	Watermark    time.Time `json:"-"` // 序列化走 MarshalJSON，EOI 显示为文本
	EmitSeq      int64     `json:"emit_seq"`
}

// windowResultJSON 是 WindowResult 的序列化形状（watermark 用文本表达 EOI）。
type windowResultJSON struct {
	Key          string  `json:"key"`
	WindowStart  string  `json:"window_start"`
	WindowEnd    string  `json:"window_end"`
	Count        int64   `json:"count"`
	Sum          float64 `json:"sum"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	FirstEventID string  `json:"first_event_id"`
	ClosedAt     string  `json:"closed_at"`
	Watermark    string  `json:"watermark"`
	EmitSeq      int64   `json:"emit_seq"`
}

func (r WindowResult) MarshalJSON() ([]byte, error) {
	wm := r.Watermark.UTC().Format(time.RFC3339Nano)
	if r.Watermark.Equal(endOfInputWatermark) {
		wm = "EOI" // end-of-input：所有有界源结束后的 +inf 最终水位
	}
	return json.Marshal(windowResultJSON{
		Key:          r.Key,
		WindowStart:  r.WindowStart.UTC().Format(time.RFC3339Nano),
		WindowEnd:    r.WindowEnd.UTC().Format(time.RFC3339Nano),
		Count:        r.Count,
		Sum:          r.Sum,
		Min:          r.Min,
		Max:          r.Max,
		FirstEventID: r.FirstEventID,
		ClosedAt:     r.ClosedAt.UTC().Format(time.RFC3339Nano),
		Watermark:    wm,
		EmitSeq:      r.EmitSeq,
	})
}

// endOfInputWatermark 是最终排空的逻辑 +inf 哨兵（可被 time/JSON 表示）。
var endOfInputWatermark = time.Date(9000, 1, 1, 0, 0, 0, 0, time.UTC)

// WindowID 唯一标识一个键控窗口。
type WindowID struct {
	Key   string
	Start time.Time
}

// Config 是引擎可配置项。零值不可直接使用，请用 DefaultConfig。
type Config struct {
	WindowSize      time.Duration
	AllowedLateness time.Duration
	SourceQueueSize int
	MaxOutofOrder   time.Duration
	CheckpointPath  string
	CheckpointEvery int64
	// EmittedRetention 是已发布窗口结果在状态与检查点中的保留范围：
	// 当全局水位越过 windowEnd+AllowedLateness+EmittedRetention 后，
	// 该窗口的已发布记录被安全遗忘（此后任何该窗口事件必然迟到，
	// 由水位视界判定，不依赖保留记录）。这使长跑场景下内存与
	// 检查点体积只与活跃窗口、迟到视界与保留范围相关，而不随
	// 累计窗口数膨胀。<=0 表示不保留（关闭视界一过即遗忘）。
	EmittedRetention time.Duration
	// DeterministicClosedAt 为 true 时，结果的 ClosedAt 取窗口关闭视界
	// （windowEnd+AllowedLateness）而非墙钟时间，使同一批逻辑输入
	// 重复运行的输出可逐字节对比。
	DeterministicClosedAt bool
}

// DefaultConfig 返回一组可用的默认配置。
func DefaultConfig() Config {
	return Config{
		WindowSize:      time.Minute,
		AllowedLateness: 10 * time.Second,
		SourceQueueSize: 1024,
		// MaxOutofOrder 对“事件时间相对当前水位超前过多”的异常事件设界。
		// 0 表示不限制（默认关闭此项检查，乱序只受迟到语义约束）。
		MaxOutofOrder: 0,
		// EmittedRetention 默认保留 1 小时：足以覆盖恢复重放与运行中观测，
		// 同时保证长跑时状态有界。
		EmittedRetention: time.Hour,
		CheckpointEvery:  1000,
	}
}

// Stats 是一次运行的可观测计数。
type Stats struct {
	Ingested       int64
	Accepted       int64
	Malformed      int64
	InvalidTS      int64
	OutOfRange     int64
	LateDropped    int64
	Duplicates     int64
	WindowsEmitted int64
	Checkpoints    int64
}

func (r *WindowResult) UnmarshalJSON(data []byte) error {
	var v windowResultJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	parse := func(s string) (time.Time, error) {
		return time.Parse(time.RFC3339Nano, s)
	}
	ws, err := parse(v.WindowStart)
	if err != nil {
		return err
	}
	we, err := parse(v.WindowEnd)
	if err != nil {
		return err
	}
	ca, err := parse(v.ClosedAt)
	if err != nil {
		return err
	}
	wm := endOfInputWatermark
	if v.Watermark != "" && v.Watermark != "EOI" {
		wm, err = parse(v.Watermark)
		if err != nil {
			return err
		}
	}
	*r = WindowResult{
		Key: v.Key, WindowStart: ws, WindowEnd: we, Count: v.Count,
		Sum: v.Sum, Min: v.Min, Max: v.Max, FirstEventID: v.FirstEventID,
		ClosedAt: ca, Watermark: wm, EmitSeq: v.EmitSeq,
	}
	return nil
}
