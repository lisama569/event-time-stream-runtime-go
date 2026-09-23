package stream

import (
	"fmt"
	"time"
)

// ErrorKind 对异常事件进行明确分类。
type ErrorKind string

const (
	// KindMalformed 格式错误：无法按约定格式解析。
	KindMalformed ErrorKind = "MALFORMED"
	// KindInvalidTimestamp 时间戳非法：缺失、无法解析或为零值。
	KindInvalidTimestamp ErrorKind = "INVALID_TIMESTAMP"
	// KindOutOfRange 超出允许范围：事件时间超出当前可接受区间。
	KindOutOfRange ErrorKind = "OUT_OF_RANGE"
	// KindLateDropped 水位已关闭窗口后的迟到事件，不得修改已关闭结果。
	KindLateDropped ErrorKind = "LATE_DROPPED"
	// KindDuplicate 重复投递：同一事件 ID 已处理过。
	KindDuplicate ErrorKind = "DUPLICATE"
)

// ClassifiedError 是被明确分类、可观测处理的异常记录。
type ClassifiedError struct {
	Kind      ErrorKind
	Source    string
	Offset    int64
	Raw       string
	EventTime time.Time
	Watermark time.Time
	Reason    string
}

func (e *ClassifiedError) Error() string {
	return fmt.Sprintf("%s source=%s offset=%d eventTime=%s watermark=%s: %s",
		e.Kind, e.Source, e.Offset, formatTime(e.EventTime), formatTime(e.Watermark), e.Reason)
}

// ErrorHandler 接收所有被分类的异常事件。不得为 nil。
type ErrorHandler func(*ClassifiedError)

// CheckpointError 表示检查点被拒绝，Kind 给出可区分的原因。
type CheckpointError struct {
	Kind   string
	Path   string
	Reason string
}

func (e *CheckpointError) Error() string {
	return fmt.Sprintf("checkpoint rejected (%s): %s: %s", e.Kind, e.Path, e.Reason)
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return "-"
	}
	return t.UTC().Format(time.RFC3339Nano)
}
