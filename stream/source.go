package stream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

// Source 是一个本地输入源。Next 返回下一条原始记录及单调偏移量；
// 结束时返回 ok=false。偏移量从 0 开始、按物理记录单调递增，
// “已消费条数”即下一个偏移量，用于检查点恢复时精确跳过已处理记录。
type Source interface {
	Name() string
	Next(ctx context.Context) (raw string, offset int64, ok bool, err error)
}

// SliceSource 用于测试与内存回放的本地源。
type SliceSource struct {
	name  string
	lines []string
	pos   int64
	delay time.Duration
}

func NewSliceSource(name string, lines []string) *SliceSource {
	return &SliceSource{name: name, lines: lines}
}

// WithDelay 让源在每条记录前等待 d，用于模拟慢源/背压场景。
func (s *SliceSource) WithDelay(d time.Duration) *SliceSource {
	s.delay = d
	return s
}

// Seek 用于恢复：跳过前 n 条记录。
func (s *SliceSource) SeekOffset(n int64) {
	if n > int64(len(s.lines)) {
		n = int64(len(s.lines))
	}
	s.pos = n
}

func (s *SliceSource) Name() string { return s.name }

func (s *SliceSource) Next(ctx context.Context) (string, int64, bool, error) {
	for s.pos < int64(len(s.lines)) {
		if s.delay > 0 {
			select {
			case <-ctx.Done():
				return "", 0, false, ctx.Err()
			case <-time.After(s.delay):
			}
		}
		raw := s.lines[s.pos]
		off := s.pos
		s.pos++
		return raw, off, true, nil
	}
	return "", 0, false, nil
}

// FileSource 逐行扫描本地文件，偏移量即物理行号（从 0 起），恢复时按行跳过。
type FileSource struct {
	name    string
	path    string
	f       *os.File
	scanner *bufio.Scanner
	pos     int64
	started bool
}

func NewFileSource(path string) (*FileSource, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	sc := bufio.NewScanner(f)
	// 单行允许较大，默认 64K 太小，提到 4MiB。
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	return &FileSource{name: path, path: path, f: f, scanner: sc}, nil
}

// Seek 用于恢复：跳过前 n 行。必须在首次 Next 之前调用。
func (s *FileSource) SeekOffset(n int64) error {
	if s.started {
		return fmt.Errorf("source %s: seek after start not supported", s.name)
	}
	for i := int64(0); i < n; i++ {
		if !s.scanner.Scan() {
			if err := s.scanner.Err(); err != nil {
				return err
			}
			break
		}
	}
	s.pos = n
	return nil
}

func (s *FileSource) Name() string { return s.name }

func (s *FileSource) Next(ctx context.Context) (string, int64, bool, error) {
	s.started = true
	if s.scanner.Scan() {
		off := s.pos
		s.pos++
		return s.scanner.Text(), off, true, nil
	}
	if err := s.scanner.Err(); err != nil {
		return "", 0, false, err
	}
	return "", 0, false, nil
}

func (s *FileSource) Close() error {
	if s.f != nil {
		return s.f.Close()
	}
	return nil
}

// rawRecord 是 JSON Lines 格式的原始结构。
type rawRecord struct {
	ID  string          `json:"id"`
	Key string          `json:"key"`
	TS  string          `json:"ts"`
	V   json.RawMessage `json:"v"`
}

// ParseRecord 将一条原始记录解析并做字段级校验。
// 支持：
//
//	id,key,2026-01-01T00:00:00Z,1.5     CSV（4 列；# 开头为注释；空行跳过）
//	{"id":"e1","key":"a","ts":"...","v":1.5}  JSON Lines
//
// 返回 (nil, nil) 表示空行/注释，应直接跳过；
// 返回 (nil, *ClassifiedError) 表示被分类的异常事件（格式错误/时间戳非法）；
// 返回 (*Event, nil) 表示有效事件。
func ParseRecord(raw, source string, offset int64) (*Event, *ClassifiedError) {
	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return nil, nil
	}
	if strings.HasPrefix(line, "{") {
		return parseJSON(line, source, offset)
	}
	return parseCSV(line, source, offset)
}

func parseCSV(line string, source string, offset int64) (*Event, *ClassifiedError) {
	parts := strings.Split(line, ",")
	if len(parts) != 4 {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			fmt.Sprintf("expected 4 csv fields 'id,key,timestamp,value', got %d", len(parts)))
	}
	id := strings.TrimSpace(parts[0])
	key := strings.TrimSpace(parts[1])
	tsText := strings.TrimSpace(parts[2])
	vText := strings.TrimSpace(parts[3])
	if id == "" || key == "" {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"empty id or key field")
	}
	ts, err := parseTimestamp(tsText)
	if err != nil {
		return nil, classify(KindInvalidTimestamp, source, offset, line, time.Time{}, err.Error())
	}
	v, err := strconv.ParseFloat(vText, 64)
	if err != nil {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"value is not a number: "+err.Error())
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"value must be finite")
	}
	return &Event{ID: id, Key: key, Timestamp: ts, Value: v, Source: source, Offset: offset}, nil
}

func parseJSON(line string, source string, offset int64) (*Event, *ClassifiedError) {
	var r rawRecord
	if err := json.Unmarshal([]byte(line), &r); err != nil {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"invalid json: "+err.Error())
	}
	if strings.TrimSpace(r.ID) == "" || strings.TrimSpace(r.Key) == "" {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"missing id or key field")
	}
	if strings.TrimSpace(r.TS) == "" {
		return nil, classify(KindInvalidTimestamp, source, offset, line, time.Time{},
			"missing ts field")
	}
	ts, err := parseTimestamp(r.TS)
	if err != nil {
		return nil, classify(KindInvalidTimestamp, source, offset, line, time.Time{}, err.Error())
	}
	if len(r.V) == 0 {
		return nil, classify(KindMalformed, source, offset, line, time.Time{}, "missing v field")
	}
	var v float64
	if err := json.Unmarshal(r.V, &v); err != nil {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"v is not a number: "+err.Error())
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil, classify(KindMalformed, source, offset, line, time.Time{},
			"v must be finite")
	}
	return &Event{ID: r.ID, Key: r.Key, Timestamp: ts, Value: v, Source: source, Offset: offset}, nil
}

// parseTimestamp 接受 RFC3339（含纳秒、含数字时区）；无时区者按 UTC 解释。
func parseTimestamp(s string) (time.Time, error) {
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	var lastErr error
	for _, l := range layouts {
		if t, err := time.Parse(l, s); err == nil {
			if t.Location() == time.UTC || l == time.RFC3339Nano || l == time.RFC3339 {
				return t.UTC(), nil
			}
			// 无时区布局：按 UTC 解释。
			return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(),
				t.Second(), t.Nanosecond(), time.UTC), nil
		} else {
			lastErr = err
		}
	}
	return time.Time{}, fmt.Errorf("unparseable timestamp %q: %v", s, lastErr)
}

func classify(kind ErrorKind, source string, offset int64, raw string,
	eventTime time.Time, reason string) *ClassifiedError {
	return &ClassifiedError{
		Kind: kind, Source: source, Offset: offset, Raw: raw,
		EventTime: eventTime, Reason: reason,
	}
}
