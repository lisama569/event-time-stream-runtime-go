package stream

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
)

// Sink 消费最终窗口结果。
// 引擎保证：结果只在其所属状态进入检查点之后发布（崩溃恢复后向新汇重放），
// 且每个窗口恰好一次。
type Sink interface {
	Emit(r *WindowResult) error
	Close() error
}

// JSONSink 把结果以 JSON Lines 写入 io.Writer，互斥保证并发安全。
type JSONSink struct {
	mu sync.Mutex
	w  io.Writer
}

func NewJSONSink(w io.Writer) *JSONSink { return &JSONSink{w: w} }

func (s *JSONSink) Emit(r *WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := s.w.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func (s *JSONSink) Close() error {
	if c, ok := s.w.(io.Closer); ok {
		return c.Close()
	}
	return nil
}

// MemorySink 用于测试：收集结果、检测重复发射与发射顺序。
type MemorySink struct {
	mu         sync.Mutex
	Results    []*WindowResult
	seen       map[string]int
	Duplicates []string
}

func NewMemorySink() *MemorySink {
	return &MemorySink{seen: make(map[string]int)}
}

func resultKey(r *WindowResult) string {
	return fmt.Sprintf("%s@%d", r.Key, r.WindowStart.UnixNano())
}

func (s *MemorySink) Emit(r *WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := resultKey(r)
	if n, ok := s.seen[k]; ok {
		s.Duplicates = append(s.Duplicates, fmt.Sprintf("%s emitted %d times", k, n+1))
		return nil
	}
	s.seen[k] = 1
	cp := *r
	s.Results = append(s.Results, &cp)
	return nil
}

// Snapshot 返回结果副本（按发射顺序）。
func (s *MemorySink) Snapshot() []*WindowResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*WindowResult, len(s.Results))
	for i, r := range s.Results {
		cp := *r
		out[i] = &cp
	}
	return out
}

func (s *MemorySink) Close() error { return nil }

var _ Sink = (*JSONSink)(nil)
var _ Sink = (*MemorySink)(nil)

// CommittedFileSink 是与检查点协议配套的本地文件汇：
//   - 启动时以 O_TRUNC 打开（旧文件由检查点中的已提交结果完整重建）；
//   - 引擎仅在检查点提交成功后才把新结果追加进来并 fsync；
//   - 崩溃恢复时引擎重放检查点中的全部已发布结果，因此最终文件与
//     “已提交状态”严格一致：不重、不漏、无未提交结果。
type CommittedFileSink struct {
	mu sync.Mutex
	f  *os.File
}

// NewCommittedFileSink 截断打开 path。调用方一般在 Run 开始时构造一次。
func NewCommittedFileSink(path string) (*CommittedFileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	return &CommittedFileSink{f: f}, nil
}

func (s *CommittedFileSink) Emit(r *WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := s.f.Write(append(data, '\n')); err != nil {
		return err
	}
	return s.f.Sync()
}

func (s *CommittedFileSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

var _ Sink = (*CommittedFileSink)(nil)
