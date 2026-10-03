package stream

import (
	"bufio"
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
//   - 启动时以追加模式打开，并扫描已有内容建立“已写入窗口”集合
//     （崩溃半截的最后一行会被截掉，保证文件始终是合法 JSON Lines）；
//   - 引擎仅在检查点提交成功后才把新结果追加进来并 fsync；
//   - 恢复重放时，已在文件中的窗口被幂等跳过，未写入的补齐。
//
// 因此输出文件始终等于“已提交结果的全集”：不重、不漏、无未提交结果——
// 即使检查点中的已发布记录已按保留范围被遗忘（长跑下有界状态），
// 文件中的历史结果也不会丢失。
type CommittedFileSink struct {
	mu   sync.Mutex
	f    *os.File
	seen map[string]struct{}
}

// NewCommittedFileSink 以追加模式打开 path 并扫描已有结果用于去重。
// 调用方一般在 Run 开始时构造一次。
func NewCommittedFileSink(path string) (*CommittedFileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &CommittedFileSink{f: f, seen: make(map[string]struct{})}
	if err := s.rescan(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// rescan 读取已有内容，登记已写入的窗口，并把文件截断到最后一条
// 完整记录之后（清除崩溃可能留下的半行），随后定位到末尾准备追加。
func (s *CommittedFileSink) rescan() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sc := bufio.NewScanner(s.f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var validEnd int64
	for sc.Scan() {
		line := sc.Bytes()
		var r WindowResult
		if err := json.Unmarshal(line, &r); err != nil {
			// 半截/损坏行：停止扫描并在此处截断。
			break
		}
		s.seen[resultKey(&r)] = struct{}{}
		validEnd += int64(len(line)) + 1 // + '\n'
	}
	if err := s.f.Truncate(validEnd); err != nil {
		return err
	}
	_, err := s.f.Seek(validEnd, io.SeekStart)
	return err
}

func (s *CommittedFileSink) Emit(r *WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := resultKey(r)
	if _, dup := s.seen[k]; dup {
		return nil // 恢复重放/重复发布：幂等跳过，文件内恰好一次
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := s.f.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.seen[k] = struct{}{}
	return nil
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
