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
//   - 启动时扫描既有文件（不截断）：完整行恢复“已落盘高水位 emit_seq”，
//     尾部残缺行（写入过程中崩溃留下的半行）直接截断，不污染后续输出；
//   - 引擎仅在检查点提交成功后才把新结果追加进来并 fsync，因此文件内容
//     始终是全局发射序列的一段前缀（外加可能的半行，启动时已裁掉）；
//   - 崩溃恢复时引擎重放检查点保留尾部的已提交结果：已落盘的
//     （emit_seq <= 高水位）幂等跳过，缺失的尾部按序补齐——无论保留
//     范围设成多少，历史结果都留在文件里，不重、不漏、无未提交结果；
//   - 全新启动（无有效检查点）时引擎调用 Reset 截断清零，避免上一轮
//     无关运行的残留输出抑制新结果。
//
// 去重只记一个高水位序号（emit_seq 全局单调、发射按序追加），
// 内存占用 O(1)，不随累计窗口总量增长。
type CommittedFileSink struct {
	mu     sync.Mutex
	f      *os.File
	maxSeq int64 // 已完整落盘的最大 emit_seq
}

// NewCommittedFileSink 打开 path（不存在则创建），扫描恢复已落盘进度。
// 调用方一般在 Run 开始时构造一次。
func NewCommittedFileSink(path string) (*CommittedFileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	s := &CommittedFileSink{f: f}
	if err := s.recoverLocked(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// recoverLocked 扫描既有内容：逐行校验（必须以换行结尾且是合法结果 JSON），
// 用完整行恢复 maxSeq；遇到残缺/损坏行（只可能出现在崩溃点之后的尾部）
// 在该行起点截断，保证半行不会污染后续追加。
func (s *CommittedFileSink) recoverLocked() error {
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var good int64 // 最后一个完整行之后的偏移
	rd := bufio.NewReader(s.f)
	for {
		line, err := rd.ReadBytes('\n')
		if len(line) > 0 {
			var r WindowResult
			// 完整行必须以 '\n' 结尾且整体可解析；否则视为崩溃残留的半行。
			if err := json.Unmarshal(line, &r); err == nil && line[len(line)-1] == '\n' {
				if r.EmitSeq > s.maxSeq {
					s.maxSeq = r.EmitSeq
				}
				good += int64(len(line))
			} else {
				break // 残缺行：截断于此
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	if err := s.f.Truncate(good); err != nil {
		return err
	}
	_, err := s.f.Seek(good, io.SeekStart)
	return err
}

// Reset 在全新启动（无检查点可恢复）时由引擎调用：丢弃既有内容，
// 让本次运行从头重建输出。
func (s *CommittedFileSink) Reset() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.f.Truncate(0); err != nil {
		return err
	}
	if _, err := s.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	s.maxSeq = 0
	return s.f.Sync()
}

func (s *CommittedFileSink) Emit(r *WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 恢复重放会再次投递已落盘的结果：emit_seq 不超过已落盘高水位的
	// 必然已在文件中（文件是发射序列的前缀），幂等跳过，保证不重。
	if r.EmitSeq <= s.maxSeq {
		return nil
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
	s.maxSeq = r.EmitSeq
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
