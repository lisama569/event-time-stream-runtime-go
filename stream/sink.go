package stream

import (
	"bufio"
	"encoding/json"
	"errors"
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

// CommittedFileSink 是与检查点协议配套的本地文件汇，输出文件是一份
// 只增不改的完整结果历史（JSON Lines，一行一个窗口结果，按 emit_seq 升序）：
//
//   - 打开时不再截断：输出文件本身承载全部已持久化的历史结果，包括那些
//     早已被 EmittedRetention 从内存/检查点中修剪掉的窗口；
//   - PrepareResume(false)：无检查点的全新启动，截断旧文件（不可能存在
//     已提交历史）；
//   - PrepareResume(true)：有检查点的恢复启动，流式扫描旧文件，验证其为
//     严格连续的 emit_seq 前缀，剥掉崩溃残留的最后半行，记录“已持久化
//     前缀”长度；随后引擎重放检查点保留尾部时，前缀内的结果被去重跳过，
//     新结果追加写入。因此即使保留范围有限，输出文件也一条历史都不少，
//     且每个窗口恰好一行；
//   - 引擎仅在检查点提交成功后才把新结果以“一次 Write 一行 + fsync”追加。
//
// 被修剪出检查点的结果一定早已写进输出文件（修剪只发生在成功提交之后），
// 所以有界的只是检查点与内存状态，输出历史保持完整。
type CommittedFileSink struct {
	mu sync.Mutex
	f  *os.File
	// durableSeq 是输出文件中已完整持久化的最大 emit_seq；0 表示尚无完整行。
	// 重放期间 emit_seq <= durableSeq 的结果视为已在文件中，去重跳过。
	durableSeq int64
	// prepared 标记 PrepareResume 是否已经调用（Run 中恰好调用一次）。
	prepared bool
}

// NewCommittedFileSink 以读写追加方式打开 path（不存在则创建），不截断。
// O_APPEND 保证每次写入都落在文件末尾；恢复扫描用 Seek 定位读位置。
// 输出文件的截断或恢复对账由 PrepareResume 在 Run 开始时完成。
func NewCommittedFileSink(path string) (*CommittedFileSink, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &CommittedFileSink{f: f}, nil
}

// PrepareResume 在引擎 Run 开始、尚未重放任何结果前调用一次。
// fromCheckpoint=false 表示不存在可用检查点（全新启动）：输出文件截断为空；
// fromCheckpoint=true 表示将从检查点恢复：扫描并对账已有输出，剥掉残缺末行。
//
// 恢复对账只保留与文件长度成比例的扫描缓冲（常数状态），不把历史结果
// 读入内存，因此内存占用不随累计窗口总量增长。
func (s *CommittedFileSink) PrepareResume(fromCheckpoint bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prepared {
		return errors.New("stream: CommittedFileSink already prepared")
	}
	s.prepared = true
	if !fromCheckpoint {
		// 全新启动：没有任何已提交状态，旧输出不可能属于本次逻辑运行，截断。
		if err := s.f.Truncate(0); err != nil {
			return err
		}
		if _, err := s.f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		s.durableSeq = 0
		return nil
	}
	durable, validBytes, err := scanDurableOutput(s.f)
	if err != nil {
		return err
	}
	// 剥掉最后一个换行之后的残缺内容（崩溃时写了一半的末行）。
	if err := s.f.Truncate(validBytes); err != nil {
		return err
	}
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	s.durableSeq = durable
	return nil
}

// scanDurableOutput 以流式方式扫描 JSON Lines 输出文件，返回其中完整行的
// 最大 emit_seq 与最后一个换行符之后的字节偏移（即应保留的完整前缀长度）。
// 完整行必须可解析为 WindowResult，且 emit_seq 从 1 起严格连续、无重号；
// 末尾缺换行的最后一段按“崩溃半行”处理（不计入、留给调用方截断）。
func scanDurableOutput(f *os.File) (maxSeq int64, validBytes int64, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	br := bufio.NewReaderSize(f, 64*1024)
	var offset int64
	var expect int64 = 1
	var lastNL int64 = -1
	for {
		chunk, readErr := br.ReadSlice('\n')
		offset += int64(len(chunk))
		if readErr == bufio.ErrBufferFull {
			// 完整行超出缓冲：我们的输出行远小于 64KiB，出现这种行说明
			// 文件不是本汇写出的，直接拒绝而不是静默丢弃历史。
			return 0, 0, fmt.Errorf("stream: output line too long at byte %d (file is not a valid engine output?)", offset)
		}
		// chunk 以 '\n' 结尾：完整行（可能后面还有内容，也可能 EOF）。
		if readErr == nil {
			line := chunk[:len(chunk)-1]
			seq, perr := parseOutputLineSeq(line)
			if perr != nil {
				return 0, 0, perr
			}
			if seq != expect {
				return 0, 0, fmt.Errorf("stream: output file emit_seq not contiguous at byte %d: got %d, want %d (output file truncated or mixed with another run?)",
					offset, seq, expect)
			}
			expect++
			if seq > maxSeq {
				maxSeq = seq
			}
			lastNL = offset
			continue
		}
		if readErr == io.EOF {
			// chunk 是最后一个换行之后的残余（可能为空、完整无换行行或半行）。
			tail := chunk
			if len(tail) > 0 {
				// 无换行收尾：引擎写入总是带 '\n'，因此只能是崩溃半行——
				// 即使它恰好能解析，也按未完成写入截断重放，绝不信任它。
				// lastNL 之前的完整行全部保留。
				break
			}
			break
		}
		return 0, 0, readErr
	}
	if lastNL >= 0 {
		validBytes = lastNL
	}
	return maxSeq, validBytes, nil
}

// parseOutputLineSeq 解析一行输出，校验其为合法的窗口结果并返回 emit_seq。
func parseOutputLineSeq(line []byte) (int64, error) {
	var r WindowResult
	if err := json.Unmarshal(line, &r); err != nil {
		return 0, fmt.Errorf("stream: corrupt complete line in output file: %v", err)
	}
	if r.EmitSeq <= 0 {
		return 0, fmt.Errorf("stream: output line with non-positive emit_seq=%d", r.EmitSeq)
	}
	return r.EmitSeq, nil
}

func (s *CommittedFileSink) Emit(r *WindowResult) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.prepared {
		// 防御：必须先 PrepareResume，避免在未对账的文件上盲目追加。
		return errors.New("stream: CommittedFileSink used before PrepareResume")
	}
	// 恢复重放：该结果已经完整存在于输出文件的已持久化前缀中，去重跳过。
	// emit_seq 是从 1 起严格连续的全局序号，前缀内不可能再有“同窗口不同行”。
	if r.EmitSeq > 0 && r.EmitSeq <= s.durableSeq {
		return nil
	}
	if r.EmitSeq != s.durableSeq+1 {
		return fmt.Errorf("stream: out-of-order emit: got seq %d, want %d",
			r.EmitSeq, s.durableSeq+1)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	// 一次 Write 写完整的一行（含换行）：POSIX 下 O_APPEND 的写带更新偏移
	// 原子性，半行只可能来自写中途的崩溃，而不会来自交错拼接。
	if _, err := s.f.Write(append(data, '\n')); err != nil {
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.durableSeq = r.EmitSeq
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

// DurableSeq 返回输出文件中已完整持久化的最大 emit_seq（恢复对账后可用）。
func (s *CommittedFileSink) DurableSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.durableSeq
}

var _ Sink = (*CommittedFileSink)(nil)
