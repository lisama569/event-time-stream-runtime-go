package stream

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

const (
	// CheckpointVersion 是当前二进制支持的检查点格式版本。
	CheckpointVersion = 1
	checkpointMagic   = "ETSR-CKPT"
)

// 检查点被拒绝的可区分原因。
const (
	CKPTNotFound            = "NOT_FOUND"
	CKPTMalformed           = "MALFORMED"
	CKPTBadMagic            = "BAD_MAGIC"
	CKPTIncompatibleVersion = "INCOMPATIBLE_VERSION"
	CKPTBadConfig           = "BAD_CONFIG"
)

// Checkpoint 是一次一致性快照：各源偏移量与水位、窗口状态、已发射结果。
// Checksum 为除自身外全部字段的规范 JSON 的 SHA-256（hex）。
type Checkpoint struct {
	Magic      string                    `json:"magic"`
	Version    int                       `json:"version"`
	Seq        int64                     `json:"seq"`
	CreatedAt  time.Time                 `json:"created_at"`
	WindowNS   int64                     `json:"window_ns"`
	LatenessNS int64                     `json:"lateness_ns"`
	Sources    map[string]SourceProgress `json:"sources"`
	Windows    []windowState             `json:"window_state"`
	Emitted    []emittedRecord           `json:"emitted"`
	// NextEmitSeq 是下一个发射序号；已发布记录被保留策略修剪后
	// 仍靠它保持 emit_seq 单调。旧版检查点缺省为 0，按 emitted 回推。
	NextEmitSeq int64  `json:"next_emit_seq,omitempty"`
	Checksum    string `json:"checksum"`
}

// SourceProgress 是单个源在检查点中的进度。
// Offset 为已消费记录条数（即下一条应读记录的下标）。
type SourceProgress struct {
	Offset    int64     `json:"offset"`
	Watermark time.Time `json:"watermark"`
}

// checkpointErrorf 构造分类的检查点错误。
func checkpointErrorf(kind, path, format string, args ...any) error {
	return &CheckpointError{Kind: kind, Path: path, Reason: fmt.Sprintf(format, args...)}
}

// buildCheckpoint 由引擎运行态组装快照对象（不负责写盘）。
func buildCheckpoint(seq int64, cfg Config, sources map[string]SourceProgress,
	windows windowSnapshot) *Checkpoint {
	c := &Checkpoint{
		Magic:       checkpointMagic,
		Version:     CheckpointVersion,
		Seq:         seq,
		CreatedAt:   time.Now().UTC(),
		WindowNS:    cfg.WindowSize.Nanoseconds(),
		LatenessNS:  cfg.AllowedLateness.Nanoseconds(),
		Sources:     sources,
		Windows:     windows.Open,
		Emitted:     windows.Emitted,
		NextEmitSeq: windows.NextEmitSeq,
	}
	c.Checksum = computeChecksum(c)
	return c
}

func computeChecksum(c *Checkpoint) string {
	clone := *c
	clone.Checksum = ""
	// 结构体字段顺序固定，encoding/json 输出因此确定。
	data, err := json.Marshal(&clone)
	if err != nil {
		// 本结构全部为可 JSON 序列化类型，出错属于编程错误。
		panic("checkpoint: marshal failed: " + err.Error())
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// SaveCheckpoint 将快照原子写入 path：临时文件 + fsync + rename + 目录 fsync。
// 崩溃时要么保留旧完整文件，要么出现新完整文件，绝不会产生半写文件。
func SaveCheckpoint(path string, ckpt *Checkpoint) error {
	if ckpt == nil {
		return errors.New("checkpoint: nil checkpoint")
	}
	ckpt.Magic = checkpointMagic
	ckpt.Version = CheckpointVersion
	ckpt.Checksum = ""
	ckpt.Checksum = computeChecksum(ckpt)

	data, err := json.MarshalIndent(ckpt, "", "  ")
	if err != nil {
		return fmt.Errorf("checkpoint: marshal: %w", err)
	}

	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".ckpt-*"+".tmp")
	if err != nil {
		return fmt.Errorf("checkpoint: create temp: %w", err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("checkpoint: write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("checkpoint: fsync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("checkpoint: close: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return fmt.Errorf("checkpoint: rename: %w", err)
	}
	// 尽力 fsync 目录，保证 rename 落盘。
	if df, err := os.Open(dir); err == nil {
		_ = df.Sync()
		_ = df.Close()
	}
	return nil
}

// LoadCheckpoint 读取并严格校验检查点，损坏或不兼容时返回 *CheckpointError，
// 通过其 Kind 区分原因：NOT_FOUND / MALFORMED / BAD_MAGIC /
// INCOMPATIBLE_VERSION / BAD_CONFIG。
func LoadCheckpoint(path string, cfg Config) (*Checkpoint, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, checkpointErrorf(CKPTNotFound, path, "checkpoint file does not exist")
		}
		return nil, checkpointErrorf(CKPTMalformed, path, "cannot open: %v", err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, checkpointErrorf(CKPTMalformed, path, "cannot read: %v", err)
	}
	if len(data) == 0 {
		return nil, checkpointErrorf(CKPTMalformed, path, "checkpoint file is empty")
	}

	var c Checkpoint
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, checkpointErrorf(CKPTMalformed, path,
			"content is not valid json (corrupt/truncated): %v", err)
	}
	if c.Magic != checkpointMagic {
		return nil, checkpointErrorf(CKPTBadMagic, path,
			"magic mismatch: want %q, got %q", checkpointMagic, c.Magic)
	}
	if c.Version != CheckpointVersion {
		return nil, checkpointErrorf(CKPTIncompatibleVersion, path,
			"checkpoint version %d is not supported by this binary (supports %d only)",
			c.Version, CheckpointVersion)
	}
	if want := computeChecksum(&c); want != c.Checksum {
		return nil, checkpointErrorf(CKPTMalformed, path,
			"checksum mismatch: file=%s computed=%s (content corrupted or tampered)",
			c.Checksum, want)
	}
	if c.WindowNS != cfg.WindowSize.Nanoseconds() ||
		c.LatenessNS != cfg.AllowedLateness.Nanoseconds() {
		return nil, checkpointErrorf(CKPTBadConfig, path,
			"window/lateness mismatch: checkpoint has window=%s lateness=%s, current config has window=%s lateness=%s",
			time.Duration(c.WindowNS), time.Duration(c.LatenessNS),
			cfg.WindowSize, cfg.AllowedLateness)
	}
	if c.Sources == nil {
		return nil, checkpointErrorf(CKPTMalformed, path, "missing sources section")
	}
	for name, p := range c.Sources {
		if p.Offset < 0 {
			return nil, checkpointErrorf(CKPTMalformed, path,
				"negative offset for source %q", name)
		}
	}
	return &c, nil
}
