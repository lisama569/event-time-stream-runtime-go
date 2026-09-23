// Package checkpoint provides durable, atomic, versioned engine state.
//
// File layout (little-endian):
//
//	magic   [4]byte  = "ETSC"
//	version uint32   (must equal CurrentVersion)
//	crc32   uint32   (IEEE CRC of the payload that follows)
//	payload []byte   (encoding/gob of Checkpoint)
//
// Save is crash-safe: payload is written to a temp file in the same
// directory, fsynced, then atomically renamed over the live file and the
// directory is fsynced. Load rejects missing/magic/version/CRC/payload
// failures with distinct *CorruptionError reasons so callers (and tests)
// can tell corruption from incompatibility from "no checkpoint yet".
package checkpoint

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

// CurrentVersion is the only checkpoint format this binary understands.
const CurrentVersion uint32 = 1

const (
	dirPerm  = 0o755
	filePerm = 0o600
)

var magic = [4]byte{'E', 'T', 'S', 'C'}

const fileName = "checkpoint.bin"
const tmpName = "checkpoint.bin.tmp"

// ErrNotFound indicates no checkpoint exists.
var ErrNotFound = errors.New("checkpoint not found")

// Corruption error kinds, distinguishable by Kind.
const (
	KindMissingFile = "missing_file"
	KindBadMagic    = "bad_magic"
	KindBadVersion  = "bad_version"
	KindBadCRC      = "bad_crc"
	KindBadPayload  = "bad_payload"
)

// CorruptionError distinguishes checkpoint rejection causes.
type CorruptionError struct {
	Kind    string
	Detail  string
	Version uint32 // parsed version if available (0 otherwise)
}

func (e *CorruptionError) Error() string {
	return fmt.Sprintf("checkpoint rejected (%s, version=%d): %s", e.Kind, e.Version, e.Detail)
}

// WinState is a serialized window (mirrors window state).
type WinState struct {
	Key      string
	Start    int64
	Count    int64
	Sum      int64
	FirstSeq int64
	SeenSeq  []string
	SeenID   []string
	Closed   bool
}

// WinResult is a finalized result held for exactly-once emission.
type WinResult struct {
	Key         string
	WindowStart int64
	WindowEnd   int64
	Count       int64
	Sum         int64
	FirstSeq    int64
}

// Checkpoint is the full durable engine state.
type Checkpoint struct {
	Version      uint32
	EngineID     string
	SourceOffset map[string]int64 // next-to-read offsets (processed + saved)
	SourceMax    map[string]int64 // watermark raw maxima
	Windows      []WinState
	Pending      []WinResult // finalized results not yet externally published
}

// Store durably persists checkpoints under a local directory.
type Store struct {
	dir      string
	engineID string
}

// NewStore prepares the checkpoint directory.
func NewStore(dir, engineID string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("checkpoint dir must not be empty")
	}
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	return &Store{dir: dir, engineID: engineID}, nil
}

func (s *Store) tmpPath() string    { return filepath.Join(s.dir, tmpName) }
func (s *Store) LatestPath() string { return filepath.Join(s.dir, fileName) }

// Save atomically persists cp.
func (s *Store) Save(ctx context.Context, cp *Checkpoint) error {
	if cp == nil {
		return errors.New("nil checkpoint")
	}
	cp.Version = CurrentVersion
	cp.EngineID = s.engineID

	var payload bytes.Buffer
	if err := gob.NewEncoder(&payload).Encode(cp); err != nil {
		return fmt.Errorf("encode checkpoint: %w", err)
	}
	sum := crc32.ChecksumIEEE(payload.Bytes())

	var buf bytes.Buffer
	buf.Write(magic[:])
	_ = binary.Write(&buf, binary.LittleEndian, CurrentVersion)
	_ = binary.Write(&buf, binary.LittleEndian, sum)
	if _, err := io.Copy(&buf, &payload); err != nil {
		return err
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	tmp := s.tmpPath()
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, filePerm)
	if err != nil {
		return fmt.Errorf("create temp checkpoint: %w", err)
	}
	if _, err := f.Write(buf.Bytes()); err != nil {
		f.Close()
		return fmt.Errorf("write temp checkpoint: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("fsync temp checkpoint: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close temp checkpoint: %w", err)
	}
	if err := os.Rename(tmp, s.LatestPath()); err != nil {
		return fmt.Errorf("rename checkpoint: %w", err)
	}
	// fsync directory so the rename itself is durable.
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("open checkpoint dir for fsync: %w", err)
	}
	err = d.Sync()
	_ = d.Close()
	if err != nil {
		return fmt.Errorf("fsync checkpoint dir: %w", err)
	}
	return nil
}

// Load reads and validates the latest checkpoint.
func (s *Store) Load(ctx context.Context) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(s.LatestPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		}
		return nil, &CorruptionError{Kind: KindMissingFile, Detail: err.Error()}
	}
	if len(data) < 16 {
		return nil, &CorruptionError{Kind: KindBadPayload, Detail: fmt.Sprintf("file too short: %d bytes", len(data))}
	}
	if string(data[0:4]) != string(magic[:]) {
		return nil, &CorruptionError{Kind: KindBadMagic, Detail: "magic bytes do not match ETSC"}
	}
	version := binary.LittleEndian.Uint32(data[4:8])
	if version != CurrentVersion {
		return nil, &CorruptionError{
			Kind:    KindBadVersion,
			Version: version,
			Detail:  fmt.Sprintf("checkpoint version %d unsupported by binary version %d", version, CurrentVersion),
		}
	}
	storedCRC := binary.LittleEndian.Uint32(data[8:12])
	payload := data[12:]
	if calc := crc32.ChecksumIEEE(payload); calc != storedCRC {
		return nil, &CorruptionError{
			Kind:    KindBadCRC,
			Version: version,
			Detail:  fmt.Sprintf("crc mismatch: stored=%08x calculated=%08x", storedCRC, calc),
		}
	}
	cp := &Checkpoint{}
	if err := gob.NewDecoder(bytes.NewReader(payload)).Decode(cp); err != nil {
		return nil, &CorruptionError{Kind: KindBadPayload, Version: version, Detail: err.Error()}
	}
	if cp.EngineID != "" && s.engineID != "" && cp.EngineID != s.engineID {
		return nil, &CorruptionError{
			Kind:    KindBadPayload,
			Version: version,
			Detail:  fmt.Sprintf("engine id mismatch: checkpoint=%q binary=%q", cp.EngineID, s.engineID),
		}
	}
	return cp, nil
}
