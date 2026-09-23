package checkpoint

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func sample() *Checkpoint {
	return &Checkpoint{
		EngineID:     "eng",
		SourceOffset: map[string]int64{"a": 5, "b": 9},
		SourceMax:    map[string]int64{"a": 1000, "b": 2000},
		Windows: []WinState{{
			Key: "k#0", Start: 0, Count: 2, Sum: 3, FirstSeq: 1,
			SeenSeq: []string{"a:1", "a:2"}, SeenID: []string{"a|e1"}, Closed: true,
		}},
		Pending: []WinResult{{Key: "k", WindowStart: 0, WindowEnd: 10, Count: 2, Sum: 3}},
	}
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := NewStore(dir, "eng")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := st.Save(ctx, sample()); err != nil {
		t.Fatal(err)
	}
	got, err := st.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Version != CurrentVersion || got.EngineID != "eng" {
		t.Fatalf("header: %+v", got)
	}
	if got.SourceOffset["b"] != 9 || len(got.Windows) != 1 || len(got.Pending) != 1 {
		t.Fatalf("payload mismatch: %+v", got)
	}
	if len(got.Windows[0].SeenSeq) != 2 || len(got.Windows[0].SeenID) != 1 {
		t.Fatalf("seen sets lost: %+v", got.Windows[0])
	}
}

func TestLoadMissing(t *testing.T) {
	st, _ := NewStore(t.TempDir(), "eng")
	if _, err := st.Load(context.Background()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func corrupt(t *testing.T, path string, mutate func(b []byte) []byte) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, mutate(b), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRejections(t *testing.T) {
	ctx := context.Background()

	t.Run("bad_magic", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		_ = st.Save(ctx, sample())
		corrupt(t, st.LatestPath(), func(b []byte) []byte { b[0] = 'X'; return b })
		_, err := st.Load(ctx)
		wantKind(t, err, KindBadMagic)
	})

	t.Run("bad_crc", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		_ = st.Save(ctx, sample())
		corrupt(t, st.LatestPath(), func(b []byte) []byte { b[len(b)-1] ^= 0xFF; return b })
		_, err := st.Load(ctx)
		wantKind(t, err, KindBadCRC)
	})

	t.Run("bad_payload", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		// valid magic/version, valid crc, but payload is not a gob Checkpoint
		payload := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22}
		f, err := os.Create(st.LatestPath())
		if err != nil {
			t.Fatal(err)
		}
		f.Write(magic[:])
		_ = binary.Write(f, binary.LittleEndian, CurrentVersion)
		_ = binary.Write(f, binary.LittleEndian, crc32.ChecksumIEEE(payload))
		f.Write(payload)
		f.Close()
		_, err = st.Load(ctx)
		wantKind(t, err, KindBadPayload)
	})

	t.Run("bad_version", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		_ = st.Save(ctx, sample())
		// rewrite version bytes to 999 without fixing crc: version check must
		// run BEFORE crc check and report bad_version
		corrupt(t, st.LatestPath(), func(b []byte) []byte {
			b[4] = 231
			b[5] = 3
			return b
		})
		_, err := st.Load(ctx)
		ce := wantKind(t, err, KindBadVersion)
		if ce.Version != 999 {
			t.Fatalf("version=%d want 999", ce.Version)
		}
	})

	t.Run("engine_id_mismatch", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		_ = st.Save(ctx, sample())
		st2, _ := NewStore(dir, "other-engine")
		_, err := st2.Load(ctx)
		wantKind(t, err, KindBadPayload)
	})

	t.Run("truncated_file", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		_ = st.Save(ctx, sample())
		corrupt(t, st.LatestPath(), func(b []byte) []byte { return b[:8] })
		_, err := st.Load(ctx)
		var ce *CorruptionError
		if !errors.As(err, &ce) {
			t.Fatalf("want corruption error, got %v", err)
		}
	})

	t.Run("temp_file_leftover_does_not_block", func(t *testing.T) {
		dir := t.TempDir()
		st, _ := NewStore(dir, "eng")
		_ = os.WriteFile(filepath.Join(dir, tmpName), []byte("garbage"), 0o600)
		if _, err := st.Load(ctx); !errors.Is(err, ErrNotFound) {
			t.Fatalf("temp junk should be ignored, got %v", err)
		}
		if err := st.Save(ctx, sample()); err != nil {
			t.Fatal(err)
		}
	})
}

func wantKind(t *testing.T, err error, kind string) *CorruptionError {
	t.Helper()
	var ce *CorruptionError
	if !errors.As(err, &ce) {
		t.Fatalf("want *CorruptionError(%s), got %v", kind, err)
	}
	if ce.Kind != kind {
		t.Fatalf("kind=%s want %s (detail=%s)", ce.Kind, kind, ce.Detail)
	}
	return ce
}
