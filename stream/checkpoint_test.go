package stream

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func validCfg() Config {
	c := DefaultConfig()
	c.WindowSize = time.Minute
	c.AllowedLateness = 10 * time.Second
	return c
}

func TestCheckpointRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "ckpt.json")
	store := NewWindowStore(time.Minute, 10*time.Second)
	wm := time.Time{}
	store.Add(ev("e1", "a", "2026-01-01T00:00:05Z", 1), wm)
	wm2 := iso(t, "2026-01-01T00:01:10Z")
	id := WindowID{Key: "a", Start: iso(t, "2026-01-01T00:00:00Z")}
	store.Close(id, wm2, wm2, 1)
	snap := store.Snapshot(time.Time{})
	ck := buildCheckpoint(1, validCfg(), map[string]SourceProgress{
		"s": {Offset: 5, Watermark: wm2},
	}, snap)
	if err := SaveCheckpoint(p, ck); err != nil {
		t.Fatal(err)
	}
	got, err := LoadCheckpoint(p, validCfg())
	if err != nil {
		t.Fatalf("valid checkpoint rejected: %v", err)
	}
	if got.Version != CheckpointVersion || got.Seq != 1 {
		t.Fatalf("header wrong: %+v", got)
	}
	if got.Sources["s"].Offset != 5 {
		t.Fatalf("offset wrong: %+v", got.Sources)
	}
	if len(got.Emitted) != 1 || got.Emitted[0].R.Key != "a" {
		t.Fatalf("emitted not restored: %+v", got.Emitted)
	}
	// 原子写：不应遗留临时文件。
	if fis, _ := os.ReadDir(dir); len(fis) != 1 {
		t.Fatalf("expected exactly one file in dir, got %v", fis)
	}
}

func TestCheckpointRejections(t *testing.T) {
	dir := t.TempDir()
	cfg := validCfg()

	// 先造一个有效检查点。
	p := filepath.Join(dir, "ckpt.json")
	good := buildCheckpoint(1, cfg, map[string]SourceProgress{"s": {Offset: 1}},
		windowSnapshot{})
	if err := SaveCheckpoint(p, good); err != nil {
		t.Fatal(err)
	}

	t.Run("not found", func(t *testing.T) {
		_, err := LoadCheckpoint(filepath.Join(dir, "missing.json"), cfg)
		assertKind(t, err, CKPTNotFound)
	})

	t.Run("empty file", func(t *testing.T) {
		q := filepath.Join(dir, "empty.json")
		if err := os.WriteFile(q, []byte(""), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := LoadCheckpoint(q, cfg)
		assertKind(t, err, CKPTMalformed)
	})

	t.Run("truncated json", func(t *testing.T) {
		q := filepath.Join(dir, "trunc.json")
		if err := os.WriteFile(q, []byte(`{"magic":"ETSR-CKPT","versio`), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := LoadCheckpoint(q, cfg)
		assertKind(t, err, CKPTMalformed)
	})

	t.Run("tampered content fails checksum", func(t *testing.T) {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		tampered := strings.Replace(string(raw), `"seq": 1`, `"seq": 9`, 1)
		q := filepath.Join(dir, "tampered.json")
		if err := os.WriteFile(q, []byte(tampered), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err = LoadCheckpoint(q, cfg)
		assertKind(t, err, CKPTMalformed) // 校验和不符归入 MALFORMED
	})

	t.Run("bad magic", func(t *testing.T) {
		q := filepath.Join(dir, "magic.json")
		bad := buildCheckpoint(1, cfg, map[string]SourceProgress{"s": {}},
			windowSnapshot{})
		bad.Magic = "SOMETHING-ELSE"
		bad.Checksum = computeChecksum(bad)
		writeRaw(t, q, bad)
		_, err := LoadCheckpoint(q, cfg)
		assertKind(t, err, CKPTBadMagic)
	})

	t.Run("incompatible version", func(t *testing.T) {
		q := filepath.Join(dir, "ver.json")
		bad := buildCheckpoint(1, cfg, map[string]SourceProgress{"s": {}},
			windowSnapshot{})
		bad.Version = CheckpointVersion + 99
		bad.Checksum = computeChecksum(bad)
		writeRaw(t, q, bad)
		_, err := LoadCheckpoint(q, cfg)
		assertKind(t, err, CKPTIncompatibleVersion)
	})

	t.Run("config mismatch window size", func(t *testing.T) {
		cfg2 := cfg
		cfg2.WindowSize = 30 * time.Second
		_, err := LoadCheckpoint(p, cfg2)
		assertKind(t, err, CKPTBadConfig)
	})

	t.Run("config mismatch lateness", func(t *testing.T) {
		cfg2 := cfg
		cfg2.AllowedLateness = 30 * time.Second
		_, err := LoadCheckpoint(p, cfg2)
		assertKind(t, err, CKPTBadConfig)
	})
}

func writeRaw(t *testing.T, path string, c *Checkpoint) {
	t.Helper()
	data, err := jsonMarshalIndent(c)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertKind(t *testing.T, err error, kind string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected rejection %s, got nil", kind)
	}
	ce, ok := err.(*CheckpointError)
	if !ok {
		t.Fatalf("expected *CheckpointError, got %T: %v", err, err)
	}
	t.Logf("rejected as %s: %s | decision: checkpoint version=%d, supported=%d",
		ce.Kind, ce.Reason, CheckpointVersion, CheckpointVersion)
	if ce.Kind != kind {
		t.Fatalf("kind: want %s got %s (reason: %s)", kind, ce.Kind, ce.Reason)
	}
}
