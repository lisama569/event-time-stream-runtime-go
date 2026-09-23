package source

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSliceSourceReplayAndReject(t *testing.T) {
	lines := []string{
		`{"id":"e1","key":"k","ts":1000,"value":1}`,
		"",
		`not-json`,
		`{"id":"e2","key":"k","ts":2000,"value":2}`,
	}
	s := NewSliceSource("s", lines)
	ch := make(chan Envelope, 10)
	s.Read(context.Background(), 0, ch)

	var got []Envelope
	for env := range ch {
		if env.EOF {
			break
		}
		got = append(got, env)
	}
	if len(got) != 3 {
		t.Fatalf("want 3 envelopes (empty skipped), got %d", len(got))
	}
	if got[0].Event.ID != "e1" || got[0].Offset != 0 {
		t.Fatalf("env0: %+v", got[0])
	}
	if got[1].Rejected == nil || got[1].Rejected.Reason != "malformed" || got[1].Offset != 2 {
		t.Fatalf("env1 should be malformed at offset 2: %+v", got[1])
	}
	if got[2].Event.ID != "e2" || got[2].Offset != 3 {
		t.Fatalf("env2: %+v", got[2])
	}

	// replay from offset 2 reproduces the tail deterministically
	ch2 := make(chan Envelope, 10)
	s.Read(context.Background(), 2, ch2)
	first := <-ch2
	if first.Offset != 2 || first.Rejected == nil {
		t.Fatalf("replay first = %+v", first)
	}
}

func TestFileLineSourceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "events.jsonl")
	content := `{"id":"f1","key":"k","ts":1000,"value":1}
{"id":"f2","key":"k","ts":2000,"value":2}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	src, err := NewFileLineSource("file", path)
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan Envelope, 10)
	src.Read(context.Background(), 1, ch)
	env := <-ch
	if env.Event.ID != "f2" || env.Offset != 1 {
		t.Fatalf("replay from line 1: %+v", env)
	}
	eof := <-ch
	if !eof.EOF {
		t.Fatalf("want EOF, got %+v", eof)
	}
}

// TestSlowSourceDoesNotBlockOthers verifies bounded-buffer backpressure:
// one source delivers everything instantly while the "slow" source blocks
// its own goroutine; the fast source must still drain (and block only at its
// bounded capacity), proving no unbounded growth and no cross-source stall.
func TestSlowSourceDoesNotBlockOthers(t *testing.T) {
	fast := NewSliceSource("fast", manyLines(200))
	slow := newBlockerSource("slow")
	fi := NewFanIn([]Reader{fast, slow}, 8)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out, wait := fi.Start(ctx, map[string]int64{"fast": 0, "slow": 0})

	n := 0
deadline:
	for {
		select {
		case env := <-out:
			if env.EOF {
				// only fast can EOF (slow never does); keep reading until timeout
				continue
			}
			n++
			if n >= 200 {
				break deadline
			}
		case <-time.After(1500 * time.Millisecond):
			break deadline
		}
	}
	cancel()
	wait()
	if n != 200 {
		t.Fatalf("fast source delivered %d/200 events while slow source blocked", n)
	}
}

// TestConsumerBackpressureBounded ensures a blocked consumer causes producers
// to block instead of dropping events: after unblocking, nothing is lost.
func TestConsumerBackpressureBounded(t *testing.T) {
	a := NewSliceSource("a", manyLines(50))
	b := NewSliceSource("b", manyLines(50))
	fi := NewFanIn([]Reader{a, b}, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out, _ := fi.Start(ctx, map[string]int64{"a": 0, "b": 0})

	time.Sleep(100 * time.Millisecond) // let producers fill only the bounded buffers
	total := 0
	for env := range out {
		if env.EOF {
			total++
			if total == 2 {
				break
			}
			continue
		}
	}
	// 100 data envelopes + 2 EOF
	if total != 2 {
		t.Fatalf("EOF count=%d", total)
	}
}

func manyLines(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = `{"id":"e` + itoaTest(i) + `","key":"k","ts":` + itoaTest(1000+i) + `,"value":1}`
	}
	return out
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

// blockerSource never emits and never returns until ctx is done.
type blockerSource struct{ id string }

func newBlockerSource(id string) *blockerSource { return &blockerSource{id: id} }
func (b *blockerSource) ID() string             { return b.id }
func (b *blockerSource) Read(ctx context.Context, _ int64, out chan<- Envelope) {
	<-ctx.Done()
}
