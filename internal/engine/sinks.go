package engine

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/lisama569/event-time-stream-runtime-go/internal/window"
)

// LogSink writes deterministic one-line results to a writer. Lines are
// ordered by checkpoint order (which is deterministic: window start, key).
type LogSink struct {
	mu sync.Mutex
	w  io.Writer
}

func NewLogSink(w io.Writer) *LogSink { return &LogSink{w: w} }

func (s *LogSink) Publish(ctx context.Context, r window.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := fmt.Fprintf(s.w,
		"RESULT key=%s window=[%d,%d) count=%d sum=%d firstSeq=%d\n",
		r.Key, r.WindowStart, r.WindowEnd, r.Count, r.Sum, r.FirstSeq)
	return err
}

// MemorySink records results in arrival order (test aid).
type MemorySink struct {
	mu      sync.Mutex
	results []window.Result
}

func NewMemorySink() *MemorySink { return &MemorySink{} }

func (s *MemorySink) Publish(ctx context.Context, r window.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.results = append(s.results, r)
	return nil
}

// Results returns a snapshot copy.
func (s *MemorySink) Results() []window.Result {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]window.Result, len(s.results))
	copy(out, s.results)
	return out
}
