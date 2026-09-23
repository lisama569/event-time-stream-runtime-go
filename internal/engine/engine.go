package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lisama569/event-time-stream-runtime-go/internal/checkpoint"
	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
	"github.com/lisama569/event-time-stream-runtime-go/internal/source"
	"github.com/lisama569/event-time-stream-runtime-go/internal/watermark"
	"github.com/lisama569/event-time-stream-runtime-go/internal/window"
)

// Sink receives finalized window results. The engine may redeliver a result
// after a crash that occurred while publishing; implementations MUST be
// idempotent for a given (Key, WindowStart). The engine itself guarantees
// each window result is finalized from a unique input set exactly once.
type Sink interface {
	Publish(ctx context.Context, r window.Result) error
}

// RejectHandler observes every classified invalid input (observability).
type RejectHandler func(event.Rejected)

// CrashHook is an internal test hook invoked after a checkpoint has been made
// durable but BEFORE its pending results are published. Production passes nil.
type CrashHook func(cpSeq int64, pending []window.Result)

// Stats summarizes a run for observability/tests.
type Stats struct {
	Processed    int64 // accepted valid events applied
	Duplicates   int64 // redelivered events suppressed by window dedup
	LateDropped  int64 // events arriving after window close/finalization
	Rejected     int64 // classified-invalid envelopes
	Checkpoints  int64
	Published    int64
	EOFSeen      int64
	WatermarkEnd int64 // final global watermark (watermark.NegativeInfinity if none)
}

// Engine is the runtime.
type Engine struct {
	cfg      Config
	sources  []source.Reader
	cpStore  *checkpoint.Store
	sink     Sink
	onReject RejectHandler
	hook     CrashHook

	mu    sync.Mutex
	stats Stats
}

// New constructs an engine and prepares its checkpoint directory.
func New(cfg Config, sources []source.Reader, cpStore *checkpoint.Store, sink Sink, onReject RejectHandler) (*Engine, error) {
	if len(sources) == 0 {
		return nil, errors.New("engine requires at least one source")
	}
	if cpStore == nil {
		return nil, errors.New("engine requires a checkpoint store")
	}
	if sink == nil {
		return nil, errors.New("engine requires a sink")
	}
	if cfg.WindowSize <= 0 {
		return nil, errors.New("WindowSize must be > 0")
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = time.Second
	}
	if cfg.PerSourceBuffer <= 0 {
		cfg.PerSourceBuffer = 32
	}
	return &Engine{
		cfg:      cfg,
		sources:  sources,
		cpStore:  cpStore,
		sink:     sink,
		onReject: onReject,
	}, nil
}

// WithCrashHook installs the internal test hook (see CrashHook).
func (e *Engine) WithCrashHook(h CrashHook) *Engine { e.hook = h; return e }

// Stats returns a copy of the current counters.
func (e *Engine) Stats() Stats {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stats
}

func (e *Engine) add(f func(*Stats)) {
	e.mu.Lock()
	f(&e.stats)
	e.mu.Unlock()
}

// Run processes until all sources reach EOF (or MaxEvents/ctx). On startup it
// recovers from the latest valid checkpoint; corrupt/incompatible/missing-by
// -error checkpoints are rejected as errors with a distinguishable cause.
func (e *Engine) Run(ctx context.Context) (Stats, error) {
	sizeMs := e.cfg.WindowSize.Milliseconds()
	lateMs := e.cfg.AllowedLateness.Milliseconds()
	wmTrack := watermark.NewTracker(e.cfg.OutOfOrderBound.Milliseconds())
	win := window.NewStore(sizeMs, lateMs)
	offsets := map[string]int64{}
	for _, s := range e.sources {
		offsets[s.ID()] = 0
		wmTrack.Init(s.ID())
	}

	// --- Recovery -------------------------------------------------------
	cp, err := e.cpStore.Load(ctx)
	if err != nil && !errors.Is(err, checkpoint.ErrNotFound) {
		// Corrupt / wrong version / incompatible engine id: refuse to start.
		return Stats{}, fmt.Errorf("recovery refused: %w", err)
	}
	if cp != nil {
		for id, off := range cp.SourceOffset {
			if _, ok := offsets[id]; ok {
				offsets[id] = off
			}
		}
		wmTrack.Restore(cp.SourceMax)
		win.Restore(window.State{Windows: fromCPWindows(cp.Windows)})
		// Committed-but-not-yet-published results are published only now that
		// recovery is complete; nothing uncommitted is ever emitted.
		if err := e.publishResults(ctx, fromCPPending(cp.Pending)); err != nil {
			return Stats{}, fmt.Errorf("publish recovered results: %w", err)
		}
	}

	fan := source.NewFanIn(e.sources, e.cfg.PerSourceBuffer)
	in, wait := fan.Start(ctx, offsets)
	defer wait()

	cpTicker := time.NewTicker(e.cfg.CheckpointInterval)
	defer cpTicker.Stop()
	idleTicker := time.NewTicker(time.Second)
	defer idleTicker.Stop()

	lastActivity := map[string]time.Time{}
	finished := map[string]bool{}
	var processed int64
	var cpSeq int64

	checkpointNow := func(forceWM int64) error {
		wm := wmTrack.Watermark()
		if forceWM > wm {
			wm = forceWM
		}
		win.Advance(wm)
		win.PurgeOlder(wm)
		c := &checkpoint.Checkpoint{
			SourceOffset: cloneOffsets(offsets),
			SourceMax:    wmTrack.Snapshot(),
			Windows:      toCPWindows(win.Snapshot().Windows),
			Pending:      toCPPending(win.PendingResults()),
		}
		if err := e.cpStore.Save(ctx, c); err != nil {
			return err
		}
		cpSeq++
		pending := win.DrainPending()
		if e.hook != nil {
			e.hook(cpSeq, pending)
		}
		if err := e.publishResults(ctx, pending); err != nil {
			return err
		}
		// Publishing succeeded: durably clear Pending so a clean restart does
		// not redeliver. If the process died between the two saves (or during
		// publish), recovery redelivers the saved Pending; sinks must be
		// idempotent, and the unique input set is guaranteed by source offsets
		// plus window dedup.
		if len(pending) > 0 {
			c2 := &checkpoint.Checkpoint{
				SourceOffset: cloneOffsets(offsets),
				SourceMax:    wmTrack.Snapshot(),
				Windows:      toCPWindows(win.Snapshot().Windows),
				Pending:      nil,
			}
			if err := e.cpStore.Save(ctx, c2); err != nil {
				return err
			}
		}
		e.add(func(s *Stats) { s.Checkpoints++ })
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return e.Stats(), ctx.Err()
		case <-cpTicker.C:
			if err := checkpointNow(watermark.NegativeInfinity); err != nil {
				return e.Stats(), err
			}
		case <-idleTicker.C:
			if e.cfg.IdleTimeout > 0 {
				cutoff := time.Now().Add(-e.cfg.IdleTimeout)
				for id, t := range lastActivity {
					if !finished[id] && t.Before(cutoff) {
						wmTrack.MarkIdle(id)
					}
				}
			}
		case env, ok := <-in:
			if !ok {
				// Merged channel closed (all EOF seen or cancellation).
				if ctx.Err() != nil {
					return e.Stats(), ctx.Err()
				}
				// Drain a final checkpoint with +inf watermark so every live
				// window closes and finalizes deterministically.
				if err := checkpointNow(1<<62 - 1); err != nil {
					return e.Stats(), err
				}
				s := e.Stats()
				s.WatermarkEnd = 1<<62 - 1
				return s, nil
			}

			if env.EOF {
				// A finished source is NOT idle: its highest event time stays a
				// valid bound on the joint watermark permanently. It ceases to
				// matter only at the forced +inf final checkpoint.
				finished[env.SourceID] = true
				e.add(func(s *Stats) { s.EOFSeen++ })
				continue
			}

			if env.Rejected != nil {
				r := *env.Rejected
				e.add(func(s *Stats) { s.Rejected++ })
				if e.onReject != nil {
					e.onReject(r)
				}
				offsets[env.SourceID] = env.Offset + 1
				processed++
			} else {
				ev := env.Event
				lastActivity[env.SourceID] = time.Now()
				wmTrack.Observe(env.SourceID, ev.Timestamp)
				curWM := wmTrack.Watermark()
				res := win.Apply(ev, curWM)
				switch res {
				case window.ApplyAccepted:
					e.add(func(s *Stats) { s.Processed++ })
				case window.ApplyDuplicate:
					e.add(func(s *Stats) { s.Duplicates++ })
				case window.ApplyLateDropped:
					e.add(func(s *Stats) { s.LateDropped++ })
					if e.onReject != nil {
						e.onReject(event.Rejected{
							SourceID: ev.SourceID,
							Seq:      ev.Seq,
							Raw:      ev.ID,
							Reason:   "late_dropped",
							Detail: fmt.Sprintf("eventTime=%d watermark=%d windowStart=%d windowEnd=%d basis=closed_or_finalized",
								ev.Timestamp, curWM,
								window.WindowStart(ev.Timestamp, sizeMs),
								window.WindowStart(ev.Timestamp, sizeMs)+sizeMs),
						})
					}
				}
				// Duplicates and late drops still consume the input position:
				// they are valid inputs that were processed.
				offsets[env.SourceID] = env.Offset + 1
				processed++

				// Opportunistically close/purge behind the new watermark.
				win.Advance(curWM)
				win.PurgeOlder(curWM)
			}

			if e.cfg.MaxEvents > 0 && processed >= e.cfg.MaxEvents {
				// Simulated crash: stop WITHOUT a trailing checkpoint.
				s := e.Stats()
				s.WatermarkEnd = wmTrack.Watermark()
				return s, nil
			}
		}
	}
}

func (e *Engine) publishResults(ctx context.Context, rs []window.Result) error {
	for _, r := range rs {
		if err := e.sink.Publish(ctx, r); err != nil {
			return err
		}
	}
	if len(rs) > 0 {
		e.add(func(s *Stats) { s.Published += int64(len(rs)) })
	}
	return nil
}

func cloneOffsets(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func toCPWindows(ws []window.WinState) []checkpoint.WinState {
	out := make([]checkpoint.WinState, len(ws))
	for i, w := range ws {
		out[i] = checkpoint.WinState{
			Key: w.Key, Start: w.Start, Count: w.Count, Sum: w.Sum,
			FirstSeq: w.FirstSeq, SeenSeq: w.SeenSeq, SeenID: w.SeenID, Closed: w.Closed,
		}
	}
	return out
}

func fromCPWindows(ws []checkpoint.WinState) []window.WinState {
	out := make([]window.WinState, len(ws))
	for i, w := range ws {
		seenSeq := append([]string(nil), w.SeenSeq...)
		seenID := append([]string(nil), w.SeenID...)
		out[i] = window.WinState{
			Key: w.Key, Start: w.Start, Count: w.Count, Sum: w.Sum,
			FirstSeq: w.FirstSeq, SeenSeq: seenSeq, SeenID: seenID, Closed: w.Closed,
		}
	}
	return out
}

func toCPPending(rs []window.Result) []checkpoint.WinResult {
	out := make([]checkpoint.WinResult, len(rs))
	for i, r := range rs {
		out[i] = checkpoint.WinResult{
			Key: r.Key, WindowStart: r.WindowStart, WindowEnd: r.WindowEnd,
			Count: r.Count, Sum: r.Sum, FirstSeq: r.FirstSeq,
		}
	}
	return out
}

func fromCPPending(rs []checkpoint.WinResult) []window.Result {
	out := make([]window.Result, len(rs))
	for i, r := range rs {
		out[i] = window.Result{
			Key: r.Key, WindowStart: r.WindowStart, WindowEnd: r.WindowEnd,
			Count: r.Count, Sum: r.Sum, FirstSeq: r.FirstSeq,
		}
	}
	return out
}
