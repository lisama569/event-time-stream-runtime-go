// Package watermark tracks per-source event-time progress and derives the
// global watermark as the minimum across active (non-idle) sources.
//
// The per-source watermark lags the highest observed event time by
// outOfOrderLateness, expressing "no event with timestamp < W is expected
// from this source". The global watermark is the min over active sources so
// the joint stream advances only as fast as its slowest live source; sources
// can be marked idle (no data for IdleTimeout) so a quiet source does not
// hold back the whole pipeline forever.
package watermark

import "sync"

// NegativeInfinity is the watermark before any active source has produced data.
const NegativeInfinity int64 = -1 << 62

// Tracker keeps the high-water observed event time per source.
type Tracker struct {
	mu                 sync.Mutex
	sourceMax          map[string]int64 // raw max observed event time
	idle               map[string]bool
	outOfOrderLateness int64
}

// NewTracker creates a tracker with the given event-time out-of-order bound.
func NewTracker(outOfOrderLatenessMs int64) *Tracker {
	if outOfOrderLatenessMs < 0 {
		outOfOrderLatenessMs = 0
	}
	return &Tracker{
		sourceMax:          map[string]int64{},
		idle:               map[string]bool{},
		outOfOrderLateness: outOfOrderLatenessMs,
	}
}

// Init registers a source. It starts with no observation and is active but
// excluded from the min until its first event (watermark -inf semantics).
func (t *Tracker) Init(sourceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.sourceMax[sourceID]; !ok {
		t.sourceMax[sourceID] = NegativeInfinity
	}
	t.idle[sourceID] = false
}

// MarkIdle marks a source idle: it is excluded from the global min until it
// emits again. An idle source that never saw data stays at -inf anyway.
func (t *Tracker) MarkIdle(sourceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.idle[sourceID] = true
}

// Observe records an event time for a source, clears idle and bumps the max.
func (t *Tracker) Observe(sourceID string, eventTime int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if cur, ok := t.sourceMax[sourceID]; !ok || eventTime > cur {
		t.sourceMax[sourceID] = eventTime
	}
	t.idle[sourceID] = false
}

// Restore replaces tracker state from a checkpoint (all sources active).
func (t *Tracker) Restore(sourceMax map[string]int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sourceMax = make(map[string]int64, len(sourceMax))
	for k, v := range sourceMax {
		t.sourceMax[k] = v
	}
	t.idle = make(map[string]bool, len(sourceMax))
	for k := range sourceMax {
		t.idle[k] = false
	}
}

// Snapshot returns a copy of the per-source raw maxima for checkpointing.
// Values equal to NegativeInfinity (never produced) are omitted.
func (t *Tracker) Snapshot() map[string]int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int64, len(t.sourceMax))
	for k, v := range t.sourceMax {
		if v != NegativeInfinity {
			out[k] = v
		}
	}
	return out
}

// PerSource returns the lag-adjusted watermark of one source.
func (t *Tracker) PerSource(sourceID string) int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.sourceMax[sourceID]
	if !ok || v == NegativeInfinity {
		return NegativeInfinity
	}
	return v - t.outOfOrderLateness
}

// Watermark returns the global watermark: min of lag-adjusted maxima over
// all active (non-idle) sources. A source that was Init'ed but has produced
// no data yet holds the watermark at NegativeInfinity, so windows never close
// before every source has started. A source that reached EOF is NOT idle: its
// max stays a valid permanent bound. NegativeInfinity means "unknown".
func (t *Tracker) Watermark() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	w := int64(1<<62 - 1)
	found := false
	for sid, max := range t.sourceMax {
		if t.idle[sid] {
			continue
		}
		found = true
		if max == NegativeInfinity {
			// a registered, not-yet-started source pins the watermark down
			return NegativeInfinity
		}
		if v := max - t.outOfOrderLateness; v < w {
			w = v
		}
	}
	if !found {
		return NegativeInfinity
	}
	return w
}
