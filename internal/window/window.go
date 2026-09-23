// Package window implements keyed tumbling event-time windows with bounded
// state, allowed-lateness handling and deterministic per-window deduplication.
//
// Lifecycle of a keyed window [start, start+size):
//
//	open    : watermark < end; on-time and (later) late-but-allowed events apply
//	closed  : watermark >= end; closed windows are immutable, no event may
//	          alter them; such events are reported ApplyLateDropped
//	finalized: watermark >= end+allowedLateness; the result is made durable by
//	          the next checkpoint and the window state is purged (bounded state)
//
// Duplicate delivery (same source id + per-source seq, which covers both
// transport redelivery and identical event ids replayed after a crash) is
// suppressed inside a live window via a per-window seen-set, so every
// aggregate is computed once from a unique input set.
package window

import "github.com/lisama569/event-time-stream-runtime-go/internal/event"

// Result is a finalized, deterministic window aggregate.
type Result struct {
	Key         string
	WindowStart int64
	WindowEnd   int64
	Count       int64
	Sum         int64
	// FirstSeq is the lowest (sourceIndex, seq) among contributing events,
	// encoded as sourceIndex*2^40 + seq. It is a deterministic fingerprint of
	// "which input participated first"; useful in failure diagnostics.
	FirstSeq int64
}

// dedupKey identifies an input within a window.
type dedupKey struct {
	Source string
	Seq    int64
}

type winState struct {
	key      string
	start    int64
	count    int64
	sum      int64
	firstSeq int64
	seenSeq  map[dedupKey]struct{} // redelivery with same source position
	seenID   map[string]struct{}   // redelivery with same business event id
	closed   bool                  // immutable once true
}

func (w *winState) end(size int64) int64 { return w.start + size }

// State is the checkpointable window store state.
type State struct {
	Windows []WinState
}

// WinState is the serializable form of one window.
type WinState struct {
	Key      string
	Start    int64
	Count    int64
	Sum      int64
	FirstSeq int64
	SeenSeq  []string // "source:seq"
	SeenID   []string // "source:id"
	Closed   bool
}

// Store manages windows for a fixed size / allowed-lateness configuration.
type Store struct {
	sizeMs          int64
	allowedLateness int64
	windows         map[string]*winState
	pending         []Result // finalized, awaiting durable emission
}

// NewStore creates a window store.
func NewStore(sizeMs, allowedLatenessMs int64) *Store {
	if sizeMs <= 0 {
		sizeMs = 1
	}
	if allowedLatenessMs < 0 {
		allowedLatenessMs = 0
	}
	return &Store{
		sizeMs:          sizeMs,
		allowedLateness: allowedLatenessMs,
		windows:         map[string]*winState{},
	}
}

// WindowStart returns the tumbling-window start for an event time.
func WindowStart(eventTime, sizeMs int64) int64 {
	if eventTime < 0 {
		// floor division
		return ((eventTime - sizeMs + 1) / sizeMs) * sizeMs
	}
	return (eventTime / sizeMs) * sizeMs
}

func windowKey(key string, start int64) string {
	// keys never contain '#' in practice but encode defensively
	return key + "#" + itoa(start)
}

// ApplyResult classifies an Apply call.
type ApplyResult int

const (
	ApplyAccepted ApplyResult = iota
	ApplyDuplicate
	ApplyLateDropped // window closed or purged: result must not change
)

// Apply inserts an event into its window. Purged (already finalized) windows
// and closed windows reject the event as too late.
func (s *Store) Apply(ev event.Event, currentWatermark int64) ApplyResult {
	start := WindowStart(ev.Timestamp, s.sizeMs)
	end := start + s.sizeMs

	// Already finalized and purged?
	if currentWatermark >= end+s.allowedLateness {
		return ApplyLateDropped
	}

	id := windowKey(ev.Key, start)
	w := s.windows[id]
	if w == nil {
		// A new window cannot appear behind the close frontier.
		if currentWatermark >= end {
			return ApplyLateDropped
		}
		w = &winState{
			key:      ev.Key,
			start:    start,
			seenSeq:  map[dedupKey]struct{}{},
			seenID:   map[string]struct{}{},
			firstSeq: 1 << 62,
		}
		s.windows[id] = w
	}
	// Membership is checked before closedness: a redelivery of an event that
	// already contributed is a harmless duplicate even after the window is
	// closed, whereas a genuinely new late event is dropped.
	dk := dedupKey{Source: ev.SourceID, Seq: ev.Seq}
	idK := ev.SourceID + "|" + ev.ID
	_, dupSeq := w.seenSeq[dk]
	_, dupID := w.seenID[idK]
	if dupSeq || dupID {
		return ApplyDuplicate
	}
	if w.closed {
		return ApplyLateDropped
	}
	w.seenSeq[dk] = struct{}{}
	if ev.ID != "" {
		w.seenID[idK] = struct{}{}
	}
	w.count++
	w.sum += ev.Value
	if fp := fingerprint(ev); fp < w.firstSeq {
		w.firstSeq = fp
	}
	return ApplyAccepted
}

// fingerprint deterministically orders inputs by source index then seq.
// sourceIndex is stable per engine run-set (assigned by engine); events from
// the same source carry SourceID, so we hash it to a stable small index only
// for FirstSeq diagnostics -- ordering ties across sources use the hash.
func fingerprint(ev event.Event) int64 {
	// Stable per-source index derived from a FNV-1a of the source id.
	h := uint64(1469598103934665603)
	for i := 0; i < len(ev.SourceID); i++ {
		h ^= uint64(ev.SourceID[i])
		h *= 1099511628211
	}
	srcIdx := int64(h % 4096)
	return srcIdx<<40 | (ev.Seq & ((1 << 40) - 1))
}

// Advance closes windows whose end <= watermark. Closing makes a window
// immutable; finalization/purge happens at end+allowedLateness.
func (s *Store) Advance(watermark int64) int {
	n := 0
	for _, w := range s.windows {
		if !w.closed && watermark >= w.end(s.sizeMs) {
			w.closed = true
			n++
		}
	}
	return n
}

// PurgeOlder finalizes windows with end+allowedLateness <= watermark: their
// Results are queued for exactly-once emission (drained at checkpoint) and
// the window state is removed, bounding state to live windows.
func (s *Store) PurgeOlder(watermark int64) []Result {
	out := []Result{}
	for id, w := range s.windows {
		if watermark >= w.end(s.sizeMs)+s.allowedLateness {
			out = append(out, Result{
				Key:         w.key,
				WindowStart: w.start,
				WindowEnd:   w.end(s.sizeMs),
				Count:       w.count,
				Sum:         w.sum,
				FirstSeq:    w.firstSeq,
			})
			delete(s.windows, id)
		}
	}
	if len(out) > 0 {
		sortResults(out)
		s.pending = append(s.pending, out...)
	}
	return out
}

// DrainPending returns and clears finalized results (called at checkpoint).
func (s *Store) DrainPending() []Result {
	out := s.pending
	s.pending = nil
	return out
}

// PendingResults returns finalized results without clearing.
func (s *Store) PendingResults() []Result { return s.pending }

// ActiveWindows returns the number of live windows (observability/tests).
func (s *Store) ActiveWindows() int { return len(s.windows) }

// Snapshot returns a deep copy for checkpointing.
func (s *Store) Snapshot() State {
	st := State{Windows: make([]WinState, 0, len(s.windows))}
	ids := make([]string, 0, len(s.windows))
	for id := range s.windows {
		ids = append(ids, id)
	}
	sortStrings(ids)
	for _, id := range ids {
		w := s.windows[id]
		seenSeq := make([]string, 0, len(w.seenSeq))
		for dk := range w.seenSeq {
			seenSeq = append(seenSeq, dk.Source+":"+itoa(dk.Seq))
		}
		sortStrings(seenSeq)
		seenID := make([]string, 0, len(w.seenID))
		for k := range w.seenID {
			seenID = append(seenID, k)
		}
		sortStrings(seenID)
		st.Windows = append(st.Windows, WinState{
			Key: w.key, Start: w.start, Count: w.count, Sum: w.sum,
			FirstSeq: w.firstSeq, SeenSeq: seenSeq, SeenID: seenID, Closed: w.closed,
		})
	}
	return st
}

// Restore rebuilds store state from a checkpoint.
func (s *Store) Restore(st State) {
	s.windows = map[string]*winState{}
	s.pending = nil
	for _, w := range st.Windows {
		ws := &winState{
			key:      w.Key,
			start:    w.Start,
			count:    w.Count,
			sum:      w.Sum,
			firstSeq: w.FirstSeq,
			seenSeq:  map[dedupKey]struct{}{},
			seenID:   map[string]struct{}{},
			closed:   w.Closed,
		}
		for _, item := range w.SeenSeq {
			src, seq, ok := splitSeen(item)
			if !ok {
				continue
			}
			ws.seenSeq[dedupKey{Source: src, Seq: seq}] = struct{}{}
		}
		for _, item := range w.SeenID {
			ws.seenID[item] = struct{}{}
		}
		s.windows[windowKey(w.Key, w.Start)] = ws
	}
}
