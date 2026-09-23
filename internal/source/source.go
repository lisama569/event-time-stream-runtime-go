// Package source provides local data sources and bounded-concurrency ingestion.
//
// Every source emits Envelopes numbered by a per-source monotonic Offset.
// Parsing also happens at the source boundary so malformed input is classified
// close to its origin. Offsets are advanced by the engine only after the
// envelope's effects have been included in a durable checkpoint.
package source

import (
	"context"

	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
)

// Envelope is either a valid event or a classified rejection.
type Envelope struct {
	SourceID string
	Event    event.Event
	Rejected *event.Rejected
	// Offset is this envelope's 0-based position in its source.
	Offset int64
	EOF    bool
}

// Reader is a replayable local source. Replay must be deterministic: reading
// from offset N reproduces exactly the envelopes from position N onward.
type Reader interface {
	ID() string
	// Read sends envelopes from fromOffset (inclusive) onto out until
	// exhausted, then sends exactly one Envelope{EOF:true}. It blocks when
	// out is full (backpressure) and stops promptly on ctx cancellation.
	Read(ctx context.Context, fromOffset int64, out chan<- Envelope)
}

// SliceSource is an in-memory source used by tests and the demo.
type SliceSource struct {
	id     string
	events []event.Event // used by NewSliceSourceEvents (already valid)
	lines  []string      // raw lines parsed lazily ("" => skipped empty)
	opts   event.ParseOptions
}

// NewSliceSource builds an in-memory source from raw JSON lines.
func NewSliceSource(id string, lines []string) *SliceSource {
	return &SliceSource{id: id, lines: append([]string(nil), lines...)}
}

// NewSliceSourceEvents builds an in-memory source from prebuilt events.
func NewSliceSourceEvents(id string, events []event.Event) *SliceSource {
	return &SliceSource{id: id, events: append([]event.Event(nil), events...)}
}

// WithParseOptions sets acceptable event-time bounds for this source.
func (s *SliceSource) WithParseOptions(o event.ParseOptions) *SliceSource { s.opts = o; return s }

func (s *SliceSource) ID() string { return s.id }

func (s *SliceSource) Read(ctx context.Context, fromOffset int64, out chan<- Envelope) {
	emit := func(env Envelope) bool {
		select {
		case out <- env:
			return true
		case <-ctx.Done():
			return false
		}
	}
	offset := int64(0)
	if s.events != nil {
		for _, ev := range s.events {
			if offset < fromOffset {
				offset++
				continue
			}
			ev.SourceID = s.id
			ev.Seq = offset
			if !emit(Envelope{Event: ev, Offset: offset}) {
				return
			}
			offset++
		}
	} else {
		for _, line := range s.lines {
			if offset < fromOffset {
				offset++
				continue
			}
			env := Envelope{Offset: offset}
			ev, err := event.ParseLine(line, s.opts)
			if err == event.ErrEmptyRecord {
				offset++
				continue
			}
			if err != nil {
				r := event.Classify(s.id, offset, line, err)
				env.Rejected = &r
			} else {
				ev.SourceID = s.id
				ev.Seq = offset
				env.Event = ev
			}
			if !emit(env) {
				return
			}
			offset++
		}
	}
	emit(Envelope{EOF: true})
}
