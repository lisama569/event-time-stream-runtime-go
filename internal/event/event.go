// Package event defines the event model, parsing and rejection classification.
//
// On-disk/line format is one JSON object per line:
//
//	{"id":"e1","key":"k1","ts":"2026-09-23T10:00:00Z","value":3}
//
// "ts" may also be an integer number of unix milliseconds. Internal time is
// always int64 unix milliseconds so that window math is deterministic.
package event

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Event is a parsed, valid event in internal (int64 unix-millisecond) time.
type Event struct {
	ID        string
	Key       string
	Timestamp int64 // event time, unix milliseconds
	Value     int64
	SourceID  string // populated by the source that ingested it
	Seq       int64  // per-source monotonic sequence (for dedup/replay)
}

// RejectReason categorizes invalid input.
type RejectReason string

const (
	RejectMalformed RejectReason = "malformed"   // unparsable line / bad payload
	RejectBadTime   RejectReason = "bad_time"    // missing/unparsable/zero timestamp
	RejectOutOfSpan RejectReason = "out_of_span" // timestamp outside configured range
)

// Rejected is an explicitly classified invalid input. Raw lines are kept so
// the failure is observable and explainable.
type Rejected struct {
	SourceID string
	Seq      int64
	Raw      string
	Reason   RejectReason
	Detail   string
}

func (r Rejected) String() string {
	return fmt.Sprintf("rejected source=%s seq=%d reason=%s detail=%q raw=%q",
		r.SourceID, r.Seq, r.Reason, r.Detail, truncate(r.Raw, 120))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// ErrEmptyRecord is returned for blank lines handled by callers.
var ErrEmptyRecord = errors.New("empty record")

// ParseOptions bounds acceptable event time; zero MinTime/MaxTime = unbounded.
type ParseOptions struct {
	MinTime int64 // inclusive unix-ms
	MaxTime int64 // inclusive unix-ms
}

// wireEvent is the JSON representation. ts accepts RFC3339 string or unix-ms int.
type wireEvent struct {
	ID    string      `json:"id"`
	Key   string      `json:"key"`
	TS    interface{} `json:"ts"`
	Value int64       `json:"value"`
}

// ParseLine parses one JSON line into an Event (SourceID/Seq filled later).
func ParseLine(line string, opts ParseOptions) (Event, error) {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return Event{}, ErrEmptyRecord
	}
	var w wireEvent
	if err := json.Unmarshal([]byte(trimmed), &w); err != nil {
		return Event{}, fmt.Errorf("%w: %v", errMalformed, err)
	}
	if w.ID == "" {
		return Event{}, fmt.Errorf("%w: missing id", errMalformed)
	}
	if w.Key == "" {
		return Event{}, fmt.Errorf("%w: missing key", errMalformed)
	}
	ts, err := parseTS(w.TS)
	if err != nil {
		return Event{}, err
	}
	if opts.MinTime != 0 && ts < opts.MinTime {
		return Event{}, fmt.Errorf("%w: ts %d < min %d", errOutOfSpan, ts, opts.MinTime)
	}
	if opts.MaxTime != 0 && ts > opts.MaxTime {
		return Event{}, fmt.Errorf("%w: ts %d > max %d", errOutOfSpan, ts, opts.MaxTime)
	}
	return Event{ID: w.ID, Key: w.Key, Timestamp: ts, Value: w.Value}, nil
}

var (
	errMalformed = errors.New("malformed event")
	errBadTime   = errors.New("bad event timestamp")
	errOutOfSpan = errors.New("event timestamp out of allowed range")
)

func parseTS(v interface{}) (int64, error) {
	switch t := v.(type) {
	case nil:
		return 0, fmt.Errorf("%w: missing ts", errBadTime)
	case string:
		if t == "" {
			return 0, fmt.Errorf("%w: empty ts", errBadTime)
		}
		tm, err := time.Parse(time.RFC3339Nano, t)
		if err != nil {
			// tolerate integer encoded as string
			if n, err2 := strconv.ParseInt(t, 10, 64); err2 == nil {
				if n <= 0 {
					return 0, fmt.Errorf("%w: non-positive unix-ms %d", errBadTime, n)
				}
				return n, nil
			}
			return 0, fmt.Errorf("%w: %v", errBadTime, err)
		}
		n := tm.UnixMilli()
		if n <= 0 {
			return 0, fmt.Errorf("%w: non-positive time %q", errBadTime, t)
		}
		return n, nil
	case float64: // JSON numbers decode as float64
		if t <= 0 {
			return 0, fmt.Errorf("%w: non-positive unix-ms %v", errBadTime, t)
		}
		return int64(t), nil
	default:
		return 0, fmt.Errorf("%w: unsupported ts type %T", errBadTime, v)
	}
}

// Classify turns a parse error into a typed Rejected record.
func Classify(sourceID string, seq int64, raw string, err error) Rejected {
	reason := RejectMalformed
	switch {
	case errors.Is(err, errBadTime):
		reason = RejectBadTime
	case errors.Is(err, errOutOfSpan):
		reason = RejectOutOfSpan
	case errors.Is(err, errMalformed):
		reason = RejectMalformed
	}
	return Rejected{
		SourceID: sourceID,
		Seq:      seq,
		Raw:      truncate(strings.TrimSpace(raw), 4096),
		Reason:   reason,
		Detail:   err.Error(),
	}
}
