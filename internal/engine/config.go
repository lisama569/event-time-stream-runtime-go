// Package engine wires ingestion, watermark, windows, checkpointing and
// output into one local, deterministic processing loop.
package engine

import "time"

// Config controls all engine knobs.
type Config struct {
	// EngineID is embedded in checkpoints; a checkpoint from a different
	// EngineID is rejected on load.
	EngineID string
	// WindowSize is the tumbling event-time window length (required > 0).
	WindowSize time.Duration
	// AllowedLateness is how long past a window's end an out-of-order event
	// may still enter it (event time). After end+AllowedLateness the result is
	// finalized and purged; later events are dropped (observable counters).
	AllowedLateness time.Duration
	// OutOfOrderBound is how far the per-source watermark lags the highest
	// observed event time.
	OutOfOrderBound time.Duration
	// MinEventTime / MaxEventTime bound acceptable event times; zero = unbounded.
	MinEventTime time.Time
	MaxEventTime time.Time
	// PerSourceBuffer bounds in-flight envelopes per source (bounded memory).
	PerSourceBuffer int
	// CheckpointInterval is the wall-clock cadence of consistent checkpoints;
	// a checkpoint is also taken when all sources reach EOF.
	CheckpointInterval time.Duration
	// IdleTimeout marks a source idle for watermark purposes after this long
	// without data, so a quiet source cannot hold back the watermark forever.
	IdleTimeout time.Duration
	// MaxEvents, when > 0, stops the run after N accepted/rejected envelopes
	// without a final checkpoint (used for crash-recovery tests/demo).
	MaxEvents int64
}

// DefaultConfig returns sane local defaults (10s windows, 5s lateness, etc.).
func DefaultConfig() Config {
	return Config{
		EngineID:           "local-engine-1",
		WindowSize:         10 * time.Second,
		AllowedLateness:    5 * time.Second,
		OutOfOrderBound:    2 * time.Second,
		PerSourceBuffer:    32,
		CheckpointInterval: 500 * time.Millisecond,
		IdleTimeout:        30 * time.Second,
	}
}
