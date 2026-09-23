package engine

import (
	"fmt"

	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
	"github.com/lisama569/event-time-stream-runtime-go/internal/source"
)

// ParseOptions converts engine time bounds into event.ParseOptions.
func (c Config) ParseOptions() event.ParseOptions {
	o := event.ParseOptions{}
	if !c.MinEventTime.IsZero() {
		o.MinTime = c.MinEventTime.UnixMilli()
	}
	if !c.MaxEventTime.IsZero() {
		o.MaxTime = c.MaxEventTime.UnixMilli()
	}
	return o
}

// LineSources builds in-memory sources from JSON lines, applying the engine's
// timestamp bounds so out-of-span input is classified at ingestion.
func (c Config) LineSources(streams map[string][]string) []source.Reader {
	rs := make([]source.Reader, 0, len(streams))
	for id, lines := range streams {
		rs = append(rs, source.NewSliceSource(id, lines).WithParseOptions(c.ParseOptions()))
	}
	return rs
}

// EventSources builds in-memory sources from prebuilt valid events.
func EventSources(streams map[string][]event.Event) []source.Reader {
	rs := make([]source.Reader, 0, len(streams))
	for id, evs := range streams {
		rs = append(rs, source.NewSliceSourceEvents(id, evs))
	}
	return rs
}

// FileSources opens local newline-delimited JSON files as sources.
func (c Config) FileSources(paths map[string]string) ([]source.Reader, error) {
	rs := make([]source.Reader, 0, len(paths))
	for id, p := range paths {
		s, err := source.NewFileLineSource(id, p)
		if err != nil {
			return nil, fmt.Errorf("open source %s: %w", id, err)
		}
		s.WithParseOptions(c.ParseOptions())
		rs = append(rs, s)
	}
	return rs, nil
}
