package source

import (
	"context"
	"reflect"
	"sync"
)

// FanIn concurrently reads all sources, each into its own bounded buffer.
// Backpressure is per-source: when the merged consumer is slow, a source
// blocks only once its own buffer is full; faster sources keep ingesting up
// to their buffer bound and a single slow source never stalls the others
// beyond the bounded capacity. Memory use is therefore bounded by
// len(sources)*perSourceBuffer envelopes regardless of consumer speed.
type FanIn struct {
	sources  []Reader
	bufDepth int
}

// NewFanIn builds a FanIn. perSourceBuffer must be > 0 (default 16).
func NewFanIn(sources []Reader, perSourceBuffer int) *FanIn {
	if perSourceBuffer <= 0 {
		perSourceBuffer = 16
	}
	return &FanIn{sources: sources, bufDepth: perSourceBuffer}
}

// Start launches every reader at its offset and returns the merged output
// channel plus a wait function that returns after all readers and the merger
// have stopped. The merged channel is closed once every source has emitted
// its EOF marker or ctx is cancelled.
func (f *FanIn) Start(ctx context.Context, offsets map[string]int64) (<-chan Envelope, func()) {
	// Snapshot offsets: the caller is free to keep mutating its own map while
	// reader goroutines run (avoids sharing the same map across goroutines).
	startOffsets := make(map[string]int64, len(f.sources))
	for _, r := range f.sources {
		startOffsets[r.ID()] = offsets[r.ID()]
	}
	perSrc := make([]chan Envelope, len(f.sources))
	var wg sync.WaitGroup
	for i, r := range f.sources {
		ch := make(chan Envelope, f.bufDepth)
		perSrc[i] = ch
		wg.Add(1)
		go func(r Reader, ch chan Envelope, from int64) {
			defer wg.Done()
			r.Read(ctx, from, ch)
		}(r, ch, startOffsets[r.ID()])
	}

	merged := make(chan Envelope, f.bufDepth)
	var mergeWg sync.WaitGroup
	mergeWg.Add(1)
	go func() {
		defer mergeWg.Done()
		defer close(merged)

		cases := make([]reflect.SelectCase, len(perSrc))
		open := make([]bool, len(perSrc))
		for i, ch := range perSrc {
			cases[i] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ch)}
			open[i] = true
		}
		remaining := len(perSrc)
		ctxCase := reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}

		for remaining > 0 {
			active := make([]reflect.SelectCase, 0, remaining+1)
			mapping := make([]int, 0, remaining+1)
			active = append(active, ctxCase)
			mapping = append(mapping, -1)
			for i := range cases {
				if open[i] {
					active = append(active, cases[i])
					mapping = append(mapping, i)
				}
			}
			chosen, recv, recvOK := reflect.Select(active)
			if mapping[chosen] == -1 {
				return // ctx cancelled
			}
			idx := mapping[chosen]
			if !recvOK {
				// Reader exited without EOF (e.g. ctx cancel): retire it.
				open[idx] = false
				remaining--
				continue
			}
			env := recv.Interface().(Envelope)
			sid := f.sources[idx].ID()
			env.SourceID = sid
			if env.Rejected != nil && env.Rejected.SourceID == "" {
				env.Rejected.SourceID = sid
			}
			if env.EOF {
				open[idx] = false
				remaining--
			}
			select {
			case merged <- env:
			case <-ctx.Done():
				return
			}
		}
	}()

	wait := func() {
		wg.Wait()
		mergeWg.Wait()
	}
	return merged, wait
}
