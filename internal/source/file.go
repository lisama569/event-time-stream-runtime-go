package source

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/lisama569/event-time-stream-runtime-go/internal/event"
)

// FileLineSource reads newline-delimited JSON records from a local file.
// Offset is a 0-based line index, so replay requires an unchanged file.
// The file's identity (size/mtime) is captured at construction and verified
// during Read: a changed file is reported as a classified rejection rather
// than silently replaying different data.
type FileLineSource struct {
	id   string
	path string
	info os.FileInfo
	opts event.ParseOptions
}

// NewFileLineSource opens the file eagerly to fail fast and capture identity.
func NewFileLineSource(id, path string) (*FileLineSource, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("source %s: stat %s: %w", id, path, err)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("source %s: %s is a directory", id, path)
	}
	return &FileLineSource{id: id, path: path, info: info}, nil
}

// WithParseOptions sets acceptable event-time bounds for this source.
func (s *FileLineSource) WithParseOptions(o event.ParseOptions) *FileLineSource { s.opts = o; return s }

func (s *FileLineSource) ID() string { return s.id }

func (s *FileLineSource) Read(ctx context.Context, fromOffset int64, out chan<- Envelope) {
	f, err := os.Open(s.path)
	if err != nil {
		s.reject(ctx, -1, "", fmt.Errorf("open: %w", err), out)
		return
	}
	defer f.Close()

	if info, statErr := f.Stat(); statErr == nil {
		if info.Size() != s.info.Size() || !info.ModTime().Equal(s.info.ModTime()) {
			s.reject(ctx, -1, "", errors.New("source file changed since startup; refusing replay"), out)
			return
		}
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	var lineNo int64
	for scanner.Scan() {
		line := scanner.Text()
		if lineNo < fromOffset {
			lineNo++
			continue
		}
		env := Envelope{Offset: lineNo}
		ev, perr := event.ParseLine(line, s.opts)
		if perr == event.ErrEmptyRecord {
			lineNo++
			continue
		}
		if perr != nil {
			r := event.Classify(s.id, lineNo, line, perr)
			env.Rejected = &r
		} else {
			ev.SourceID = s.id
			ev.Seq = lineNo
			env.Event = ev
		}
		select {
		case out <- env:
		case <-ctx.Done():
			return
		}
		lineNo++
	}
	if err := scanner.Err(); err != nil {
		s.reject(ctx, lineNo, "", fmt.Errorf("scan: %w", err), out)
		return
	}
	select {
	case out <- Envelope{EOF: true}:
	case <-ctx.Done():
	}
}

// reject emits a classified rejection (seq -1 marks a source-level failure),
// then the EOF marker so fan-in can finish.
func (s *FileLineSource) reject(ctx context.Context, seq int64, raw string, err error, out chan<- Envelope) {
	r := event.Rejected{SourceID: s.id, Seq: seq, Raw: raw, Reason: event.RejectMalformed, Detail: err.Error()}
	select {
	case out <- Envelope{Rejected: &r}:
	case <-ctx.Done():
		return
	}
	select {
	case out <- Envelope{EOF: true}:
	case <-ctx.Done():
	}
}
