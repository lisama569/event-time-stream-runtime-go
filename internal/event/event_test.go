package event

import (
	"errors"
	"testing"
)

func TestParseLineOK(t *testing.T) {
	ev, err := ParseLine(`{"id":"e1","key":"k","ts":1000,"value":5}`, ParseOptions{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ev.ID != "e1" || ev.Key != "k" || ev.Timestamp != 1000 || ev.Value != 5 {
		t.Fatalf("bad event: %+v", ev)
	}
	// RFC3339 timestamp
	ev2, err := ParseLine(`{"id":"e2","key":"k","ts":"2026-09-23T10:00:00.500Z","value":1}`, ParseOptions{})
	if err != nil {
		t.Fatalf("rfc3339: %v", err)
	}
	if ev2.Timestamp != 1790157600500 {
		t.Fatalf("rfc3339 ms = %d", ev2.Timestamp)
	}
}

func TestParseLineEmpty(t *testing.T) {
	if _, err := ParseLine("   \n", ParseOptions{}); !errors.Is(err, ErrEmptyRecord) {
		t.Fatalf("want ErrEmptyRecord, got %v", err)
	}
}

func TestClassify(t *testing.T) {
	cases := []struct {
		line string
		want RejectReason
	}{
		{`{"id":"x","key":"k","ts":1,"value":1}`, RejectOutOfSpan},     // below min
		{`{"id":"x","key":"k","ts":99999,"value":1}`, RejectOutOfSpan}, // above max
		{`{"id":"x","key":"k","ts":0,"value":1}`, RejectBadTime},
		{`{"id":"x","key":"k","value":1}`, RejectBadTime},
		{`{"id":"x","key":"k","ts":"nope","value":1}`, RejectBadTime},
		{`{"key":"k","ts":1,"value":1}`, RejectMalformed},
		{`{"id":"x","ts":1,"value":1}`, RejectMalformed},
		{`{bad json`, RejectMalformed},
	}
	opts := ParseOptions{MinTime: 100, MaxTime: 1000}
	for i, c := range cases {
		_, err := ParseLine(c.line, opts)
		if err == nil {
			t.Fatalf("case %d expected error", i)
		}
		r := Classify("src", int64(i), c.line, err)
		if r.Reason != c.want {
			t.Fatalf("case %d line=%q got=%s want=%s detail=%s", i, c.line, r.Reason, c.want, r.Detail)
		}
	}
}
