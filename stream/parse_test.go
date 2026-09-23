package stream

import (
	"testing"
	"time"
)

func TestParseRecord(t *testing.T) {
	ts := func(s string) time.Time {
		tm, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return tm
	}

	t.Run("csv valid", func(t *testing.T) {
		ev, ce := ParseRecord("e1,a,2026-01-01T00:00:01Z,1.5", "s", 0)
		if ce != nil || ev == nil {
			t.Fatalf("expected event, got ce=%v", ce)
		}
		if ev.ID != "e1" || ev.Key != "a" || ev.Value != 1.5 ||
			!ev.Timestamp.Equal(ts("2026-01-01T00:00:01Z")) {
			t.Fatalf("unexpected event: %+v", ev)
		}
	})

	t.Run("json valid", func(t *testing.T) {
		ev, ce := ParseRecord(`{"id":"e2","key":"b","ts":"2026-01-01T00:00:02Z","v":2.5}`, "s", 1)
		if ce != nil || ev == nil {
			t.Fatalf("expected event, got ce=%v", ce)
		}
		if ev.ID != "e2" || ev.Value != 2.5 {
			t.Fatalf("unexpected: %+v", ev)
		}
	})

	t.Run("blank and comment are skipped", func(t *testing.T) {
		for _, raw := range []string{"", "   ", "# a comment"} {
			if ev, ce := ParseRecord(raw, "s", 0); ev != nil || ce != nil {
				t.Fatalf("blank/comment must be nil,nil; got %v %v", ev, ce)
			}
		}
	})

	kindCases := []struct {
		name string
		raw  string
		want ErrorKind
	}{
		{"csv wrong fields", "a,b,c", KindMalformed},
		{"json broken", `{"id":"x"`, KindMalformed},
		{"json missing key", `{"id":"x","ts":"2026-01-01T00:00:00Z","v":1}`, KindMalformed},
		{"csv value nan", "e,a,2026-01-01T00:00:00Z,notanum", KindMalformed},
		{"bad timestamp csv", "e,a,not-a-time,1", KindInvalidTimestamp},
		{"missing ts json", `{"id":"e","key":"a","v":1}`, KindInvalidTimestamp},
		{"garbage ts json", `{"id":"e","key":"a","ts":"nope","v":1}`, KindInvalidTimestamp},
	}
	for _, tc := range kindCases {
		t.Run(tc.name, func(t *testing.T) {
			_, ce := ParseRecord(tc.raw, "s", 7)
			if ce == nil {
				t.Fatalf("expected %s for %q", tc.want, tc.raw)
			}
			if ce.Kind != tc.want {
				t.Fatalf("kind: want %s got %s (eventTime=%s watermark=%s reason=%s)",
					tc.want, ce.Kind, formatTime(ce.EventTime), formatTime(ce.Watermark), ce.Reason)
			}
			if ce.Offset != 7 || ce.Source != "s" {
				t.Fatalf("provenance lost: %+v", ce)
			}
		})
	}

	t.Run("timezone-naive timestamp is UTC", func(t *testing.T) {
		ev, ce := ParseRecord("e,a,2026-01-01 00:00:01,1.5", "s", 0)
		if ce != nil {
			t.Fatal(ce)
		}
		if ev.Timestamp.Location() != time.UTC {
			t.Fatalf("expected UTC, got %v", ev.Timestamp.Location())
		}
	})
}
