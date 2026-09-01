package store

import (
	"testing"
	"time"
)

// The windowed search exists because the unwindowed one is a ~50s full scan on
// the live host (2.3M events); see SearchSessionsInWindow. These tests pin the
// window SEMANTICS — [since, until), zero = unbounded — and the boundary id
// resolution, since an off-by-one there silently widens or narrows every
// window and no caller would notice anything but wrong attribution.

func storeAt(t *testing.T, s *Store, session, data, createdAt string) {
	t.Helper()
	if _, err := s.StoreEvent(session, "user_message", []byte(data)); err != nil {
		t.Fatalf("StoreEvent: %v", err)
	}
	// Backdate the row just written: StoreEvent stamps CURRENT_TIMESTAMP, and
	// the window under test needs rows spread over days, not microseconds.
	if _, err := s.writer.Exec(
		`UPDATE events SET created_at = ? WHERE id = (SELECT MAX(id) FROM events)`, createdAt,
	); err != nil {
		t.Fatalf("backdate: %v", err)
	}
}

func windowFixture(t *testing.T) *Store {
	s := newTestStore(t)
	storeAt(t, s, "s_old", `{"text":"needle early"}`, "2026-08-01 10:00:00")
	storeAt(t, s, "s_mid", `{"text":"needle midway"}`, "2026-08-15 10:00:00")
	storeAt(t, s, "s_new", `{"text":"needle late"}`, "2026-08-30 10:00:00")
	return s
}

func hitIDs(hits []SearchHit) []string {
	ids := make([]string, len(hits))
	for i, h := range hits {
		ids[i] = h.SessionID
	}
	return ids
}

func TestWindowedSearchBoundsBothSides(t *testing.T) {
	s := windowFixture(t)
	hits, err := s.SearchSessionsInWindow("needle", 10,
		time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("windowed search: %v", err)
	}
	if got := hitIDs(hits); len(got) != 1 || got[0] != "s_mid" {
		t.Fatalf("want [s_mid], got %v", got)
	}
}

func TestZeroTimesLeaveTheSearchUnbounded(t *testing.T) {
	s := windowFixture(t)
	hits, err := s.SearchSessionsInWindow("needle", 10, time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("unbounded search: %v", err)
	}
	if len(hits) != 3 {
		t.Fatalf("want all 3 sessions, got %v", hitIDs(hits))
	}
	// And the one-argument form still answers identically.
	same, err := s.SearchSessions("needle", 10)
	if err != nil || len(same) != 3 {
		t.Fatalf("SearchSessions changed behaviour: %v %v", hitIDs(same), err)
	}
}

func TestSinceExactlyOnAnEventIncludesIt(t *testing.T) {
	// [since, until) — the left bound is inclusive, and an event stamped at
	// the boundary second must land inside, not fall out through a > vs >=.
	s := windowFixture(t)
	hits, err := s.SearchSessionsInWindow("needle", 10,
		time.Date(2026, 8, 15, 10, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 16, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("windowed search: %v", err)
	}
	if got := hitIDs(hits); len(got) != 1 || got[0] != "s_mid" {
		t.Fatalf("want [s_mid] at the inclusive boundary, got %v", got)
	}
}

func TestWindowEntirelyPastTheTableIsEmptyNotUnbounded(t *testing.T) {
	// A since later than every event must answer nothing — falling back to an
	// unbounded scan here would silently hand back the whole corpus.
	s := windowFixture(t)
	hits, err := s.SearchSessionsInWindow("needle", 10,
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{})
	if err != nil {
		t.Fatalf("future window: %v", err)
	}
	if len(hits) != 0 {
		t.Fatalf("want no hits for a window past the table, got %v", hitIDs(hits))
	}
}

func TestUntilPastTheTableLeavesTheRightEdgeOpen(t *testing.T) {
	s := windowFixture(t)
	hits, err := s.SearchSessionsInWindow("needle", 10,
		time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("open right edge: %v", err)
	}
	if got := hitIDs(hits); len(got) != 1 || got[0] != "s_new" {
		t.Fatalf("want [s_new], got %v", got)
	}
}
