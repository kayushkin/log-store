package store

import (
	"encoding/json"
	"fmt"
	"testing"
)

// storeEvent inserts a minimal event of the given type and returns its row id.
func storeEvent(t *testing.T, s *Store, sessionID, typ, text string) int64 {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"type":              typ,
		"bridge_session_id": sessionID,
		"result":            map[string]any{"text": text},
	})
	id, err := s.StoreEvent(sessionID, typ, body)
	if err != nil {
		t.Fatalf("StoreEvent: %v", err)
	}
	return id
}

// EventPage default returns the last N turns (user_message delimited), bounded.
func TestEventPage_LastNTurns(t *testing.T) {
	s := newTestStore(t)
	sess := "sess1"
	// 5 turns, each: user_message + result.
	var ids []int64
	for i := 0; i < 5; i++ {
		ids = append(ids, storeEvent(t, s, sess, "user_message", fmt.Sprintf("q%d", i)))
		ids = append(ids, storeEvent(t, s, sess, "result", fmt.Sprintf("a%d", i)))
	}
	// Last 2 turns → should start at the 4th user_message (index 6 in ids: turn 3).
	rows, more, err := s.EventPage(sess, 2, 0)
	if err != nil {
		t.Fatalf("EventPage: %v", err)
	}
	if !more {
		t.Errorf("expected more=true (3 older turns remain)")
	}
	// 2 turns * 2 events = 4 events.
	if len(rows) != 4 {
		t.Fatalf("expected 4 events for last 2 turns, got %d", len(rows))
	}
	// First returned event must be a user_message (turn boundary).
	if rows[0].Type != "user_message" {
		t.Errorf("window must start on a turn boundary, got %q", rows[0].Type)
	}
}

// A before cursor pages older turns and eventually reports more=false.
func TestEventPage_BeforeCursor(t *testing.T) {
	s := newTestStore(t)
	sess := "sess2"
	for i := 0; i < 4; i++ {
		storeEvent(t, s, sess, "user_message", fmt.Sprintf("q%d", i))
		storeEvent(t, s, sess, "result", fmt.Sprintf("a%d", i))
	}
	// Page the last 2 turns.
	rows, more, err := s.EventPage(sess, 2, 0)
	if err != nil {
		t.Fatalf("EventPage: %v", err)
	}
	if !more {
		t.Fatalf("expected older turns remaining")
	}
	first := rows[0].ID
	// Page 2 turns older than the first event of page 1.
	older, more2, err := s.EventPage(sess, 2, first)
	if err != nil {
		t.Fatalf("EventPage before: %v", err)
	}
	if len(older) == 0 {
		t.Fatalf("expected an older page")
	}
	for _, r := range older {
		if r.ID >= first {
			t.Errorf("before cursor leaked a newer event: %d >= %d", r.ID, first)
		}
	}
	if more2 {
		t.Errorf("no turns should remain before the first page's window")
	}
}

// A session with no user_message boundaries is still bounded (never unbounded)
// and returns events without panicking.
func TestEventPage_NoTurnBoundaries(t *testing.T) {
	s := newTestStore(t)
	sess := "sess3"
	for i := 0; i < 10; i++ {
		storeEvent(t, s, sess, "system", "noise")
	}
	rows, _, err := s.EventPage(sess, 30, 0)
	if err != nil {
		t.Fatalf("EventPage: %v", err)
	}
	if len(rows) != 10 {
		t.Errorf("expected all 10 bounded events, got %d", len(rows))
	}
}

func TestValidators(t *testing.T) {
	s := newTestStore(t)
	a, b := "sa", "sb"
	storeEvent(t, s, a, "user_message", "hi")
	storeEvent(t, s, a, "result", "yo")
	last := storeEvent(t, s, a, "result", "bye")
	// b has no events.
	vs, err := s.Validators([]string{a, b})
	if err != nil {
		t.Fatalf("Validators: %v", err)
	}
	va := vs[a]
	if va.MaxEventID != last {
		t.Errorf("maxEventId = %d, want %d", va.MaxEventID, last)
	}
	if va.EventCount != 3 {
		t.Errorf("eventCount = %d, want 3", va.EventCount)
	}
	if va.UpdatedAt.IsZero() {
		t.Errorf("updatedAt should be set")
	}
	vb := vs[b]
	if vb.MaxEventID != 0 || vb.EventCount != 0 {
		t.Errorf("empty session validator should be zero-value, got %+v", vb)
	}
}

// A prompt is stored twice — the bridge's user_message and the OTel echo of it,
// same turn_id — and the pair must never be split by a page window. Before this,
// each row was a boundary, so limit=1 opened the page ON the echo and left the
// bridge's copy outside: the echo went out unpaired and primary, and the client
// drew the prompt twice (once before the answer, once after).
func TestEventPage_TurnBoundaryIsOnePerTurn_EchoNeverSplit(t *testing.T) {
	s := newTestStore(t)
	sess := "s-echo"
	var harnessIDs []int64
	for i := 0; i < 3; i++ {
		turn := fmt.Sprintf("turn_%d", i)
		h := storeEventRaw(t, s, sess, "user_message", fmt.Sprintf(`{"type":"user_message","turn_id":%q,"result":{"text":"q%d"}}`, turn, i))
		harnessIDs = append(harnessIDs, h)
		storeEventRaw(t, s, sess, "user_message", fmt.Sprintf(`{"type":"user_message","turn_id":%q,"result":{"text":"q%d"},"extensions":{"source":"otel"}}`, turn, i))
		storeEventRaw(t, s, sess, "result", fmt.Sprintf(`{"type":"result","turn_id":%q,"result":{"text":"a%d"}}`, turn, i))
	}
	rows, more, err := s.EventPage(sess, 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !more {
		t.Fatalf("expected more=true with two older turns")
	}
	if len(rows) != 3 {
		t.Fatalf("last turn is 3 rows (prompt, echo, result); got %d", len(rows))
	}
	if rows[0].ID != harnessIDs[2] {
		t.Fatalf("page must open on the bridge's copy of the prompt (id %d), not the echo; opened on %d", harnessIDs[2], rows[0].ID)
	}
	// And limit=2 reaches back exactly two TURNS, not two rows.
	rows, _, err = s.EventPage(sess, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rows[0].ID != harnessIDs[1] || len(rows) != 6 {
		t.Fatalf("limit=2 must start at turn 1's bridge copy (id %d) and hold 6 rows; got start %d, %d rows", harnessIDs[1], rows[0].ID, len(rows))
	}
}

// storeEventRaw stores an event with a caller-shaped body, for rows whose turn_id
// or extensions matter to the test.
func storeEventRaw(t *testing.T, s *Store, sessionID, typ, body string) int64 {
	t.Helper()
	id, err := s.StoreEvent(sessionID, typ, []byte(body))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// The event cap must not cut a turn in half — and never between a prompt and its
// OTel echo. With a cap that lands on turn 1's echo, the page snaps forward to
// turn 2 and holds whole turns only.
func TestEventPage_EventFloorSnapsToATurnBoundary(t *testing.T) {
	s := newTestStore(t)
	sess := "s-floor"
	var starts []int64
	for i := 0; i < 3; i++ {
		turn := fmt.Sprintf("turn_%d", i)
		starts = append(starts, storeEventRaw(t, s, sess, "user_message", fmt.Sprintf(`{"type":"user_message","turn_id":%q,"result":{"text":"q%d"}}`, turn, i)))
		storeEventRaw(t, s, sess, "user_message", fmt.Sprintf(`{"type":"user_message","turn_id":%q,"result":{"text":"q%d"},"extensions":{"source":"otel"}}`, turn, i))
		storeEventRaw(t, s, sess, "block", fmt.Sprintf(`{"type":"block","turn_id":%q}`, turn))
		storeEventRaw(t, s, sess, "result", fmt.Sprintf(`{"type":"result","turn_id":%q,"result":{"text":"a%d"}}`, turn, i))
	}
	// 12 rows; a cap of 7 lands on turn 1's echo. Ask for all 3 turns so only the
	// cap constrains.
	rows, more, err := s.eventPage(sess, 3, 0, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !more {
		t.Fatalf("turns were left out, so more must be true")
	}
	if len(rows) != 4 || rows[0].ID != starts[2] {
		t.Fatalf("expected the page to snap to turn 2's bridge prompt (id %d) with 4 rows; got start %d, %d rows", starts[2], rows[0].ID, len(rows))
	}
	// A cap that lands exactly on a boundary is already whole-turn and unchanged.
	rows, _, err = s.eventPage(sess, 3, 0, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 8 || rows[0].ID != starts[1] {
		t.Fatalf("a cap on turn 1's boundary keeps turns 1-2 (8 rows from id %d); got start %d, %d rows", starts[1], rows[0].ID, len(rows))
	}
}
