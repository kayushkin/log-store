package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The raw page and the reading page hold the same turns under the same names:
// both choose them from the turn index.
func TestRawPage_ChoosesTheSameTurnsAsTheReadingPage(t *testing.T) {
	srv, s := newTestServer(t)
	for i := 0; i < 5; i++ {
		turn := fmt.Sprintf("t%d", i)
		ingestJSON(t, s, "both", "user_message", map[string]any{"turn_id": turn, "result": map[string]any{"text": "q" + turn}})
		ingestJSON(t, s, "both", "system", map[string]any{}) // no turn_id: carried into turn
		ingestJSON(t, s, "both", "result", map[string]any{"turn_id": turn, "result": map[string]any{"text": "a" + turn}})
	}
	reading, err := srv.storedTail("both", 2, 0, payloadFull)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := srv.materializeTail("both", 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(reading.Turns) != 2 || len(raw.Turns) != 2 {
		t.Fatalf("turns: reading %d raw %d", len(reading.Turns), len(raw.Turns))
	}
	for i := range raw.Turns {
		if raw.Turns[i].ID != reading.Turns[i].ID {
			t.Errorf("turn %d: raw %q reading %q", i, raw.Turns[i].ID, reading.Turns[i].ID)
		}
	}
	if !raw.More || !reading.More {
		t.Errorf("more: raw %v reading %v", raw.More, reading.More)
	}
	// Every event of those turns is on the raw page, duplicates and bookkeeping included.
	if len(raw.Entries) != 6 {
		t.Errorf("raw entries %d, want 6", len(raw.Entries))
	}
}

// Whole older turns are left out while the page is over the event cap.
func TestRawPage_LeavesOutOlderTurnsOverTheEventCap(t *testing.T) {
	srv, s := newTestServer(t)
	ingestJSON(t, s, "cap", "user_message", map[string]any{"turn_id": "old", "result": map[string]any{"text": "q"}})
	for i := 0; i < rawEventCap-2; i++ {
		ingestJSON(t, s, "cap", "stream", map[string]any{"turn_id": "old"})
	}
	ingestJSON(t, s, "cap", "user_message", map[string]any{"turn_id": "new", "result": map[string]any{"text": "q2"}})
	for i := 0; i < 5; i++ {
		ingestJSON(t, s, "cap", "stream", map[string]any{"turn_id": "new"})
	}

	raw, err := srv.materializeTail("cap", 30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.Turns) != 1 || raw.Turns[0].ID != "new" || !raw.More {
		t.Fatalf("turns %+v more %v; want only the newest turn and more", turnNames(raw.Turns), raw.More)
	}
}

// A newest turn that alone is over the cap is served as its newest events.
func TestRawPage_ServesTheTailOfAnOversizedTurn(t *testing.T) {
	srv, s := newTestServer(t)
	ingestJSON(t, s, "huge", "user_message", map[string]any{"turn_id": "t", "result": map[string]any{"text": "q"}})
	var last int64
	for i := 0; i < rawEventCap+10; i++ {
		last = ingestJSON(t, s, "huge", "stream", map[string]any{"turn_id": "t"})
	}
	raw, err := srv.materializeTail("huge", 30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw.Entries) != rawEventCap || !raw.More {
		t.Fatalf("entries %d more %v; want %d and more", len(raw.Entries), raw.More, rawEventCap)
	}
	if _, ok := raw.Entries[fmt.Sprintf("e_%d", last)]; !ok {
		t.Fatal("the newest event is not on the page")
	}
}

func TestMessages_RefusesARequestWithNoBound(t *testing.T) {
	srv, s := newTestServer(t)
	ingestJSON(t, s, "x", "user_message", map[string]any{"turn_id": "t", "result": map[string]any{"text": "q"}})
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/x/messages", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
}

func turnNames(turns []Turn) []string {
	var out []string
	for _, t := range turns {
		out = append(out, t.ID)
	}
	return out
}
