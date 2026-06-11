package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// newTestStore opens a Store backed by a throwaway SQLite file under the test's
// temp dir. The dir is created by New (it MkdirAll's the parent), so we point at
// a nested path to exercise that too. Cleanup closes the handle.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "nested", "log-store.db")
	s, err := New(dbPath)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return s
}

// resultEvent builds a result-type event body matching the JSON shape that
// updateSessionProjection parses (result.usage / result.cost / result.model /
// result.duration_ms).
func resultEvent(inTok, outTok int64, costUSD float64, durMS int64, model string) []byte {
	body := map[string]any{
		"result": map[string]any{
			"usage": map[string]any{
				"input_tokens":  inTok,
				"output_tokens": outTok,
			},
			"cost":        map[string]any{"total_usd": costUSD},
			"duration_ms": durMS,
			"model":       model,
		},
	}
	b, _ := json.Marshal(body)
	return b
}

func TestNewCreatesSchemaAndClose(t *testing.T) {
	s := newTestStore(t)

	// Both tables must exist after New() runs migrate().
	for _, tbl := range []string{"events", "sessions"} {
		var name string
		err := s.db.QueryRow(
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl,
		).Scan(&name)
		if err != nil {
			t.Fatalf("expected table %q to exist: %v", tbl, err)
		}
		if name != tbl {
			t.Fatalf("got table %q, want %q", name, tbl)
		}
	}

	// Pool is pinned to a single connection (see store.go rationale).
	if got := s.db.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("MaxOpenConnections = %d, want 1", got)
	}
}

func TestNewInvalidPath(t *testing.T) {
	// A path whose parent can't be created (a file standing in for a dir)
	// should surface an error rather than silently succeeding.
	f := filepath.Join(t.TempDir(), "afile")
	if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	if _, err := New(filepath.Join(f, "child", "db.sqlite")); err == nil {
		t.Fatal("expected error opening db under a non-directory parent, got nil")
	}
}

func TestStoreEventPersistsAndAssignsIDs(t *testing.T) {
	s := newTestStore(t)

	id1, err := s.StoreEvent("sess-a", "user_message", []byte(`{"text":"hi"}`))
	if err != nil {
		t.Fatalf("StoreEvent 1: %v", err)
	}
	id2, err := s.StoreEvent("sess-a", "assistant", []byte(`{"text":"yo"}`))
	if err != nil {
		t.Fatalf("StoreEvent 2: %v", err)
	}
	if id1 <= 0 || id2 <= id1 {
		t.Fatalf("expected ascending positive ids, got %d then %d", id1, id2)
	}

	events, err := s.ListEvents("sess-a", nil)
	if err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ListEvents returned %d events, want 2", len(events))
	}

	// Each returned event must carry its row id as an injected "event_id" field.
	var first struct {
		EventID int64  `json:"event_id"`
		Text    string `json:"text"`
	}
	if err := json.Unmarshal(events[0], &first); err != nil {
		t.Fatalf("unmarshal event 0: %v", err)
	}
	if first.EventID != id1 {
		t.Errorf("event_id = %d, want %d", first.EventID, id1)
	}
	if first.Text != "hi" {
		t.Errorf("text = %q, want %q (original fields must survive injection)", first.Text, "hi")
	}
}

func TestListEventsTypeFilterAndIsolation(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.StoreEvent("sess-a", "user_message", []byte(`{"n":1}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StoreEvent("sess-a", "result", resultEvent(10, 5, 0.01, 100, "opus")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StoreEvent("sess-b", "user_message", []byte(`{"n":2}`)); err != nil {
		t.Fatal(err)
	}

	// Type filter: only user_message events of sess-a.
	got, err := s.ListEvents("sess-a", []string{"user_message"})
	if err != nil {
		t.Fatalf("ListEvents filtered: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("filtered ListEvents returned %d, want 1", len(got))
	}

	// Session isolation: sess-b must not leak into sess-a reads.
	all, err := s.ListEvents("sess-a", nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("sess-a has %d events, want 2 (no cross-session leak)", len(all))
	}
}

func TestListEventsSinceID(t *testing.T) {
	s := newTestStore(t)
	id1, _ := s.StoreEvent("s", "user_message", []byte(`{"n":1}`))
	id2, _ := s.StoreEvent("s", "assistant", []byte(`{"n":2}`))
	id3, _ := s.StoreEvent("s", "result", resultEvent(1, 1, 0, 0, "m"))

	got, err := s.ListEventsSinceID("s", int(id1), nil)
	if err != nil {
		t.Fatalf("ListEventsSinceID: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("since id1 returned %d events, want 2", len(got))
	}
	// First returned should be id2.
	var ev struct {
		EventID int64 `json:"event_id"`
	}
	if err := json.Unmarshal(got[0], &ev); err != nil {
		t.Fatal(err)
	}
	if ev.EventID != id2 {
		t.Errorf("first event_id = %d, want %d", ev.EventID, id2)
	}

	// Nothing after the last id.
	tail, err := s.ListEventsSinceID("s", int(id3), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(tail) != 0 {
		t.Errorf("since last id returned %d events, want 0", len(tail))
	}
}

func TestSessionProjectionFromEvents(t *testing.T) {
	s := newTestStore(t)

	// Two user turns and two result events for one session.
	if _, err := s.StoreEvent("sess-p", "user_message", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StoreEvent("sess-p", "result", resultEvent(100, 40, 0.02, 1500, "claude-opus")); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StoreEvent("sess-p", "user_message", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.StoreEvent("sess-p", "result", resultEvent(50, 10, 0.03, 500, "claude-sonnet")); err != nil {
		t.Fatal(err)
	}

	aggs, err := s.ListSessionAggregates()
	if err != nil {
		t.Fatalf("ListSessionAggregates: %v", err)
	}
	if len(aggs) != 1 {
		t.Fatalf("got %d aggregate rows, want 1", len(aggs))
	}
	a := aggs[0]
	if a.SessionID != "sess-p" {
		t.Errorf("SessionID = %q, want sess-p", a.SessionID)
	}
	if a.Turns != 2 {
		t.Errorf("Turns = %d, want 2 (one per user_message)", a.Turns)
	}
	if a.InputTokens != 150 {
		t.Errorf("InputTokens = %d, want 150", a.InputTokens)
	}
	if a.OutputTokens != 50 {
		t.Errorf("OutputTokens = %d, want 50", a.OutputTokens)
	}
	if a.DurationMS != 2000 {
		t.Errorf("DurationMS = %d, want 2000", a.DurationMS)
	}
	// cost_usd is float; allow tiny epsilon.
	if a.CostUSD < 0.0499 || a.CostUSD > 0.0501 {
		t.Errorf("CostUSD = %v, want ~0.05", a.CostUSD)
	}
	// Latest result's model wins.
	if a.Model != "claude-sonnet" {
		t.Errorf("Model = %q, want claude-sonnet (latest result)", a.Model)
	}
}

func TestListSessionAggregatesOmitsUsageless(t *testing.T) {
	s := newTestStore(t)
	// A session with only a user_message (no result → no usage) must not appear.
	if _, err := s.StoreEvent("no-usage", "user_message", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	aggs, err := s.ListSessionAggregates()
	if err != nil {
		t.Fatal(err)
	}
	if len(aggs) != 0 {
		t.Fatalf("got %d aggregate rows, want 0 (no result events)", len(aggs))
	}
}

func TestTurnState(t *testing.T) {
	s := newTestStore(t)

	// No events yet → zero, not in flight.
	ts, err := s.TurnState("t")
	if err != nil {
		t.Fatalf("TurnState empty: %v", err)
	}
	if ts.InFlight || ts.LastUserMessageEventID != 0 || ts.LastTerminatorEventID != 0 {
		t.Fatalf("empty TurnState = %+v, want all zero / not in flight", ts)
	}

	// user_message with no terminator → in flight.
	umID, _ := s.StoreEvent("t", "user_message", []byte(`{}`))
	ts, err = s.TurnState("t")
	if err != nil {
		t.Fatal(err)
	}
	if !ts.InFlight {
		t.Errorf("expected in-flight after user_message")
	}
	if ts.LastUserMessageEventID != umID {
		t.Errorf("LastUserMessageEventID = %d, want %d", ts.LastUserMessageEventID, umID)
	}

	// result terminates the turn → no longer in flight.
	termID, _ := s.StoreEvent("t", "result", resultEvent(1, 1, 0, 0, "m"))
	ts, err = s.TurnState("t")
	if err != nil {
		t.Fatal(err)
	}
	if ts.InFlight {
		t.Errorf("expected not in-flight after result terminator")
	}
	if ts.LastTerminatorEventID != termID {
		t.Errorf("LastTerminatorEventID = %d, want %d", ts.LastTerminatorEventID, termID)
	}

	// error also terminates (and a later user_message re-opens flight).
	s.StoreEvent("t", "user_message", []byte(`{}`))
	ts, _ = s.TurnState("t")
	if !ts.InFlight {
		t.Errorf("expected in-flight after second user_message")
	}
	s.StoreEvent("t", "error", []byte(`{}`))
	ts, _ = s.TurnState("t")
	if ts.InFlight {
		t.Errorf("expected not in-flight after error terminator")
	}
}

func TestSessionIDs(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("b", "user_message", []byte(`{}`))
	s.StoreEvent("a", "user_message", []byte(`{}`))
	s.StoreEvent("a", "result", resultEvent(1, 1, 0, 0, "m"))

	ids, err := s.SessionIDs()
	if err != nil {
		t.Fatalf("SessionIDs: %v", err)
	}
	// Distinct + ordered ascending.
	want := []string{"a", "b"}
	if len(ids) != len(want) {
		t.Fatalf("SessionIDs = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Errorf("SessionIDs[%d] = %q, want %q", i, ids[i], want[i])
		}
	}
}

func TestSearchSessions(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("s1", "user_message", []byte(`{"text":"needle here"}`))
	s.StoreEvent("s1", "assistant", []byte(`{"text":"needle again"}`))
	s.StoreEvent("s2", "user_message", []byte(`{"text":"unrelated"}`))

	hits, err := s.SearchSessions("needle", 10)
	if err != nil {
		t.Fatalf("SearchSessions: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1", len(hits))
	}
	if hits[0].SessionID != "s1" {
		t.Errorf("hit session = %q, want s1", hits[0].SessionID)
	}
	if hits[0].MatchCount != 2 {
		t.Errorf("match count = %d, want 2", hits[0].MatchCount)
	}

	// No match → empty.
	none, err := s.SearchSessions("haystack", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(none) != 0 {
		t.Errorf("got %d hits for absent term, want 0", len(none))
	}
}

func TestProjectionSurvivesReopenWithBackfill(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "ls.db")

	s1, err := New(dbPath)
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	s1.StoreEvent("sess", "user_message", []byte(`{}`))
	s1.StoreEvent("sess", "result", resultEvent(7, 3, 0.5, 99, "mdl"))

	// Drop the projection rows so reopening must rebuild them from events.
	if _, err := s1.db.Exec(`DELETE FROM sessions`); err != nil {
		t.Fatalf("delete sessions: %v", err)
	}
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	s2, err := New(dbPath)
	if err != nil {
		t.Fatalf("New 2 (reopen): %v", err)
	}
	defer s2.Close()

	aggs, err := s2.ListSessionAggregates()
	if err != nil {
		t.Fatalf("ListSessionAggregates after reopen: %v", err)
	}
	if len(aggs) != 1 {
		t.Fatalf("backfill produced %d rows, want 1", len(aggs))
	}
	a := aggs[0]
	if a.Turns != 1 || a.InputTokens != 7 || a.OutputTokens != 3 || a.Model != "mdl" {
		t.Errorf("backfilled aggregate = %+v, want turns=1 in=7 out=3 model=mdl", a)
	}
}

func TestPlaceholders(t *testing.T) {
	cases := map[int]string{
		0: "",
		1: "?",
		3: "?,?,?",
	}
	for n, want := range cases {
		if got := placeholders(n); got != want {
			t.Errorf("placeholders(%d) = %q, want %q", n, got, want)
		}
	}
	// Negative is treated as zero.
	if got := placeholders(-1); got != "" {
		t.Errorf("placeholders(-1) = %q, want empty", got)
	}
}

func TestInjectEventID(t *testing.T) {
	// Normal object.
	out := injectEventID([]byte(`{"a":1}`), 42)
	var m map[string]any
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal injected: %v", err)
	}
	if m["event_id"] != float64(42) {
		t.Errorf("event_id = %v, want 42", m["event_id"])
	}
	if m["a"] != float64(1) {
		t.Errorf("original field a = %v, want 1", m["a"])
	}

	// Empty object → just event_id, valid JSON.
	out = injectEventID([]byte(`{}`), 7)
	m = nil
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal injected empty: %v (%s)", err, out)
	}
	if m["event_id"] != float64(7) || len(m) != 1 {
		t.Errorf("empty-object injection = %v, want {event_id:7}", m)
	}

	// Leading whitespace is tolerated.
	out = injectEventID([]byte("  \n{\"a\":1}"), 5)
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("unmarshal injected w/ whitespace: %v", err)
	}

	// Non-object input is returned unchanged.
	in := []byte(`[1,2,3]`)
	if got := injectEventID(in, 9); string(got) != string(in) {
		t.Errorf("non-object injection = %q, want unchanged %q", got, in)
	}
}
