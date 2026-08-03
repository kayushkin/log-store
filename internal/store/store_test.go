package store

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
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
		err := s.reader.QueryRow(
			`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl,
		).Scan(&name)
		if err != nil {
			t.Fatalf("expected table %q to exist: %v", tbl, err)
		}
		if name != tbl {
			t.Fatalf("got table %q, want %q", name, tbl)
		}
	}

	// Writes stay pinned to a single connection (see store.go rationale);
	// reads get a pool of their own so a long query can't stall ingest.
	if got := s.writer.Stats().MaxOpenConnections; got != 1 {
		t.Errorf("writer MaxOpenConnections = %d, want 1", got)
	}
	if got := s.reader.Stats().MaxOpenConnections; got != readerPoolSize {
		t.Errorf("reader MaxOpenConnections = %d, want %d", got, readerPoolSize)
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
	if _, err := s1.writer.Exec(`DELETE FROM sessions`); err != nil {
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

// TestAWriteThroughTheReaderPoolIsRefused pins the pragma that keeps reads and
// writes apart. query_only is a PER-CONNECTION pragma, and a pool hands out
// whichever connection is free — so setting it once with an Exec would land on
// one connection and leave the rest able to write. The DSN form is supposed to
// apply it to every connection modernc opens; this drives enough concurrent
// writes to force the pool wide open and requires every one of them to fail.
//
// Asserting the refusal rather than reading `PRAGMA query_only` back is
// deliberate: the readout has been seen to report the value it was set to on a
// connection that then accepted the write. The behaviour is the contract.
func TestAWriteThroughTheReaderPoolIsRefused(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.StoreEvent("sess", "user_message", []byte(`{}`)); err != nil {
		t.Fatalf("seed StoreEvent: %v", err)
	}

	const attempts = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	var accepted int
	start := make(chan struct{})
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := s.reader.Exec(
				`INSERT INTO events (session_id, type, data) VALUES (?,?,?)`,
				"smuggled", "user_message", `{}`,
			); err == nil {
				mu.Lock()
				accepted++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	if accepted != 0 {
		t.Errorf("%d of %d writes through the reader pool were accepted, want 0", accepted, attempts)
	}
	var n int
	if err := s.reader.QueryRow(`SELECT count(*) FROM events`).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	if n != 1 {
		t.Errorf("events table holds %d rows, want the 1 seeded row", n)
	}
}

// TestALongReadDoesNotStallAWrite is the assertion the whole change exists for.
// While reads and writes shared one pinned connection, an open *sql.Rows held
// that connection for its entire scan, so every event ingest across every live
// session queued behind it — 573ms for the largest real session, measured on a
// copy of the live database.
//
// Sabotage that proves it is curative: point the reader at s.writer and this
// deadlocks until the read finishes.
func TestALongReadDoesNotStallAWrite(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 500; i++ {
		if _, err := s.StoreEvent("sess", "stream", []byte(`{}`)); err != nil {
			t.Fatalf("seed StoreEvent %d: %v", i, err)
		}
	}

	// Hold a read open mid-scan: one row consumed, the rest still pending, so
	// the connection serving it stays checked out of the pool.
	rows, err := s.reader.Query(`SELECT id, data FROM events ORDER BY id`)
	if err != nil {
		t.Fatalf("open long read: %v", err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatalf("expected at least one row: %v", rows.Err())
	}

	done := make(chan error, 1)
	go func() {
		_, err := s.StoreEvent("sess", "user_message", []byte(`{}`))
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("write during an open read: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a write blocked behind an open read — reads and writes are sharing a connection")
	}

	// The reader must also still be usable after the write, and must see it:
	// two handles on one WAL file, so a read opened later has to observe rows
	// the writer committed in between.
	rows.Close()
	var n int
	if err := s.reader.QueryRow(
		`SELECT count(*) FROM events WHERE type='user_message'`,
	).Scan(&n); err != nil {
		t.Fatalf("count after write: %v", err)
	}
	if n != 1 {
		t.Errorf("reader sees %d user_message rows, want 1 — the reader pool is on a stale snapshot", n)
	}
}

// harnessEvent builds an event body carrying a harness-native session id, the
// shape every real llm-bridge event has. The pre-existing fixtures in this file
// deliberately omit the field; a test for the id must not reuse them, or it
// asserts on a body no harness produces.
func harnessEvent(harnessSessionID, text string) []byte {
	body, err := json.Marshal(map[string]any{
		"harness_session_id": harnessSessionID,
		"harness":            "claude_code",
		"text":               text,
	})
	if err != nil {
		panic(err)
	}
	return body
}

// A search hit has to carry the id that resolves it. log-store's own
// session_id is whatever the writer handed it, and on the live host a third of
// those are bridge ids llm-bridge-server has since deleted; the harness id from
// the events is what still matches a real session row.
func TestSearchSessionsReportsHarnessSessionID(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("br_phantom", "user_message", harnessEvent("cc-uuid-1", "needle here"))
	s.StoreEvent("br_phantom", "assistant", harnessEvent("cc-uuid-1", "needle again"))
	// A session whose events name no harness id at all — 1,675 of the live
	// host's 11,640. It must still come back as a hit, just without the id.
	s.StoreEvent("no_harness_id", "user_message", []byte(`{"text":"needle too"}`))

	hits, err := s.SearchSessions("needle", 10)
	if err != nil {
		t.Fatalf("SearchSessions: %v", err)
	}
	got := map[string]SearchHit{}
	for _, h := range hits {
		got[h.SessionID] = h
	}
	if len(got) != 2 {
		t.Fatalf("got %d hits, want 2: %+v", len(got), hits)
	}
	if got["br_phantom"].HarnessSessionID != "cc-uuid-1" {
		t.Errorf("harness id = %q, want cc-uuid-1", got["br_phantom"].HarnessSessionID)
	}
	if got["br_phantom"].MatchCount != 2 {
		t.Errorf("match count = %d, want 2", got["br_phantom"].MatchCount)
	}
	// Asserted as its own case, not folded into the one above: an inner join
	// would drop this row entirely and every other assertion here would still
	// pass.
	h, ok := got["no_harness_id"]
	if !ok {
		t.Fatal("session with no harness id was dropped from the results")
	}
	if h.HarnessSessionID != "" {
		t.Errorf("harness id = %q, want empty", h.HarnessSessionID)
	}
}

// A resumed or forked Claude Code session reports a new harness uuid partway
// through its stream. The gateway's row holds the current one, so log-store
// must too — keeping the first would point the consumer at a session that has
// moved on.
func TestHarnessSessionIDTracksLatest(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("br_1", "user_message", harnessEvent("cc-uuid-first", "needle"))
	s.StoreEvent("br_1", "result", harnessEvent("cc-uuid-second", "needle"))

	hits, err := s.SearchSessions("needle", 10)
	if err != nil {
		t.Fatalf("SearchSessions: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1", len(hits))
	}
	if hits[0].HarnessSessionID != "cc-uuid-second" {
		t.Errorf("harness id = %q, want cc-uuid-second (the latest)", hits[0].HarnessSessionID)
	}
	// An event that names no id must not blank out the one already recorded.
	s.StoreEvent("br_1", "assistant", []byte(`{"text":"needle, no harness id"}`))
	hits, err = s.SearchSessions("needle", 10)
	if err != nil {
		t.Fatalf("SearchSessions: %v", err)
	}
	if hits[0].HarnessSessionID != "cc-uuid-second" {
		t.Errorf("harness id = %q after an id-less event, want cc-uuid-second", hits[0].HarnessSessionID)
	}
}

// The live database predates the column, so the value of this change rests
// entirely on the backfill: without it every already-stored session stays
// unresolvable forever. Reopening is what runs it.
func TestHarnessSessionIDBackfilledOnReopen(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "log-store.db")

	s1, err := New(dbPath)
	if err != nil {
		t.Fatalf("New 1: %v", err)
	}
	s1.StoreEvent("br_old", "user_message", harnessEvent("cc-uuid-superseded", "needle"))
	s1.StoreEvent("br_old", "result", harnessEvent("cc-uuid-old", "needle"))
	if err := s1.Close(); err != nil {
		t.Fatalf("Close 1: %v", err)
	}

	// Drop the column and its index to reproduce a pre-migration database
	// holding real events, then reopen and let migrate() do the import.
	pre, err := New(dbPath)
	if err != nil {
		t.Fatalf("New pre: %v", err)
	}
	if _, err := pre.writer.Exec(`DROP INDEX IF EXISTS idx_sessions_harness_session_id`); err != nil {
		t.Fatalf("drop index: %v", err)
	}
	if _, err := pre.writer.Exec(`ALTER TABLE sessions DROP COLUMN harness_session_id`); err != nil {
		t.Fatalf("drop column: %v", err)
	}
	if err := pre.Close(); err != nil {
		t.Fatalf("Close pre: %v", err)
	}

	s2, err := New(dbPath)
	if err != nil {
		t.Fatalf("New 2: %v", err)
	}
	defer s2.Close()
	hits, err := s2.SearchSessions("needle", 10)
	if err != nil {
		t.Fatalf("SearchSessions: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("got %d hits, want 1", len(hits))
	}
	if hits[0].HarnessSessionID != "cc-uuid-old" {
		t.Errorf("harness id = %q after reopen, want cc-uuid-old — the backfill did not run", hits[0].HarnessSessionID)
	}
}

// The dedupe key an importer needs. Discovery decides a transcript is new by
// asking its own database and writes the answer into log-store; the check has
// to be answerable by the store that holds the write, and the harness id is
// the only id both services agree on.
func TestSessionsHoldingHarnessSessionID(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("br_first", "user_message", harnessEvent("cc-uuid-held", "one"))
	s.StoreEvent("br_first", "assistant", harnessEvent("cc-uuid-held", "two"))
	// The same harness session imported a second time under a fresh bridge
	// id — 103 harness sessions on the live host carry more than one.
	s.StoreEvent("br_duplicate", "user_message", harnessEvent("cc-uuid-held", "one"))
	// An unrelated session, and one whose events name no harness id.
	s.StoreEvent("br_other", "user_message", harnessEvent("cc-uuid-other", "x"))
	s.StoreEvent("br_anonymous", "user_message", []byte(`{"text":"y"}`))

	held, err := s.SessionsHoldingHarnessSessionID("cc-uuid-held")
	if err != nil {
		t.Fatalf("SessionsHoldingHarnessSessionID: %v", err)
	}
	if len(held) != 2 {
		t.Fatalf("got %d held sessions, want 2: %+v", len(held), held)
	}
	counts := map[string]int{}
	for _, h := range held {
		counts[h.SessionID] = h.EventCount
	}
	if counts["br_first"] != 2 {
		t.Errorf("br_first event_count = %d, want 2", counts["br_first"])
	}
	if counts["br_duplicate"] != 1 {
		t.Errorf("br_duplicate event_count = %d, want 1", counts["br_duplicate"])
	}

	unknown, err := s.SessionsHoldingHarnessSessionID("cc-uuid-never-seen")
	if err != nil {
		t.Fatalf("SessionsHoldingHarnessSessionID unknown: %v", err)
	}
	if len(unknown) != 0 {
		t.Errorf("unknown harness id returned %d sessions, want 0: %+v", len(unknown), unknown)
	}
}

// '' is a stored value, not a wildcard: every session whose events name no
// harness id carries it. Answering a lookup for '' would report unrelated
// transcripts as a match and talk an importer out of a real import.
func TestSessionsHoldingHarnessSessionIDRefusesEmpty(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("br_anonymous_a", "user_message", []byte(`{"text":"a"}`))
	s.StoreEvent("br_anonymous_b", "user_message", []byte(`{"text":"b"}`))

	held, err := s.SessionsHoldingHarnessSessionID("")
	if err == nil {
		t.Fatalf("empty harness id returned %d sessions and no error, want an error", len(held))
	}
	if held != nil {
		t.Errorf("empty harness id returned %+v alongside its error, want nil", held)
	}
}

// A resumed Claude Code session reports a new harness uuid partway through its
// stream and the projection rolls forward to the latest one. The lookup has to
// agree with that, or an importer asks about the id it holds and is told no.
func TestSessionsHoldingHarnessSessionIDFollowsTheLatestID(t *testing.T) {
	s := newTestStore(t)
	s.StoreEvent("br_resumed", "user_message", harnessEvent("cc-uuid-before", "one"))
	s.StoreEvent("br_resumed", "user_message", harnessEvent("cc-uuid-after", "two"))

	after, err := s.SessionsHoldingHarnessSessionID("cc-uuid-after")
	if err != nil {
		t.Fatalf("SessionsHoldingHarnessSessionID: %v", err)
	}
	if len(after) != 1 || after[0].SessionID != "br_resumed" {
		t.Fatalf("latest id resolved to %+v, want br_resumed", after)
	}
	if after[0].EventCount != 2 {
		t.Errorf("event_count = %d, want 2 — the count is the whole session, not the events naming that id", after[0].EventCount)
	}
	before, err := s.SessionsHoldingHarnessSessionID("cc-uuid-before")
	if err != nil {
		t.Fatalf("SessionsHoldingHarnessSessionID: %v", err)
	}
	if len(before) != 0 {
		t.Errorf("superseded id resolved to %+v, want nothing — the projection holds the latest", before)
	}
}
