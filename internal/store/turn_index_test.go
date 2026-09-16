package store

import (
	"encoding/json"
	"fmt"
	"testing"
)

// storeTurnEvent ingests an event with an explicit turn_id ("" omits it).
func storeTurnEvent(t *testing.T, s *Store, sessionID, typ, turnID string) int64 {
	t.Helper()
	body := map[string]any{"type": typ, "bridge_session_id": sessionID}
	if turnID != "" {
		body["turn_id"] = turnID
	}
	data, _ := json.Marshal(body)
	id, err := s.StoreEvent(sessionID, typ, data)
	if err != nil {
		t.Fatalf("StoreEvent: %v", err)
	}
	return id
}

// insertUnindexedEvent writes an event the way the binary before the turn index
// did: into events only.
func insertUnindexedEvent(t *testing.T, s *Store, sessionID, typ, turnID string) int64 {
	t.Helper()
	body := map[string]any{"type": typ, "bridge_session_id": sessionID}
	if turnID != "" {
		body["turn_id"] = turnID
	}
	data, _ := json.Marshal(body)
	res, err := s.writer.Exec(`INSERT INTO events (session_id, type, data) VALUES (?,?,?)`, sessionID, typ, string(data))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	id, _ := res.LastInsertId()
	return id
}

type turnAssignment struct {
	eventID int64
	seq     int64
	turnID  string
}

func readAssignments(t *testing.T, s *Store, sessionID string) []turnAssignment {
	t.Helper()
	rows, err := s.reader.Query(
		`SELECT et.event_id, et.turn_seq, tu.turn_id FROM event_turns et
		 JOIN turns tu ON tu.session_id = et.session_id AND tu.turn_seq = et.turn_seq
		 WHERE et.session_id=? ORDER BY et.event_id`, sessionID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []turnAssignment
	for rows.Next() {
		var a turnAssignment
		if err := rows.Scan(&a.eventID, &a.seq, &a.turnID); err != nil {
			t.Fatal(err)
		}
		out = append(out, a)
	}
	return out
}

// A new session is indexed as it is written, by the builder's turn rule.
func TestTurnIndex_NewSessionIsIndexedOnIngest(t *testing.T) {
	s := newTestStore(t)
	sess := "s-new"
	first := storeTurnEvent(t, s, sess, "system", "") // before any turn: opens t_<id>
	storeTurnEvent(t, s, sess, "user_message", "turn-a")
	storeTurnEvent(t, s, sess, "usage_total", "") // carries turn-a forward
	storeTurnEvent(t, s, sess, "result", "turn-a")
	storeTurnEvent(t, s, sess, "user_message", "turn-b")

	current, err := s.TurnIndexCurrent(sess)
	if err != nil || !current {
		t.Fatalf("index current = %v, %v; want true", current, err)
	}
	got := readAssignments(t, s, sess)
	want := []struct {
		seq    int64
		turnID string
	}{{1, fmt.Sprintf("t_%d", first)}, {2, "turn-a"}, {2, "turn-a"}, {2, "turn-a"}, {3, "turn-b"}}
	if len(got) != len(want) {
		t.Fatalf("got %d assignments, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].seq != want[i].seq || got[i].turnID != want[i].turnID {
			t.Errorf("event %d: seq %d turn %q, want seq %d turn %q", i, got[i].seq, got[i].turnID, want[i].seq, want[i].turnID)
		}
	}

	turns, more, err := s.TurnWindow(sess, 30, 0)
	if err != nil {
		t.Fatal(err)
	}
	if more || len(turns) != 3 {
		t.Fatalf("window: %d turns more=%v; want 3 false", len(turns), more)
	}
	if turns[0].HasUserMessage || !turns[1].HasUserMessage || turns[1].EventCount != 3 {
		t.Errorf("turn facts wrong: %+v", turns)
	}
}

// A session written before the index existed is left alone by ingest until it is
// built, and the build assigns exactly what indexing on ingest would have.
func TestTurnIndex_BuildMatchesIngestIndexingAndResumesIngest(t *testing.T) {
	s := newTestStore(t)
	plan := []struct{ typ, turn string }{
		{"system", ""}, {"user_message", "a"}, {"stream", ""}, {"user_message", "a"},
		{"result", "a"}, {"user_message", "b"}, {"tool_call", "b"}, {"system", ""}, {"result", "b"},
	}
	for _, p := range plan {
		storeTurnEvent(t, s, "indexed-live", p.typ, p.turn)
		insertUnindexedEvent(t, s, "old", p.typ, p.turn)
	}
	// Ingest after old events exist must not index a pending session out of order.
	storeTurnEvent(t, s, "old", "user_message", "c")
	storeTurnEvent(t, s, "indexed-live", "user_message", "c")
	if current, _ := s.TurnIndexCurrent("old"); current {
		t.Fatal("a session with unindexed events must not be marked current by ingest")
	}

	if err := s.BuildTurnIndex("old"); err != nil {
		t.Fatal(err)
	}
	storeTurnEvent(t, s, "old", "result", "c")
	storeTurnEvent(t, s, "indexed-live", "result", "c")

	built, live := readAssignments(t, s, "old"), readAssignments(t, s, "indexed-live")
	if len(built) != len(plan)+2 || len(built) != len(live) {
		t.Fatalf("built %d, live %d, want %d", len(built), len(live), len(plan)+2)
	}
	for i := range built {
		// Event ids differ between the two sessions; the synthesized id of the very
		// first turn embeds one, so compare its shape rather than its number.
		if built[i].seq != live[i].seq || (i > 0 && built[i].turnID != live[i].turnID) {
			t.Errorf("event %d: built seq %d %q, live seq %d %q", i, built[i].seq, built[i].turnID, live[i].seq, live[i].turnID)
		}
	}
}

// A build larger than one chunk indexes every event once, and a partial build left
// by an interrupted run is replaced rather than added to.
func TestTurnIndex_ChunkedBuildAndPartialLeftovers(t *testing.T) {
	s := newTestStore(t)
	sess := "big"
	n := turnIndexChunk*2 + 17
	for i := 0; i < n; i++ {
		typ := "stream"
		if i%500 == 0 {
			typ = "user_message"
		}
		insertUnindexedEvent(t, s, sess, typ, fmt.Sprintf("turn-%d", i/500))
	}
	// Leftovers of an interrupted build: a wrong turn and a stray assignment.
	if _, err := s.writer.Exec(`INSERT INTO turns (session_id, turn_seq, turn_id, first_event_id, last_event_id, event_count) VALUES (?,99,'stale',1,1,1)`, sess); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writer.Exec(`INSERT INTO event_turns (event_id, session_id, turn_seq) VALUES (1,?,99)`, sess); err != nil {
		t.Fatal(err)
	}

	if err := s.BuildTurnIndex(sess); err != nil {
		t.Fatal(err)
	}
	got := readAssignments(t, s, sess)
	if len(got) != n {
		t.Fatalf("indexed %d events, want %d", len(got), n)
	}
	var turnCount int
	if err := s.reader.QueryRow(`SELECT COUNT(*) FROM turns WHERE session_id=?`, sess).Scan(&turnCount); err != nil {
		t.Fatal(err)
	}
	if want := (n + 499) / 500; turnCount != want {
		t.Errorf("turns = %d, want %d", turnCount, want)
	}
}

// Pages count prompt turns, carry the prompt-less turns between them, and page
// older by the turn that holds the cursor event.
func TestTurnWindow_PagesByPromptTurns(t *testing.T) {
	s := newTestStore(t)
	sess := "paged"
	var promptIDs []int64
	for i := 0; i < 5; i++ {
		promptIDs = append(promptIDs, storeTurnEvent(t, s, sess, "user_message", fmt.Sprintf("p%d", i)))
		storeTurnEvent(t, s, sess, "result", fmt.Sprintf("p%d", i))
		storeTurnEvent(t, s, sess, "system", fmt.Sprintf("bookkeeping-%d", i)) // a prompt-less turn
	}

	turns, more, err := s.TurnWindow(sess, 2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(turns) != 4 || turns[0].TurnID != "p3" {
		t.Fatalf("newest page: %d turns from %q more=%v; want 4 from p3, more", len(turns), firstTurnID(turns), more)
	}

	older, more, err := s.TurnWindow(sess, 2, promptIDs[3])
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(older) != 4 || older[0].TurnID != "p1" || older[len(older)-1].TurnID != "bookkeeping-2" {
		t.Fatalf("older page: %v more=%v", turnIDs(older), more)
	}

	oldest, more, err := s.TurnWindow(sess, 2, promptIDs[1])
	if err != nil {
		t.Fatal(err)
	}
	if more || len(oldest) != 2 || oldest[0].TurnID != "p0" {
		t.Fatalf("oldest page: %v more=%v", turnIDs(oldest), more)
	}

	// A cursor that is not an event of the session pages below it, including the
	// turn of the newest event under it.
	below, _, err := s.TurnWindow(sess, 1, promptIDs[4]-1+1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(below) == 0 || below[len(below)-1].TurnID != "bookkeeping-4" {
		t.Fatalf("foreign cursor page: %v", turnIDs(below))
	}
}

func TestTurnWindow_SessionWithoutPromptsPagesByTurnCount(t *testing.T) {
	s := newTestStore(t)
	for i := 0; i < 4; i++ {
		storeTurnEvent(t, s, "subagent", "block", fmt.Sprintf("t%d", i))
	}
	turns, more, err := s.TurnWindow("subagent", 3, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !more || len(turns) != 3 || turns[0].TurnID != "t1" {
		t.Fatalf("got %v more=%v; want t1..t3, more", turnIDs(turns), more)
	}
}

// A stored turn is stale when an event lands after what the builder read, when it
// was built by other rules, or when its dual-emit pairs moved.
func TestStoredTurn_StaleAfterNewEvent(t *testing.T) {
	s := newTestStore(t)
	sess := "stale"
	storeTurnEvent(t, s, sess, "user_message", "a")
	last := storeTurnEvent(t, s, sess, "result", "a")
	turn, found, err := s.TurnOfEvent(sess, last)
	if err != nil || !found {
		t.Fatalf("TurnOfEvent: %v %v", found, err)
	}
	if err := s.ReplaceMaterializedTurn(sess, MaterializedTurn{
		Seq: turn.Seq, Version: 7, ThroughEventID: last, TurnJSON: `{}`, AggregateSourcesJSON: `{}`, DedupFingerprint: "f",
		Entries: []StoredEntry{{EventID: last, EntryJSON: `{}`}},
	}); err != nil {
		t.Fatal(err)
	}
	newest := func() StoredTurn {
		turns, err := s.NewestTurns(sess, 1)
		if err != nil || len(turns) != 1 {
			t.Fatalf("NewestTurns: %v %v", turns, err)
		}
		return turns[0]
	}
	if newest().NeedsMaterializing(7, "f") {
		t.Fatalf("freshly stored turn reported stale")
	}
	if !newest().NeedsMaterializing(7, "g") {
		t.Fatalf("turn whose pairs moved not reported stale")
	}
	if !newest().NeedsMaterializing(8, "f") {
		t.Fatalf("turn built by other rules not reported stale")
	}
	storeTurnEvent(t, s, sess, "turn_complete", "a")
	if !newest().NeedsMaterializing(7, "f") {
		t.Fatalf("turn with a newer event not reported stale")
	}
}

func firstTurnID(turns []StoredTurn) string {
	if len(turns) == 0 {
		return ""
	}
	return turns[0].TurnID
}

func turnIDs(turns []StoredTurn) []string {
	var out []string
	for _, t := range turns {
		out = append(out, t.TurnID)
	}
	return out
}
