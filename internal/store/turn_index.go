package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// The turn index and the stored turn model.
//
// WHY. /messages used to rebuild the turn model from raw events on every read.
// Measured 2026-09-16 through llm-bridge-server: br_1782941699898374171 (13,776
// events, 196 MB, two turns) took 1.2–1.4 s on EVERY request to return one entry
// of 2 KB, and the largest session paid 0.3 s per page. The work is the same each
// time because settled events do not change.
//
// WHAT IS STORED, and who owns it. Events stay the only record of what happened.
// Everything in the tables below is DERIVED from them and can be deleted and
// rebuilt at any time:
//
//	turn_index_sessions  one row per session whose turn index is current. A session
//	                     with events and no row here is still waiting for its index.
//	event_turns          which turn each event belongs to (event_id → turn_seq).
//	turns                one row per turn: its event range, whether it holds a
//	                     prompt, and the stored builder output for it.
//	turn_entries         the builder's projected entries for a turn, one row each.
//	dedup_candidates     for each prompt or reply that may be a dual-emitted copy, the
//	                     hash of what it pairs on and its source. The pairing itself
//	                     is the server's (dedup.go); this table only keeps its inputs,
//	                     so pairing a whole session reads a few lean rows.
//
// Turn assignment follows the builder's own rule (server.buildTurns): an event's
// turn is its turn_id; an event without one belongs to the previous event's turn;
// the very first such event opens a turn named "t_<event id>". The index applies
// that rule once per session in event order instead of once per page, so a page
// boundary can no longer change which turn an event lands in.
//
// The store knows nothing about what an entry looks like. It keeps the builder's
// output as opaque JSON, so the entry shape has one definition (server.Entry) and
// no column copy of it that could drift.

// turnIndexMigration creates the derived tables. Every statement is idempotent.
const turnIndexMigration = `
	CREATE TABLE IF NOT EXISTS turn_index_sessions (
		session_id               TEXT PRIMARY KEY,
		index_version            INTEGER NOT NULL,
		indexed_through_event_id INTEGER NOT NULL,
		last_turn_id             TEXT NOT NULL,
		next_turn_seq            INTEGER NOT NULL
	) WITHOUT ROWID;

	CREATE TABLE IF NOT EXISTS event_turns (
		event_id   INTEGER PRIMARY KEY,
		session_id TEXT NOT NULL,
		turn_seq   INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_event_turns_session_turn
		ON event_turns(session_id, turn_seq, event_id);

	CREATE TABLE IF NOT EXISTS turns (
		session_id                    TEXT    NOT NULL,
		turn_seq                      INTEGER NOT NULL,
		turn_id                       TEXT    NOT NULL,
		first_event_id                INTEGER NOT NULL,
		last_event_id                 INTEGER NOT NULL,
		event_count                   INTEGER NOT NULL,
		has_user_message              INTEGER NOT NULL DEFAULT 0,
		materializer_version          INTEGER NOT NULL DEFAULT 0,
		materialized_through_event_id INTEGER NOT NULL DEFAULT 0,
		turn_json                     TEXT    NOT NULL DEFAULT '',
		source_groups_json            TEXT    NOT NULL DEFAULT '',
		aggregate_sources_json        TEXT    NOT NULL DEFAULT '',
		dedup_fingerprint             TEXT    NOT NULL DEFAULT '',
		PRIMARY KEY (session_id, turn_seq)
	) WITHOUT ROWID;
	CREATE UNIQUE INDEX IF NOT EXISTS idx_turns_session_turn_id ON turns(session_id, turn_id);
	CREATE INDEX IF NOT EXISTS idx_turns_session_prompt ON turns(session_id, has_user_message, turn_seq);

	CREATE TABLE IF NOT EXISTS turn_entries (
		session_id TEXT    NOT NULL,
		turn_seq   INTEGER NOT NULL,
		event_id   INTEGER NOT NULL,
		entry_json TEXT    NOT NULL,
		PRIMARY KEY (session_id, turn_seq, event_id)
	) WITHOUT ROWID;

	CREATE TABLE IF NOT EXISTS dedup_candidates (
		event_id   INTEGER PRIMARY KEY,
		session_id TEXT    NOT NULL,
		turn_seq   INTEGER NOT NULL,
		key_hash   TEXT    NOT NULL,
		source     TEXT    NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_dedup_candidates_session ON dedup_candidates(session_id, event_id);
`

// turnIndexRulesVersion names the turn assignment rule in this file. Bump it when
// that rule changes: every session's index is then rebuilt, on its next read or by
// the background pass.
const turnIndexRulesVersion = 1

// DedupCandidateFunc reports whether an event may be a dual-emitted copy, and if so
// the hash of what it pairs on and its source. The server owns that rule; the store
// only records its answers.
type DedupCandidateFunc func(eventType string, data []byte) (keyHash, source string, ok bool, err error)

// DedupCandidate is one stored candidate.
type DedupCandidate struct {
	EventID int64
	TurnSeq int64
	KeyHash string
	Source  string
}

// SetDedupCandidates installs the server's candidate rule and its version. Until it
// is set, no candidates are recorded; the server sets it before it serves anything.
func (s *Store) SetDedupCandidates(fn DedupCandidateFunc, version int) {
	s.dedupCandidateOf = fn
	s.turnIndexVersion = turnIndexRulesVersion*1000 + version
}

// turnIndexChunk is how many events one indexing pass reads and writes at a time.
// Each chunk is one write transaction, so it bounds how long indexing an old
// session holds the single writer away from live ingest.
const turnIndexChunk = 2000

// eventTurnFields is the part of an event the index reads.
type eventTurnFields struct {
	TurnID string `json:"turn_id"`
}

// sessionTurnState is the running turn assignment for one session.
type sessionTurnState struct {
	lastTurnID  string
	nextTurnSeq int64
}

// assignTurn applies the builder's turn rule to one event and reports its turn id.
func (st *sessionTurnState) assignTurn(eventID int64, rawTurnID string) string {
	turnID := rawTurnID
	if turnID == "" {
		turnID = st.lastTurnID
	}
	if turnID == "" {
		turnID = fmt.Sprintf("t_%d", eventID)
	}
	st.lastTurnID = turnID
	return turnID
}

// sessionLocks serialises index building per session, so a read that indexes a
// session on demand and the background pass cannot both build it at once.
type sessionLocks struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func (l *sessionLocks) lock(sessionID string) func() {
	l.mu.Lock()
	if l.locks == nil {
		l.locks = map[string]*sync.Mutex{}
	}
	m, ok := l.locks[sessionID]
	if !ok {
		m = &sync.Mutex{}
		l.locks[sessionID] = m
	}
	l.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// execer is what both *sql.DB and *sql.Tx offer for writes and reads.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
}

// indexEventInTx records one newly inserted event in the turn index, inside the
// transaction that inserted it. It does nothing for a session whose index is not
// current yet: that session's events are indexed in order by BuildTurnIndex, which
// also picks up this one.
func (s *Store) indexEventInTx(tx *sql.Tx, sessionID string, eventID int64, eventType string, data []byte) error {
	var st sessionTurnState
	var indexedThrough int64
	var version int
	err := tx.QueryRow(
		`SELECT index_version, indexed_through_event_id, last_turn_id, next_turn_seq FROM turn_index_sessions WHERE session_id=?`,
		sessionID,
	).Scan(&version, &indexedThrough, &st.lastTurnID, &st.nextTurnSeq)
	if err == nil && version != s.turnIndexVersion {
		// Built by other rules: BuildTurnIndex rebuilds the whole session, this event
		// included.
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		// No index row. A session whose ONLY event is this one is brand new, and its
		// index starts current right here. Any other session still has older events
		// waiting for BuildTurnIndex.
		var olderEventExists bool
		if err := tx.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM events WHERE session_id=? AND id < ?)`, sessionID, eventID,
		).Scan(&olderEventExists); err != nil {
			return fmt.Errorf("check for older events: %w", err)
		}
		if olderEventExists {
			return nil
		}
		st = sessionTurnState{nextTurnSeq: 1}
		if _, err := tx.Exec(
			`INSERT INTO turn_index_sessions (session_id, index_version, indexed_through_event_id, last_turn_id, next_turn_seq) VALUES (?,?,0,'',1)`,
			sessionID, s.turnIndexVersion,
		); err != nil {
			return fmt.Errorf("open turn index: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("read turn index state: %w", err)
	}

	var fields eventTurnFields
	if err := json.Unmarshal(data, &fields); err != nil {
		return fmt.Errorf("read turn_id from event %d: %w", eventID, err)
	}
	if err := s.writeEventTurn(tx, sessionID, &st, eventID, eventType, fields.TurnID, data); err != nil {
		return err
	}
	_, err = tx.Exec(
		`UPDATE turn_index_sessions SET indexed_through_event_id=?, last_turn_id=?, next_turn_seq=? WHERE session_id=?`,
		eventID, st.lastTurnID, st.nextTurnSeq, sessionID,
	)
	return err
}

// writeEventTurn assigns one event to its turn and writes event_turns, turns and,
// for a possible dual-emitted copy, dedup_candidates. `data` may be nil for event
// types that are never candidates.
func (s *Store) writeEventTurn(db execer, sessionID string, st *sessionTurnState, eventID int64, eventType, rawTurnID string, data []byte) error {
	turnID := st.assignTurn(eventID, rawTurnID)
	isPrompt := 0
	if eventType == "user_message" {
		isPrompt = 1
	}
	var turnSeq int64
	err := db.QueryRow(`SELECT turn_seq FROM turns WHERE session_id=? AND turn_id=?`, sessionID, turnID).Scan(&turnSeq)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		turnSeq = st.nextTurnSeq
		st.nextTurnSeq++
		if _, err := db.Exec(
			`INSERT INTO turns (session_id, turn_seq, turn_id, first_event_id, last_event_id, event_count, has_user_message)
			 VALUES (?,?,?,?,?,1,?)`,
			sessionID, turnSeq, turnID, eventID, eventID, isPrompt,
		); err != nil {
			return fmt.Errorf("open turn %q: %w", turnID, err)
		}
	case err != nil:
		return fmt.Errorf("find turn %q: %w", turnID, err)
	default:
		if _, err := db.Exec(
			`UPDATE turns SET last_event_id=?, event_count=event_count+1, has_user_message=MAX(has_user_message, ?)
			 WHERE session_id=? AND turn_seq=?`,
			eventID, isPrompt, sessionID, turnSeq,
		); err != nil {
			return fmt.Errorf("extend turn %q: %w", turnID, err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO event_turns (event_id, session_id, turn_seq) VALUES (?,?,?)`,
		eventID, sessionID, turnSeq,
	); err != nil {
		return fmt.Errorf("index event %d: %w", eventID, err)
	}
	if s.dedupCandidateOf != nil && data != nil {
		keyHash, source, ok, err := s.dedupCandidateOf(eventType, data)
		if err != nil {
			return fmt.Errorf("dedup candidate of event %d: %w", eventID, err)
		}
		if ok {
			if _, err := db.Exec(
				`INSERT INTO dedup_candidates (event_id, session_id, turn_seq, key_hash, source) VALUES (?,?,?,?,?)`,
				eventID, sessionID, turnSeq, keyHash, source,
			); err != nil {
				return fmt.Errorf("record dedup candidate %d: %w", eventID, err)
			}
		}
	}
	return nil
}

// TurnIndexCurrent reports whether a session's turn index covers all its events.
func (s *Store) TurnIndexCurrent(sessionID string) (bool, error) {
	var exists bool
	err := s.reader.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM turn_index_sessions WHERE session_id=? AND index_version=?)`,
		sessionID, s.turnIndexVersion,
	).Scan(&exists)
	return exists, err
}

// BuildTurnIndex indexes every event of a session whose index is not current, and
// marks it current. A no-op for a session that already is.
//
// Any partial index left by an earlier interrupted build is deleted first, so the
// build always starts from the session's first event.
//
// Old events are indexed in chunks, one write transaction each, reading them
// through the reader pool. The LAST step reads whatever is still unindexed inside
// the write transaction that marks the session current. Live ingest shares that
// single writer, so no event can land between that final read and the mark — and
// every event after the mark is indexed by ingest itself.
func (s *Store) BuildTurnIndex(sessionID string) error {
	unlock := s.turnIndexLocks.lock(sessionID)
	defer unlock()

	current, err := s.TurnIndexCurrent(sessionID)
	if err != nil || current {
		return err
	}

	if err := s.deletePartialTurnIndex(sessionID); err != nil {
		return err
	}

	st := sessionTurnState{nextTurnSeq: 1}
	var through int64
	for {
		batch, err := readEventTurnFields(s.reader, sessionID, through, turnIndexChunk)
		if err != nil {
			return err
		}
		if len(batch) < turnIndexChunk {
			break // the rest is indexed inside the final transaction
		}
		tx, err := s.writer.Begin()
		if err != nil {
			return err
		}
		for _, ev := range batch {
			if err := s.writeEventTurn(tx, sessionID, &st, ev.id, ev.eventType, ev.turnID, ev.data); err != nil {
				tx.Rollback()
				return err
			}
			through = ev.id
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit turn index chunk: %w", err)
		}
	}

	tx, err := s.writer.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for {
		batch, err := readEventTurnFields(tx, sessionID, through, turnIndexChunk)
		if err != nil {
			return err
		}
		for _, ev := range batch {
			if err := s.writeEventTurn(tx, sessionID, &st, ev.id, ev.eventType, ev.turnID, ev.data); err != nil {
				return err
			}
			through = ev.id
		}
		if len(batch) < turnIndexChunk {
			break
		}
	}
	if _, err := tx.Exec(
		`INSERT INTO turn_index_sessions (session_id, index_version, indexed_through_event_id, last_turn_id, next_turn_seq) VALUES (?,?,?,?,?)`,
		sessionID, s.turnIndexVersion, through, st.lastTurnID, st.nextTurnSeq,
	); err != nil {
		return fmt.Errorf("mark turn index current: %w", err)
	}
	return tx.Commit()
}

func (s *Store) deletePartialTurnIndex(sessionID string) error {
	tx, err := s.writer.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`DELETE FROM turn_index_sessions WHERE session_id=?`,
		`DELETE FROM dedup_candidates WHERE session_id=?`,
		`DELETE FROM event_turns WHERE session_id=?`,
		`DELETE FROM turn_entries WHERE session_id=?`,
		`DELETE FROM turns WHERE session_id=?`,
	} {
		if _, err := tx.Exec(q, sessionID); err != nil {
			return fmt.Errorf("clear partial turn index: %w", err)
		}
	}
	return tx.Commit()
}

type eventTurnRow struct {
	id        int64
	eventType string
	turnID    string
	data      []byte // only for the event types that can be dedup candidates
}

type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// readEventTurnFields reads the next events of a session after `afterID`, with
// only the fields the index needs. json_extract keeps the event body inside
// SQLite rather than copying 14 KB stream frames into Go to read one string.
func readEventTurnFields(db querier, sessionID string, afterID int64, limit int) ([]eventTurnRow, error) {
	rows, err := db.Query(
		`SELECT id, type, COALESCE(json_extract(data, '$.turn_id'), ''),
		        CASE WHEN type IN ('user_message', 'result') THEN data END
		 FROM events WHERE session_id=? AND id > ? ORDER BY id LIMIT ?`,
		sessionID, afterID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("read events to index: %w", err)
	}
	defer rows.Close()
	var out []eventTurnRow
	for rows.Next() {
		var r eventTurnRow
		var turnID any
		var data sql.NullString
		if err := rows.Scan(&r.id, &r.eventType, &turnID, &data); err != nil {
			return nil, err
		}
		if data.Valid {
			r.data = []byte(data.String)
		}
		// A turn_id that is not a JSON string is not a turn id the builder would
		// read either (msg.Event.TurnID is a string), so it counts as absent.
		if s, ok := turnID.(string); ok {
			r.turnID = s
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SessionsAwaitingTurnIndex returns up to `limit` sessions that have events but no
// current turn index, most recently active first.
func (s *Store) SessionsAwaitingTurnIndex(limit int) ([]string, error) {
	rows, err := s.reader.Query(
		`SELECT s.session_id FROM sessions s
		 WHERE NOT EXISTS (SELECT 1 FROM turn_index_sessions t WHERE t.session_id = s.session_id AND t.index_version = ?)
		 ORDER BY s.last_active DESC LIMIT ?`,
		s.turnIndexVersion, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// StoredTurn is one turns row: the index facts and whatever the builder last stored.
type StoredTurn struct {
	Seq                        int64
	TurnID                     string
	FirstEventID               int64
	LastEventID                int64
	EventCount                 int64
	HasUserMessage             bool
	MaterializerVersion        int
	MaterializedThroughEventID int64
	TurnJSON                   string
	SourceGroupsJSON           string
	AggregateSourcesJSON       string
	DedupFingerprint           string
}

// NeedsMaterializing reports whether the stored builder output is missing, was
// built by different rules, predates an event the turn now holds, or was built
// with different dual-emit pairs than `dedupFingerprint`.
func (t StoredTurn) NeedsMaterializing(version int, dedupFingerprint string) bool {
	return t.MaterializerVersion != version || t.MaterializedThroughEventID < t.LastEventID ||
		t.DedupFingerprint != dedupFingerprint
}

const storedTurnColumns = `turn_seq, turn_id, first_event_id, last_event_id, event_count, has_user_message,
	materializer_version, materialized_through_event_id, turn_json, source_groups_json, aggregate_sources_json,
	dedup_fingerprint`

func scanStoredTurns(rows *sql.Rows) ([]StoredTurn, error) {
	defer rows.Close()
	var out []StoredTurn
	for rows.Next() {
		var t StoredTurn
		var hasPrompt int
		if err := rows.Scan(&t.Seq, &t.TurnID, &t.FirstEventID, &t.LastEventID, &t.EventCount, &hasPrompt,
			&t.MaterializerVersion, &t.MaterializedThroughEventID, &t.TurnJSON, &t.SourceGroupsJSON, &t.AggregateSourcesJSON,
			&t.DedupFingerprint); err != nil {
			return nil, err
		}
		t.HasUserMessage = hasPrompt == 1
		out = append(out, t)
	}
	return out, rows.Err()
}

// TurnWindow returns the turns of the newest page of a session — or, when
// beforeEventID > 0, of the page just older than the turn holding that event — in
// turn order, and whether older turns remain.
//
// A page holds `limitTurns` prompts, counting a turn as a prompt boundary when it
// holds a user_message. Turns without a prompt
// ride along with the prompt turn before them. A session with no prompt turns at
// all pages by plain turn count.
//
// The session's turn index must be current; the caller builds it first.
func (s *Store) TurnWindow(sessionID string, limitTurns int, beforeEventID int64) ([]StoredTurn, bool, error) {
	if limitTurns <= 0 {
		limitTurns = 30
	}

	// Upper bound: turns strictly before the turn holding beforeEventID.
	var beforeSeq int64 // 0 = no bound
	if beforeEventID > 0 {
		var seq sql.NullInt64
		err := s.reader.QueryRow(
			`SELECT turn_seq FROM event_turns WHERE event_id=? AND session_id=?`, beforeEventID, sessionID,
		).Scan(&seq)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			// The cursor names no event of this session. EventPage treats `before` as
			// "rows with a smaller id", so take the turn of the newest event below it
			// and include that turn.
			err = s.reader.QueryRow(
				`SELECT turn_seq FROM event_turns WHERE session_id=? AND event_id < ? ORDER BY event_id DESC LIMIT 1`,
				sessionID, beforeEventID,
			).Scan(&seq)
			if errors.Is(err, sql.ErrNoRows) {
				return nil, false, nil
			}
			if err != nil {
				return nil, false, err
			}
			beforeSeq = seq.Int64 + 1
		case err != nil:
			return nil, false, err
		default:
			beforeSeq = seq.Int64
		}
	}
	bound := ""
	args := []any{sessionID}
	if beforeSeq > 0 {
		bound = ` AND turn_seq < ?`
		args = append(args, beforeSeq)
	}

	// Lower bound: the limitTurns-th newest prompt turn in range.
	var startSeq sql.NullInt64
	err := s.reader.QueryRow(
		`SELECT turn_seq FROM turns WHERE session_id=? AND has_user_message=1`+bound+
			` ORDER BY turn_seq DESC LIMIT 1 OFFSET ?`,
		append(args, limitTurns-1)...,
	).Scan(&startSeq)
	if errors.Is(err, sql.ErrNoRows) {
		var promptTurns int
		if err := s.reader.QueryRow(
			`SELECT COUNT(*) FROM turns WHERE session_id=? AND has_user_message=1`+bound, args...,
		).Scan(&promptTurns); err != nil {
			return nil, false, err
		}
		if promptTurns > 0 {
			startSeq = sql.NullInt64{Int64: 1, Valid: true} // fewer prompts than asked: everything in range
		} else {
			err = s.reader.QueryRow(
				`SELECT turn_seq FROM turns WHERE session_id=?`+bound+` ORDER BY turn_seq DESC LIMIT 1 OFFSET ?`,
				append(args, limitTurns-1)...,
			).Scan(&startSeq)
			if errors.Is(err, sql.ErrNoRows) {
				startSeq = sql.NullInt64{Int64: 1, Valid: true}
			} else if err != nil {
				return nil, false, err
			}
		}
	} else if err != nil {
		return nil, false, err
	}

	rows, err := s.reader.Query(
		`SELECT `+storedTurnColumns+` FROM turns WHERE session_id=? AND turn_seq >= ?`+
			bound+` ORDER BY turn_seq`,
		append([]any{sessionID, startSeq.Int64}, args[1:]...)...,
	)
	if err != nil {
		return nil, false, err
	}
	turns, err := scanStoredTurns(rows)
	if err != nil {
		return nil, false, err
	}
	more := false
	if startSeq.Int64 > 1 {
		if err := s.reader.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM turns WHERE session_id=? AND turn_seq < ?)`, sessionID, startSeq.Int64,
		).Scan(&more); err != nil {
			return nil, false, err
		}
	}
	return turns, more, nil
}

// NewestTurns returns up to `limit` of a session's newest turns, newest first.
func (s *Store) NewestTurns(sessionID string, limit int) ([]StoredTurn, error) {
	rows, err := s.reader.Query(
		`SELECT `+storedTurnColumns+` FROM turns WHERE session_id=? ORDER BY turn_seq DESC LIMIT ?`,
		sessionID, limit,
	)
	if err != nil {
		return nil, err
	}
	return scanStoredTurns(rows)
}

// DedupCandidates returns every dual-emit candidate of a session, in event order.
func (s *Store) DedupCandidates(sessionID string) ([]DedupCandidate, error) {
	rows, err := s.reader.Query(
		`SELECT event_id, turn_seq, key_hash, source FROM dedup_candidates WHERE session_id=? ORDER BY event_id`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DedupCandidate
	for rows.Next() {
		var c DedupCandidate
		if err := rows.Scan(&c.EventID, &c.TurnSeq, &c.KeyHash, &c.Source); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// TurnEvents returns every event of one turn in event order, each carrying the
// turn id the index assigned, so the builder groups them exactly as indexed.
func (s *Store) TurnEvents(sessionID string, turn StoredTurn) ([]EventRow, error) {
	rows, err := s.reader.Query(
		`SELECT e.id, e.type, e.data FROM event_turns t JOIN events e ON e.id = t.event_id
		 WHERE t.session_id=? AND t.turn_seq=? ORDER BY t.event_id`,
		sessionID, turn.Seq,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		r := EventRow{TurnID: turn.TurnID}
		var data string
		if err := rows.Scan(&r.ID, &r.Type, &data); err != nil {
			return nil, err
		}
		r.Data = []byte(data)
		out = append(out, r)
	}
	return out, rows.Err()
}

// TurnEventsTail is TurnEvents limited to the turn's newest `max` events, still in
// event order. It exists for the raw page's one unbounded case: a single turn with
// more events than a page may carry.
func (s *Store) TurnEventsTail(sessionID string, turn StoredTurn, max int) ([]EventRow, error) {
	rows, err := s.reader.Query(
		`SELECT id, type, data FROM (
			SELECT e.id, e.type, e.data FROM event_turns t JOIN events e ON e.id = t.event_id
			WHERE t.session_id=? AND t.turn_seq=? ORDER BY t.event_id DESC LIMIT ?
		) ORDER BY id`,
		sessionID, turn.Seq, max,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		r := EventRow{TurnID: turn.TurnID}
		var data string
		if err := rows.Scan(&r.ID, &r.Type, &data); err != nil {
			return nil, err
		}
		r.Data = []byte(data)
		out = append(out, r)
	}
	return out, rows.Err()
}

// StoredEntry is one entry of the builder's output for a turn.
type StoredEntry struct {
	EventID   int64
	EntryJSON string
}

// MaterializedTurn is the builder's output for one turn, ready to store.
type MaterializedTurn struct {
	Seq                  int64
	Version              int
	ThroughEventID       int64 // the newest event the builder read
	TurnJSON             string
	SourceGroupsJSON     string
	AggregateSourcesJSON string
	DedupFingerprint     string
	Entries              []StoredEntry
}

// ReplaceMaterializedTurn stores the builder's output for a turn, replacing what
// was there, in one transaction. ThroughEventID is what the builder READ, not the
// turn's last event now: an event that landed while it ran leaves the turn stale,
// and the next read rebuilds it.
func (s *Store) ReplaceMaterializedTurn(sessionID string, m MaterializedTurn) error {
	tx, err := s.writer.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`DELETE FROM turn_entries WHERE session_id=? AND turn_seq=?`, sessionID, m.Seq); err != nil {
		return fmt.Errorf("clear stored entries: %w", err)
	}
	for _, e := range m.Entries {
		if _, err := tx.Exec(
			`INSERT INTO turn_entries (session_id, turn_seq, event_id, entry_json) VALUES (?,?,?,?)`,
			sessionID, m.Seq, e.EventID, e.EntryJSON,
		); err != nil {
			return fmt.Errorf("store entry for event %d: %w", e.EventID, err)
		}
	}
	res, err := tx.Exec(
		`UPDATE turns SET materializer_version=?, materialized_through_event_id=?, turn_json=?,
		 source_groups_json=?, aggregate_sources_json=?, dedup_fingerprint=? WHERE session_id=? AND turn_seq=?`,
		m.Version, m.ThroughEventID, m.TurnJSON, m.SourceGroupsJSON, m.AggregateSourcesJSON, m.DedupFingerprint, sessionID, m.Seq,
	)
	if err != nil {
		return fmt.Errorf("store turn: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("store turn %d of %s: no such turn", m.Seq, sessionID)
	}
	return tx.Commit()
}

// StoredEntries returns the stored entries of the given turns, in event order.
func (s *Store) StoredEntries(sessionID string, firstSeq, lastSeq int64) ([]StoredEntry, error) {
	rows, err := s.reader.Query(
		`SELECT event_id, entry_json FROM turn_entries
		 WHERE session_id=? AND turn_seq BETWEEN ? AND ? ORDER BY event_id`,
		sessionID, firstSeq, lastSeq,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoredEntry
	for rows.Next() {
		var e StoredEntry
		if err := rows.Scan(&e.EventID, &e.EntryJSON); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// EventsByID returns the stored bodies of the given events of one session.
func (s *Store) EventsByID(sessionID string, ids []int64) (map[int64][]byte, error) {
	out := make(map[int64][]byte, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	args := []any{sessionID}
	for _, id := range ids {
		args = append(args, id)
	}
	rows, err := s.reader.Query(
		`SELECT id, data FROM events WHERE session_id=? AND id IN (`+placeholders(len(ids))+`)`, args...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		var data string
		if err := rows.Scan(&id, &data); err != nil {
			return nil, err
		}
		out[id] = []byte(data)
	}
	return out, rows.Err()
}

// TurnOfEvent returns the turn an event of a session belongs to.
func (s *Store) TurnOfEvent(sessionID string, eventID int64) (StoredTurn, bool, error) {
	rows, err := s.reader.Query(
		`SELECT `+storedTurnColumns+` FROM turns
		 WHERE session_id=? AND turn_seq = (SELECT turn_seq FROM event_turns WHERE event_id=? AND session_id=?)`,
		sessionID, eventID, sessionID,
	)
	if err != nil {
		return StoredTurn{}, false, err
	}
	turns, err := scanStoredTurns(rows)
	if err != nil || len(turns) == 0 {
		return StoredTurn{}, false, err
	}
	return turns[0], true, nil
}
