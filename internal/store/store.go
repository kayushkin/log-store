package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// readerPoolSize bounds how many queries run at once. Reads here are not
// small — one /messages call over the largest real session scans 13,776 rows
// and serialises 306MB — so an unbounded pool would let a handful of clients
// pin every core. Eight is well above the number of dashboards this serves
// and still leaves the machine room to run the harnesses that feed it.
const readerPoolSize = 8

type Store struct {
	// writer runs every INSERT and UPDATE through a single connection.
	// modernc.org/sqlite treats each pooled connection as an independent
	// writer, so more than one still hits SQLITE_BUSY (5) under concurrent
	// ingest and silently drops events when the caller swallows the 500.
	writer *sql.DB

	// reader runs every SELECT on its own pool. Under WAL a reader never
	// blocks the writer, so a query can no longer stall event ingest for
	// every live session. Measured on the live 1.9GB database: draining the
	// largest session's events holds its connection for 573ms, and while
	// reads and writes shared this one connection that was 573ms in which
	// no session anywhere could log an event. Opened query_only so a write
	// routed here fails loudly instead of quietly competing for the lock.
	//
	// Honest about the size of the win: an end-to-end A/B (sustained reads
	// against old and new binaries on a copy of the live database) could
	// not separate them — the effect sits inside the noise of a busy box.
	// This is a correctness change, not a measured speedup.
	reader *sql.DB
}

// busyTimeoutMillisecondsWanted is how long a connection waits for a lock
// before giving up. Named so both pools' DSNs and the check that proves the
// writer's took effect cannot drift apart.
const busyTimeoutMillisecondsWanted = 5000

func New(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	writer, err := sql.Open("sqlite", fmt.Sprintf("%s?_pragma=busy_timeout(%d)", dbPath, busyTimeoutMillisecondsWanted))
	if err != nil {
		return nil, err
	}
	// journal_mode is a property of the database file, so this one Exec sets
	// WAL for the reader pool too. busy_timeout is per-connection, so both
	// pools carry their own in the DSN — a one-shot Exec would reach only
	// whichever connection the pool happened to hand out. The writer is capped
	// at one connection below, which used to make the Exec form work by
	// accident; it stops working the moment that connection is replaced.
	if _, err := writer.Exec("PRAGMA journal_mode=WAL"); err != nil {
		writer.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	// An unrecognised DSN key is silently ignored by this driver, so prove the
	// setting actually took effect instead of trusting the connection string.
	var writerBusyTimeoutMilliseconds int
	if err := writer.QueryRow("PRAGMA busy_timeout").Scan(&writerBusyTimeoutMilliseconds); err != nil {
		writer.Close()
		return nil, fmt.Errorf("read writer busy_timeout pragma: %w", err)
	}
	if writerBusyTimeoutMilliseconds != busyTimeoutMillisecondsWanted {
		writer.Close()
		return nil, fmt.Errorf("writer busy_timeout is %d, want %d: the DSN did not take effect",
			writerBusyTimeoutMilliseconds, busyTimeoutMillisecondsWanted)
	}
	writer.SetMaxOpenConns(1)

	// modernc applies _pragma= to every connection it opens, which a one-shot
	// Exec on a pool cannot do — query_only and busy_timeout are both
	// per-connection. Verified by behaviour, not by reading the pragma back:
	// TestAWriteThroughTheReaderPoolIsRefused drives 40 concurrent writes
	// through this handle and every one must fail.
	reader, err := sql.Open("sqlite", fmt.Sprintf("%s?_pragma=query_only(1)&_pragma=busy_timeout(%d)",
		dbPath, busyTimeoutMillisecondsWanted))
	if err != nil {
		writer.Close()
		return nil, err
	}
	reader.SetMaxOpenConns(readerPoolSize)

	s := &Store{writer: writer, reader: reader}
	if err := s.migrate(); err != nil {
		writer.Close()
		reader.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	rerr := s.reader.Close()
	if err := s.writer.Close(); err != nil {
		return err
	}
	return rerr
}

func (s *Store) migrate() error {
	if _, err := s.writer.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			type       TEXT NOT NULL,
			data       TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
		CREATE INDEX IF NOT EXISTS idx_events_session_type ON events(session_id, type);
	`); err != nil {
		return err
	}
	// sessions projection — per-session rollup of token / cost / turn / model
	// totals derived from the events table. Maintained synchronously by
	// StoreEvent so reads are O(1) instead of O(events). Always rebuildable
	// from events; backfilled below if rows are missing.
	if _, err := s.writer.Exec(`
		CREATE TABLE IF NOT EXISTS sessions (
			session_id    TEXT PRIMARY KEY,
			turn_count    INTEGER NOT NULL DEFAULT 0,
			input_tokens  INTEGER NOT NULL DEFAULT 0,
			output_tokens INTEGER NOT NULL DEFAULT 0,
			cost_usd      REAL    NOT NULL DEFAULT 0,
			duration_ms   INTEGER NOT NULL DEFAULT 0,
			model         TEXT    NOT NULL DEFAULT '',
			started_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			last_active   DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
			ended_at      DATETIME
		);
		CREATE INDEX IF NOT EXISTS idx_sessions_last_active ON sessions(last_active);
	`); err != nil {
		return err
	}
	if err := s.migrateHarnessSessionID(); err != nil {
		return err
	}
	// Backfill from events for any session not already projected. Runs once
	// per session — guarded by NOT IN to skip already-populated sessions.
	if _, err := s.writer.Exec(`
		INSERT INTO sessions (
			session_id, turn_count, input_tokens, output_tokens,
			cost_usd, duration_ms, model, started_at, last_active, ended_at
		)
		SELECT
			e.session_id,
			COALESCE(SUM(CASE WHEN e.type = 'user_message' THEN 1 ELSE 0 END), 0)               AS turn_count,
			COALESCE(SUM(CASE WHEN e.type = 'result' THEN json_extract(e.data, '$.result.usage.input_tokens') ELSE 0 END), 0)  AS input_tokens,
			COALESCE(SUM(CASE WHEN e.type = 'result' THEN json_extract(e.data, '$.result.usage.output_tokens') ELSE 0 END), 0) AS output_tokens,
			COALESCE(SUM(CASE WHEN e.type = 'result' THEN json_extract(e.data, '$.result.cost.total_usd') ELSE 0 END), 0)      AS cost_usd,
			COALESCE(SUM(CASE WHEN e.type = 'result' THEN json_extract(e.data, '$.result.duration_ms') ELSE 0 END), 0)         AS duration_ms,
			COALESCE((
				SELECT json_extract(e2.data, '$.result.model')
				FROM events e2
				WHERE e2.session_id = e.session_id
				  AND e2.type = 'result'
				  AND json_extract(e2.data, '$.result.model') IS NOT NULL
				ORDER BY e2.id DESC LIMIT 1
			), '') AS model,
			MIN(e.created_at)                                                                   AS started_at,
			MAX(e.created_at)                                                                   AS last_active,
			(
				SELECT MAX(e3.created_at) FROM events e3
				WHERE e3.session_id = e.session_id AND e3.type IN ('result','error')
			)                                                                                   AS ended_at
		FROM events e
		WHERE e.session_id NOT IN (SELECT session_id FROM sessions)
		GROUP BY e.session_id
	`); err != nil {
		return fmt.Errorf("backfill sessions projection: %w", err)
	}
	return nil
}

// migrateHarnessSessionID adds the sessions projection's harness_session_id
// column and backfills it from the events that already carry the id.
//
// Why the column exists at all: log-store keys everything on one opaque
// session_id — whatever the writer handed it. In practice that is sometimes
// llm-bridge-server's bridge_id and sometimes the harness-native id, and a
// third of this host's rows carry a bridge_id the gateway has since deleted
// as a phantom. A consumer that takes a search hit's session_id straight to
// the gateway's GET /sessions/{id} therefore gets a 404 for a session that
// very much exists — measured on this host at 4,083 of 11,640 sessions
// (35%), against only 4 that are genuinely unrecoverable.
//
// Every one of those events already names its harness_session_id, and the
// gateway keys a unique index on that same column. Publishing the id
// log-store already stores is what makes a hit resolvable, and it needs no
// knowledge of the gateway's session set — so it adds no coupling between
// the two services. log-store still does not resolve anything itself; it
// reports both ids and lets the consumer join.
//
// The backfill is deliberately tied to the ALTER: `ADD COLUMN` fails once
// the column exists, and that error is the signal that the one-shot import
// has already run. The alternative — guarding on the column still being
// empty — would re-scan every event of every session that legitimately has
// no harness id on every single boot, forever. Both statements share one
// transaction so a failed backfill rolls the column back and retries on the
// next boot rather than leaving the table half-migrated.
func (s *Store) migrateHarnessSessionID() error {
	tx, err := s.writer.Begin()
	if err != nil {
		return fmt.Errorf("begin harness_session_id migration: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.Exec(`ALTER TABLE sessions ADD COLUMN harness_session_id TEXT NOT NULL DEFAULT ''`); err != nil {
		// Column already present: the backfill ran on an earlier boot.
		return nil
	}
	// Latest wins, not first. A resumed or forked Claude Code session reports
	// a new harness uuid partway through its event stream, and the gateway's
	// sessions row holds the current one — 11 sessions on this host have more
	// than one. Taking MAX(id) is what keeps the two stores agreeing.
	if _, err := tx.Exec(`
		UPDATE sessions SET harness_session_id = COALESCE((
			SELECT json_extract(e.data, '$.harness_session_id')
			FROM events e
			WHERE e.session_id = sessions.session_id
			  AND json_extract(e.data, '$.harness_session_id') IS NOT NULL
			  AND json_extract(e.data, '$.harness_session_id') != ''
			ORDER BY e.id DESC LIMIT 1
		), '')
	`); err != nil {
		return fmt.Errorf("backfill harness_session_id: %w", err)
	}
	if _, err := tx.Exec(
		`CREATE INDEX IF NOT EXISTS idx_sessions_harness_session_id
		 ON sessions(harness_session_id) WHERE harness_session_id != ''`,
	); err != nil {
		return fmt.Errorf("index harness_session_id: %w", err)
	}
	return tx.Commit()
}

// StoreEvent persists a raw event and updates the per-session projection.
// The projection is a derived cache; failure to update is logged but does
// not fail the underlying event insert (the event is still queryable; the
// next StoreEvent call rolls forward correctly, and on next boot migrate()
// will not re-backfill an already-present row).
func (s *Store) StoreEvent(sessionID, eventType string, data []byte) (int64, error) {
	result, err := s.writer.Exec(
		`INSERT INTO events (session_id, type, data) VALUES (?,?,?)`,
		sessionID, eventType, string(data),
	)
	if err != nil {
		return 0, err
	}
	id, _ := result.LastInsertId()
	if err := s.updateSessionProjection(sessionID, eventType, data); err != nil {
		log.Printf("[log-store] update sessions projection for %s/%s: %v", sessionID, eventType, err)
	}
	return id, nil
}

// updateSessionProjection rolls forward the per-session sessions row for one
// new event. Idempotent on re-application of the same event because
// last_active uses CURRENT_TIMESTAMP and turn_count / token sums are
// monotonic only when StoreEvent is called once per event (the caller's job).
func (s *Store) updateSessionProjection(sessionID, eventType string, data []byte) error {
	// Ensure the row exists. INSERT OR IGNORE is a no-op for established
	// sessions; for the first event of a new session it seeds started_at /
	// last_active to NOW().
	if _, err := s.writer.Exec(
		`INSERT OR IGNORE INTO sessions (session_id) VALUES (?)`,
		sessionID,
	); err != nil {
		return fmt.Errorf("seed sessions row: %w", err)
	}
	// Bump last_active on every event regardless of type.
	if _, err := s.writer.Exec(
		`UPDATE sessions SET last_active = CURRENT_TIMESTAMP WHERE session_id = ?`,
		sessionID,
	); err != nil {
		return fmt.Errorf("touch last_active: %w", err)
	}
	// Roll the harness-native id forward from whatever this event reports.
	// It is written on every event type rather than on session start because
	// log-store has no session-start event: the first frame of a session is
	// just an ordinary event, and a resumed session reports a new harness
	// uuid partway through. Last writer wins, matching the migration's
	// MAX(id) backfill and the gateway's own row.
	if harnessSessionID := harnessSessionIDOf(data); harnessSessionID != "" {
		if _, err := s.writer.Exec(
			`UPDATE sessions SET harness_session_id = ?
			 WHERE session_id = ? AND harness_session_id != ?`,
			harnessSessionID, sessionID, harnessSessionID,
		); err != nil {
			return fmt.Errorf("set harness_session_id: %w", err)
		}
	}
	switch eventType {
	case "user_message":
		_, err := s.writer.Exec(
			`UPDATE sessions SET turn_count = turn_count + 1 WHERE session_id = ?`,
			sessionID,
		)
		return err
	case "result":
		// Parse usage / cost / model / duration from the result event.
		// Failures here are silent: the projection just doesn't get the
		// numbers from this event. Better to log and continue than to
		// block event ingestion on a malformed result.
		var ev struct {
			Result struct {
				Usage struct {
					InputTokens  int64 `json:"input_tokens"`
					OutputTokens int64 `json:"output_tokens"`
				} `json:"usage"`
				Cost struct {
					TotalUSD float64 `json:"total_usd"`
				} `json:"cost"`
				DurationMS int64  `json:"duration_ms"`
				Model      string `json:"model"`
			} `json:"result"`
		}
		if err := json.Unmarshal(data, &ev); err != nil {
			log.Printf("[log-store] decode result event %s: %v", sessionID, err)
			// Still set ended_at — the event terminated the turn even if
			// the body was malformed.
			_, err := s.writer.Exec(
				`UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE session_id = ?`,
				sessionID,
			)
			return err
		}
		_, err := s.writer.Exec(
			`UPDATE sessions SET
				input_tokens = input_tokens + ?,
				output_tokens = output_tokens + ?,
				cost_usd = cost_usd + ?,
				duration_ms = duration_ms + ?,
				model = CASE WHEN ? != '' THEN ? ELSE model END,
				ended_at = CURRENT_TIMESTAMP
			WHERE session_id = ?`,
			ev.Result.Usage.InputTokens,
			ev.Result.Usage.OutputTokens,
			ev.Result.Cost.TotalUSD,
			ev.Result.DurationMS,
			ev.Result.Model, ev.Result.Model,
			sessionID,
		)
		return err
	case "error":
		_, err := s.writer.Exec(
			`UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE session_id = ?`,
			sessionID,
		)
		return err
	}
	return nil
}

// ListEvents returns all stored events for a session, ordered chronologically.
// Each event's JSON is decorated with an "event_id" field carrying the row ID,
// so clients can track the high-water mark and dedup against SSE replay.
// If types is non-empty, only events whose `type` is in the set are returned.
func (s *Store) ListEvents(sessionID string, types []string) ([]json.RawMessage, error) {
	q := `SELECT id, data FROM events WHERE session_id=?`
	args := []any{sessionID}
	if len(types) > 0 {
		q += ` AND type IN (` + placeholders(len(types)) + `)`
		for _, t := range types {
			args = append(args, t)
		}
	}
	q += ` ORDER BY id ASC`
	rows, err := s.reader.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []json.RawMessage
	for rows.Next() {
		var rowID int64
		var data string
		if err := rows.Scan(&rowID, &data); err != nil {
			return nil, err
		}
		events = append(events, injectEventID([]byte(data), rowID))
	}
	return events, rows.Err()
}

// ListEventsSinceID returns events after a specific row ID. See ListEvents for
// the event_id decoration.
func (s *Store) ListEventsSinceID(sessionID string, afterID int, types []string) ([]json.RawMessage, error) {
	q := `SELECT id, data FROM events WHERE session_id=? AND id > ?`
	args := []any{sessionID, afterID}
	if len(types) > 0 {
		q += ` AND type IN (` + placeholders(len(types)) + `)`
		for _, t := range types {
			args = append(args, t)
		}
	}
	q += ` ORDER BY id ASC`
	rows, err := s.reader.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []json.RawMessage
	for rows.Next() {
		var rowID int64
		var data string
		if err := rows.Scan(&rowID, &data); err != nil {
			return nil, err
		}
		events = append(events, injectEventID([]byte(data), rowID))
	}
	return events, rows.Err()
}

func placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	out := make([]byte, 0, n*2-1)
	for i := 0; i < n; i++ {
		if i > 0 {
			out = append(out, ',')
		}
		out = append(out, '?')
	}
	return string(out)
}

// injectEventID splices an "event_id":<rowID> field into an event's top-level
// JSON object. Returns the original bytes unchanged if data isn't a JSON object.
func injectEventID(data []byte, rowID int64) json.RawMessage {
	trimmed := data
	for len(trimmed) > 0 && (trimmed[0] == ' ' || trimmed[0] == '\t' || trimmed[0] == '\n' || trimmed[0] == '\r') {
		trimmed = trimmed[1:]
	}
	if len(trimmed) == 0 || trimmed[0] != '{' {
		return data
	}
	prefix := fmt.Sprintf(`{"event_id":%d,`, rowID)
	// Handle empty object "{}" — drop the trailing comma.
	if len(trimmed) >= 2 && trimmed[1] == '}' {
		prefix = fmt.Sprintf(`{"event_id":%d`, rowID)
	}
	out := make([]byte, 0, len(prefix)+len(trimmed)-1)
	out = append(out, prefix...)
	out = append(out, trimmed[1:]...)
	return out
}

// harnessSessionIDOf reads the harness-native session id an event carries.
// Returns "" for a malformed body or an event that names no harness id —
// both are ordinary, so neither is an error: 1,675 of this host's 11,640
// sessions have no harness id anywhere in their event stream.
func harnessSessionIDOf(data []byte) string {
	var ev struct {
		HarnessSessionID string `json:"harness_session_id"`
	}
	if err := json.Unmarshal(data, &ev); err != nil {
		return ""
	}
	return ev.HarnessSessionID
}

// SearchSessions returns session IDs with events whose raw data contains query.
// Match count is the number of matching events per session.
//
// Each hit reports both ids it has. session_id is log-store's own key and is
// whatever the writer handed it — for a third of this host's sessions that is
// a bridge_id llm-bridge-server has since deleted as a phantom, so taking it
// straight to GET /sessions/{id} yields a 404 for a live session.
// harness_session_id is the harness-native id from the session's events, and
// it is what the gateway keys its own unique index on. A consumer that 404s
// on the first should retry on the second before calling a hit unrenderable.
func (s *Store) SearchSessions(query string, limit int) ([]SearchHit, error) {
	if limit <= 0 {
		limit = 100
	}
	// The join sits outside the LIMIT, not inside it. Joining in the same
	// SELECT as the GROUP BY makes SQLite look up the projection once per
	// matched group — 8,138 seeks for a whole-corpus query on this host, and
	// measurably 1.1s of the 3.9s it then took. Feeding it the already-
	// truncated result costs at most `limit` seeks instead, which brought the
	// same query back to 2.86s against 2.74s before the column existed.
	//
	// LEFT JOIN, not JOIN: a session with events but no projection row must
	// still appear as a hit with an empty harness id, exactly as it did
	// before this column existed. An inner join would silently drop it.
	rows, err := s.reader.Query(
		`SELECT hit.session_id, hit.match_count, COALESCE(s.harness_session_id, '')
		 FROM (
			SELECT session_id, COUNT(*) AS match_count, MAX(id) AS last_event_id
			FROM events
			WHERE data LIKE '%' || ? || '%'
			GROUP BY session_id
			ORDER BY last_event_id DESC
			LIMIT ?
		 ) hit
		 LEFT JOIN sessions s ON s.session_id = hit.session_id
		 ORDER BY hit.last_event_id DESC`,
		query, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hits []SearchHit
	for rows.Next() {
		var h SearchHit
		if err := rows.Scan(&h.SessionID, &h.MatchCount, &h.HarnessSessionID); err != nil {
			return nil, err
		}
		hits = append(hits, h)
	}
	return hits, rows.Err()
}

// SearchHit is a single session match from SearchSessions.
type SearchHit struct {
	SessionID  string `json:"session_id"`
	MatchCount int    `json:"match_count"`
	// HarnessSessionID is the harness-native id (a Claude Code UUID, a Codex
	// thread_id) taken from the session's own events. Empty when the events
	// name none. It is the key that resolves a hit against llm-bridge-server
	// when session_id does not.
	HarnessSessionID string `json:"harness_session_id"`
}

// HeldSession names one log-store session that already holds a transcript,
// as reported by SessionsHoldingHarnessSessionID.
type HeldSession struct {
	// SessionID is log-store's own key for the transcript — whatever the
	// writer handed it. Usually an llm-bridge-server bridge id, and for a
	// third of this host's rows a bridge id the gateway has since deleted.
	SessionID string `json:"session_id"`
	// EventCount is how many events this session holds. Counted from the
	// events table rather than read off the projection, because the caller
	// is asking whether a transcript is already durable and the projection
	// counts turns, not events.
	EventCount int `json:"event_count"`
	// LastActive is the projection's last-activity stamp for the session.
	LastActive string `json:"last_active"`
}

// SessionsHoldingHarnessSessionID answers "do you already hold this
// harness session's transcript?" — the question llm-bridge-server's
// discovery has to ask before importing one.
//
// Discovery decides a session is new by asking its OWN database, then writes
// the answer's consequence into log-store. A gateway booted with a fresh
// database and the default log-store URL therefore re-imports every
// transcript on disk; that is how 2,863 duplicate sessions reached this
// host's production log-store on 2026-08-01. The dedupe key has to live in
// the store that holds the writes, and the only id both services agree on is
// the harness-native one.
//
// An empty id is refused rather than answered. Sessions whose events name no
// harness id all carry '' in the projection, so treating '' as a lookup key
// would report thousands of unrelated transcripts as a match and suppress a
// real import (the partial index deliberately excludes them too).
//
// Newest activity first, so a caller that wants one row gets the live one.
func (s *Store) SessionsHoldingHarnessSessionID(harnessSessionID string) ([]HeldSession, error) {
	if harnessSessionID == "" {
		return nil, fmt.Errorf("SessionsHoldingHarnessSessionID: harness_session_id is required")
	}
	rows, err := s.reader.Query(
		`SELECT s.session_id,
		        (SELECT COUNT(*) FROM events e WHERE e.session_id = s.session_id),
		        s.last_active
		 FROM sessions s
		 WHERE s.harness_session_id = ?
		 ORDER BY s.last_active DESC`,
		harnessSessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var held []HeldSession
	for rows.Next() {
		var h HeldSession
		if err := rows.Scan(&h.SessionID, &h.EventCount, &h.LastActive); err != nil {
			return nil, err
		}
		held = append(held, h)
	}
	return held, rows.Err()
}

// SessionAggregateRow is one session's summed totals as returned by
// ListSessionAggregates. JSON marshalling lives on msg.SessionAggregate;
// the handler converts row → response struct.
type SessionAggregateRow struct {
	SessionID    string
	Turns        int
	InputTokens  int64
	OutputTokens int64
	CostUSD      float64
	DurationMS   int64
	Model        string
}

// ListSessionAggregates returns per-session token/cost totals from the
// sessions projection table maintained by StoreEvent. Sessions with no
// result events are omitted (no usage to report) — matches the previous
// on-demand-aggregation behavior.
//
// Turns counts user_message events (one per user turn). The previous
// implementation accidentally counted result events as "turns"; the
// projection's turn_count field is the corrected definition.
func (s *Store) ListSessionAggregates() ([]SessionAggregateRow, error) {
	rows, err := s.reader.Query(`
		SELECT session_id, turn_count, input_tokens, output_tokens,
		       cost_usd, duration_ms, model
		FROM sessions
		WHERE input_tokens > 0 OR output_tokens > 0 OR cost_usd > 0
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []SessionAggregateRow
	for rows.Next() {
		var r SessionAggregateRow
		if err := rows.Scan(&r.SessionID, &r.Turns, &r.InputTokens, &r.OutputTokens, &r.CostUSD, &r.DurationMS, &r.Model); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// TurnState reports per-session in-flight turn information by inspecting the
// events table for the latest user_message and the latest terminator
// (result/error).
//
// A turn is in-flight when a user_message exists with no later terminator.
// LastUserMessageEventID and LastTerminatorEventID are 0 when no such event
// exists; bridge-server callers compare them to decide whether to recover.
type TurnState struct {
	LastUserMessageEventID int64 `json:"last_user_message_event_id"`
	LastTerminatorEventID  int64 `json:"last_terminator_event_id"`
	InFlight               bool  `json:"in_flight"`
}

// TurnState returns the latest user_message and terminator row IDs for a
// session along with the derived in-flight flag.
func (s *Store) TurnState(sessionID string) (TurnState, error) {
	var ts TurnState
	row := s.reader.QueryRow(
		`SELECT COALESCE(MAX(id), 0) FROM events WHERE session_id=? AND type='user_message'`,
		sessionID,
	)
	if err := row.Scan(&ts.LastUserMessageEventID); err != nil {
		return ts, err
	}
	row = s.reader.QueryRow(
		`SELECT COALESCE(MAX(id), 0) FROM events WHERE session_id=? AND type IN ('result','error')`,
		sessionID,
	)
	if err := row.Scan(&ts.LastTerminatorEventID); err != nil {
		return ts, err
	}
	ts.InFlight = ts.LastUserMessageEventID > ts.LastTerminatorEventID && ts.LastUserMessageEventID > 0
	return ts, nil
}

// SessionIDs returns all distinct session IDs that have events.
func (s *Store) SessionIDs() ([]string, error) {
	rows, err := s.reader.Query(`SELECT DISTINCT session_id FROM events ORDER BY session_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
