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

type Store struct {
	db *sql.DB
}

func New(dbPath string) (*Store, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db dir: %w", err)
	}
	d, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, err
	}
	if _, err := d.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;"); err != nil {
		d.Close()
		return nil, fmt.Errorf("sqlite pragmas: %w", err)
	}

	// Single connection serializes writes through Go's sql pool. Without this,
	// modernc.org/sqlite still hits SQLITE_BUSY (5) under concurrent writers,
	// silently dropping events (e.g. user_message via PushEvent) when the
	// caller swallows the 500.
	d.SetMaxOpenConns(1)

	s := &Store{db: d}
	if err := s.migrate(); err != nil {
		d.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	if _, err := s.db.Exec(`
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
	if _, err := s.db.Exec(`
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
	// Backfill from events for any session not already projected. Runs once
	// per session — guarded by NOT IN to skip already-populated sessions.
	if _, err := s.db.Exec(`
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

// StoreEvent persists a raw event and updates the per-session projection.
// The projection is a derived cache; failure to update is logged but does
// not fail the underlying event insert (the event is still queryable; the
// next StoreEvent call rolls forward correctly, and on next boot migrate()
// will not re-backfill an already-present row).
func (s *Store) StoreEvent(sessionID, eventType string, data []byte) (int64, error) {
	result, err := s.db.Exec(
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
	if _, err := s.db.Exec(
		`INSERT OR IGNORE INTO sessions (session_id) VALUES (?)`,
		sessionID,
	); err != nil {
		return fmt.Errorf("seed sessions row: %w", err)
	}
	// Bump last_active on every event regardless of type.
	if _, err := s.db.Exec(
		`UPDATE sessions SET last_active = CURRENT_TIMESTAMP WHERE session_id = ?`,
		sessionID,
	); err != nil {
		return fmt.Errorf("touch last_active: %w", err)
	}
	switch eventType {
	case "user_message":
		_, err := s.db.Exec(
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
			_, err := s.db.Exec(
				`UPDATE sessions SET ended_at = CURRENT_TIMESTAMP WHERE session_id = ?`,
				sessionID,
			)
			return err
		}
		_, err := s.db.Exec(
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
		_, err := s.db.Exec(
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
	rows, err := s.db.Query(q, args...)
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
	rows, err := s.db.Query(q, args...)
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

// SearchSessions returns session IDs with events whose raw data contains query.
// Match count is the number of matching events per session.
func (s *Store) SearchSessions(query string, limit int) ([]SearchHit, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(
		`SELECT session_id, COUNT(*) AS n
		 FROM events
		 WHERE data LIKE '%' || ? || '%'
		 GROUP BY session_id
		 ORDER BY MAX(id) DESC
		 LIMIT ?`,
		query, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var hits []SearchHit
	for rows.Next() {
		var h SearchHit
		if err := rows.Scan(&h.SessionID, &h.MatchCount); err != nil {
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
	rows, err := s.db.Query(`
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
	row := s.db.QueryRow(
		`SELECT COALESCE(MAX(id), 0) FROM events WHERE session_id=? AND type='user_message'`,
		sessionID,
	)
	if err := row.Scan(&ts.LastUserMessageEventID); err != nil {
		return ts, err
	}
	row = s.db.QueryRow(
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
	rows, err := s.db.Query(`SELECT DISTINCT session_id FROM events ORDER BY session_id`)
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
