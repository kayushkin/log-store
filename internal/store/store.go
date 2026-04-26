package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
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
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS events (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT NOT NULL,
			type       TEXT NOT NULL,
			data       TEXT NOT NULL,
			created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_events_session ON events(session_id);
		CREATE INDEX IF NOT EXISTS idx_events_session_type ON events(session_id, type);
	`)
	return err
}

// StoreEvent persists a raw event and returns its row ID.
func (s *Store) StoreEvent(sessionID, eventType string, data []byte) (int64, error) {
	result, err := s.db.Exec(
		`INSERT INTO events (session_id, type, data) VALUES (?,?,?)`,
		sessionID, eventType, string(data),
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// ListEvents returns all stored events for a session, ordered chronologically.
// Each event's JSON is decorated with an "event_id" field carrying the row ID,
// so clients can track the high-water mark and dedup against SSE replay.
func (s *Store) ListEvents(sessionID string) ([]json.RawMessage, error) {
	rows, err := s.db.Query(
		`SELECT id, data FROM events WHERE session_id=? ORDER BY id ASC`,
		sessionID,
	)
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
func (s *Store) ListEventsSinceID(sessionID string, afterID int) ([]json.RawMessage, error) {
	rows, err := s.db.Query(
		`SELECT id, data FROM events WHERE session_id=? AND id > ? ORDER BY id ASC`,
		sessionID, afterID,
	)
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
