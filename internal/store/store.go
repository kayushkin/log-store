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
func (s *Store) ListEvents(sessionID string) ([]json.RawMessage, error) {
	rows, err := s.db.Query(
		`SELECT data FROM events WHERE session_id=? ORDER BY id ASC`,
		sessionID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []json.RawMessage
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		events = append(events, json.RawMessage(data))
	}
	return events, rows.Err()
}

// ListEventsSinceID returns events after a specific row ID.
func (s *Store) ListEventsSinceID(sessionID string, afterID int) ([]json.RawMessage, error) {
	rows, err := s.db.Query(
		`SELECT data FROM events WHERE session_id=? AND id > ? ORDER BY id ASC`,
		sessionID, afterID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []json.RawMessage
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		events = append(events, json.RawMessage(data))
	}
	return events, rows.Err()
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
