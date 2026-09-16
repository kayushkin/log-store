package store

import (
	"fmt"
	"time"
)

// EventRow is a single stored event with its row id and type, as the turn-model
// builder reads it. Unlike ListEvents (which injects event_id into the JSON body),
// this keeps the id separate so the builder can set Entry.eventId without
// re-parsing.
type EventRow struct {
	ID   int64
	Type string
	Data []byte

	// TurnID is the turn the turn index assigned this event, and the builder
	// groups by it. Empty only on rows a test builds by hand, where the builder
	// applies the turn rule itself.
	TurnID string
}

type SessionValidator struct {
	MaxEventID int64
	EventCount int
	// UpdatedAt is the created_at of the newest event, in UTC. Zero when the
	// session has no events.
	UpdatedAt time.Time
}

// Validators returns the validator for each requested session id. Sessions with
// no events are returned with a zero-value validator (MaxEventID 0, EventCount
// 0) rather than omitted, so the caller can distinguish "known empty" from
// "unknown" if it needs to.
func (s *Store) Validators(ids []string) (map[string]SessionValidator, error) {
	out := make(map[string]SessionValidator, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		v, err := s.validator(id)
		if err != nil {
			return nil, err
		}
		out[id] = v
	}
	return out, nil
}

// validator reads a session's validator from its turn index: the newest event is
// the largest last_event_id and the count is the sum over its turns. Counting the
// events themselves cost ~100 ms on a 25,000-event session and ran on every page.
// A session whose index is not current is indexed first.
func (s *Store) validator(id string) (SessionValidator, error) {
	if err := s.BuildTurnIndex(id); err != nil {
		return SessionValidator{}, fmt.Errorf("index turns of %s: %w", id, err)
	}
	var maxID, count int64
	if err := s.reader.QueryRow(
		`SELECT COALESCE(MAX(last_event_id), 0), COALESCE(SUM(event_count), 0) FROM turns WHERE session_id=?`, id,
	).Scan(&maxID, &count); err != nil {
		return SessionValidator{}, err
	}
	v := SessionValidator{MaxEventID: maxID, EventCount: int(count)}
	if maxID == 0 {
		return v, nil
	}
	// created_at is declared DATETIME, so the driver returns it parsed.
	var t time.Time
	if err := s.reader.QueryRow(`SELECT created_at FROM events WHERE id=?`, maxID).Scan(&t); err != nil {
		return SessionValidator{}, fmt.Errorf("newest event of %s: %w", id, err)
	}
	v.UpdatedAt = t.UTC()
	return v, nil
}
