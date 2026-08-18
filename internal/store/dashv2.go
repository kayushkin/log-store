package store

import (
	"database/sql"
	"time"
)

// EventRow is a single stored event with its row id and type, used by the
// dashv2 turn-model materializer. Unlike ListEvents (which injects event_id
// into the JSON body), this keeps the id separate so the materializer can set
// Entry.eventId without re-parsing.
type EventRow struct {
	ID   int64
	Type string
	Data []byte
}

// maxEventsPerPage is an absolute safety cap on how many raw events a single
// EventPage call may return, independent of the turn count. It exists so the
// materialized-messages path can NEVER be unbounded: the legacy /messages code
// materialized 306MB / 85s for one pathological session, and the turn-window
// start alone does not bound event volume for a session with very few turn
// boundaries but a huge event stream. The floor guarantees a hard ceiling.
const maxEventsPerPage = 5000

// EventPage returns the events for the last `limitTurns` turns of a session
// (a turn boundary is a user_message event), or — when before > 0 — the page of
// turns immediately older than the event whose row id is `before`. It also
// reports whether any older events remain (`more`), so the client knows it can
// paginate further back with before=<first returned event id>.
//
// The window is bounded two ways, whichever is tighter: by turn count and by
// maxEventsPerPage raw events. Ordering and cursoring reuse the events.id
// primary key, which is already indexed — the same monotonic row id the
// /events?after=N path uses.
func (s *Store) EventPage(sessionID string, limitTurns int, before int64) ([]EventRow, bool, error) {
	if limitTurns <= 0 {
		limitTurns = 30
	}

	start, err := s.turnWindowStart(sessionID, limitTurns, before)
	if err != nil {
		return nil, false, err
	}
	floor, err := s.eventFloorStart(sessionID, maxEventsPerPage, before)
	if err != nil {
		return nil, false, err
	}
	if floor > start {
		start = floor
	}

	q := `SELECT id, type, data FROM events WHERE session_id=? AND id >= ?`
	args := []any{sessionID, start}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` ORDER BY id ASC`
	rows, err := s.reader.Query(q, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	var out []EventRow
	for rows.Next() {
		var r EventRow
		var data string
		if err := rows.Scan(&r.ID, &r.Type, &data); err != nil {
			return nil, false, err
		}
		r.Data = []byte(data)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	// Older content remains iff any event exists strictly before the window
	// start. This is correct regardless of why `start` was chosen (turn count
	// or the event floor).
	more := false
	if start > 0 {
		if err := s.reader.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM events WHERE session_id=? AND id < ?)`,
			sessionID, start,
		).Scan(&more); err != nil {
			return nil, false, err
		}
	}
	return out, more, nil
}

// turnWindowStart returns the row id of the first event that belongs to the
// window of the last `limitTurns` user-message-delimited turns (older than
// `before` when before > 0). Returns 0 when the session has no user_message
// boundaries at all, so the caller falls back to the event floor.
func (s *Store) turnWindowStart(sessionID string, limitTurns int, before int64) (int64, error) {
	q := `SELECT id FROM events WHERE session_id=? AND type='user_message'`
	args := []any{sessionID}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limitTurns)
	rows, err := s.reader.Query(q, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}
	// ids are DESC; the oldest of the last `limitTurns` boundaries is the
	// window start.
	return ids[len(ids)-1], nil
}

// eventFloorStart returns the row id of the cap-th newest event (older than
// `before` when set), i.e. the lowest id we may include without exceeding the
// hard event ceiling. Returns 0 when the session has cap or fewer events.
func (s *Store) eventFloorStart(sessionID string, cap int, before int64) (int64, error) {
	q := `SELECT id FROM events WHERE session_id=?`
	args := []any{sessionID}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` ORDER BY id DESC LIMIT 1 OFFSET ?`
	args = append(args, cap-1)
	var id int64
	err := s.reader.QueryRow(q, args...).Scan(&id)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return id, nil
}

// SessionValidator is the cheap staleness currency for one session: the highest
// event row id, the total event count, and the timestamp of the newest event.
// The client compares these against a cached model's validator to decide, without
// shipping any messages, whether the cache is fresh. Every field derives from the
// log-store events table — the single source of truth for event row ids — so the
// maxEventId here is directly comparable to Entry.eventId in the materialized
// turn model.
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
		var maxID sql.NullInt64
		var count int
		var updated sql.NullString
		err := s.reader.QueryRow(
			`SELECT COALESCE(MAX(id), 0), COUNT(*), MAX(created_at) FROM events WHERE session_id=?`,
			id,
		).Scan(&maxID, &count, &updated)
		if err != nil {
			return nil, err
		}
		v := SessionValidator{MaxEventID: maxID.Int64, EventCount: count}
		if updated.Valid && updated.String != "" {
			// events.created_at is always written by CURRENT_TIMESTAMP, so its
			// format is the fixed SQLite datetime shape in UTC.
			if t, perr := time.ParseInLocation("2006-01-02 15:04:05", updated.String, time.UTC); perr == nil {
				v.UpdatedAt = t
			}
		}
		out[id] = v
	}
	return out, nil
}
