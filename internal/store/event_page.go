package store

import (
	"database/sql"
	"time"
)

// EventRow is a single stored event with its row id and type, used by the
// The chat page's turn-model materializer. Unlike ListEvents (which injects event_id
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
	return s.eventPage(sessionID, limitTurns, before, maxEventsPerPage)
}

// eventPage is EventPage with the event cap as a parameter, so a test can reach
// the cap with a handful of rows instead of five thousand.
func (s *Store) eventPage(sessionID string, limitTurns int, before int64, cap int) ([]EventRow, bool, error) {
	if limitTurns <= 0 {
		limitTurns = 30
	}

	start, err := s.turnWindowStart(sessionID, limitTurns, before)
	if err != nil {
		return nil, false, err
	}
	floor, err := s.eventFloorStart(sessionID, cap, before)
	if err != nil {
		return nil, false, err
	}
	if floor > start {
		// The cap is a count of rows and knows nothing about turns, so left alone it
		// cuts wherever the arithmetic lands — inside a turn, and in the worst case
		// between a prompt and its OTel echo (~20ms, a few rows apart). That worst
		// case is not rare: the floor slides forward one row per event, so on a long
		// live session it passes through EVERY prompt pair in turn, and a page fetched
		// at that moment opens on the echo with the prompt itself just outside
		// (observed live 2026-09-02, br_1788370653337509270, 6224 events). So the
		// floor snaps UP to the first turn boundary at or past it: the page holds
		// whole turns only, and a turn the cap would have cut in half is left out
		// rather than served in pieces. Only a session whose newest turn alone
		// exceeds the cap has no boundary to snap to, and then the raw floor stands.
		snapped, err := s.turnBoundaryAtOrAfter(sessionID, floor, before)
		if err != nil {
			return nil, false, err
		}
		if snapped > 0 {
			start = snapped
		} else {
			start = floor
		}
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
	// ONE boundary per turn, not one per user_message row. A prompt is stored twice —
	// the bridge's copy and Claude Code's OTel echo of it, ~20ms later, both
	// type='user_message' and both carrying the same turn_id. Counting rows put a
	// second boundary inside every turn, so a window of N "turns" covered N/2 real
	// ones and its floor could land ON the echo: the page then opened with the echo
	// as its only copy of the prompt, unpaired and primary, while the bridge's copy
	// sat just outside the window. chat-core kept its own live copy of that prompt
	// (the page never reported it) and ordered it after the page's entries, so the
	// user saw their message once before the answer and once after (observed live
	// 2026-09-02, br_1788370653337509270). The boundary is the FIRST row of each
	// turn; a row with no turn_id is its own turn, which is what it always was.
	q := `SELECT MIN(id) AS id FROM events WHERE session_id=? AND type='user_message'`
	args := []any{sessionID}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` GROUP BY COALESCE(NULLIF(json_extract(data, '$.turn_id'), ''), CAST(id AS TEXT))`
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

// turnBoundaryAtOrAfter returns the smallest per-turn boundary (the first
// user_message row of a turn, see turnWindowStart) whose id is >= floor and, when
// before > 0, < before. Returns 0 when no whole turn starts in that range.
func (s *Store) turnBoundaryAtOrAfter(sessionID string, floor, before int64) (int64, error) {
	q := `SELECT MIN(id) AS id FROM events WHERE session_id=? AND type='user_message'`
	args := []any{sessionID}
	if before > 0 {
		q += ` AND id < ?`
		args = append(args, before)
	}
	q += ` GROUP BY COALESCE(NULLIF(json_extract(data, '$.turn_id'), ''), CAST(id AS TEXT))`
	q += ` HAVING MIN(id) >= ? ORDER BY id ASC LIMIT 1`
	args = append(args, floor)
	var id sql.NullInt64
	if err := s.reader.QueryRow(q, args...).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	if !id.Valid {
		return 0, nil
	}
	return id.Int64, nil
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
