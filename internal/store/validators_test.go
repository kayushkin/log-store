package store

import (
	"encoding/json"
	"testing"
)

// storeEvent inserts a minimal event of the given type and returns its row id.
func storeEvent(t *testing.T, s *Store, sessionID, typ, text string) int64 {
	t.Helper()
	body, _ := json.Marshal(map[string]any{
		"type":              typ,
		"bridge_session_id": sessionID,
		"result":            map[string]any{"text": text},
	})
	id, err := s.StoreEvent(sessionID, typ, body)
	if err != nil {
		t.Fatalf("StoreEvent: %v", err)
	}
	return id
}

func TestValidators(t *testing.T) {
	s := newTestStore(t)
	a, b := "sa", "sb"
	storeEvent(t, s, a, "user_message", "hi")
	storeEvent(t, s, a, "result", "yo")
	last := storeEvent(t, s, a, "result", "bye")
	// b has no events.
	vs, err := s.Validators([]string{a, b})
	if err != nil {
		t.Fatalf("Validators: %v", err)
	}
	va := vs[a]
	if va.MaxEventID != last {
		t.Errorf("maxEventId = %d, want %d", va.MaxEventID, last)
	}
	if va.EventCount != 3 {
		t.Errorf("eventCount = %d, want 3", va.EventCount)
	}
	if va.UpdatedAt.IsZero() {
		t.Errorf("updatedAt should be set")
	}
	vb := vs[b]
	if vb.MaxEventID != 0 || vb.EventCount != 0 {
		t.Errorf("empty session validator should be zero-value, got %+v", vb)
	}
}
