package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kayushkin/log-store/internal/store"
)

// newTestServer wires a Server over a throwaway store. No forwarder: none of
// the read paths touch it, and a nil one keeps logstack out of a unit test.
func newTestServer(t *testing.T) (*Server, *store.Store) {
	t.Helper()
	s, err := store.New(filepath.Join(t.TempDir(), "log-store.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return New(s, nil), s
}

func harnessBody(t *testing.T, harnessSessionID string) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"harness_session_id": harnessSessionID,
		"harness":            "claude_code",
		"text":               "hello",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return body
}

type byHarnessIDResponse struct {
	HarnessSessionID string              `json:"harness_session_id"`
	Sessions         []store.HeldSession `json:"sessions"`
}

func getByHarnessID(t *testing.T, srv *Server, query string) (*httptest.ResponseRecorder, byHarnessIDResponse) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/by-harness-id"+query, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	var out byHarnessIDResponse
	if rec.Code == http.StatusOK {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("decode body %q: %v", rec.Body.String(), err)
		}
	}
	return rec, out
}

// The route has to answer over HTTP, not just in the store: the only caller
// that matters is in another process.
func TestSessionsByHarnessIDOverHTTP(t *testing.T) {
	srv, st := newTestServer(t)
	st.StoreEvent("br_held", "user_message", harnessBody(t, "cc-uuid-held"))
	st.StoreEvent("br_held", "assistant", harnessBody(t, "cc-uuid-held"))
	st.StoreEvent("br_other", "user_message", harnessBody(t, "cc-uuid-other"))

	rec, out := getByHarnessID(t, srv, "?harness_session_id=cc-uuid-held")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if out.HarnessSessionID != "cc-uuid-held" {
		t.Errorf("echoed id = %q, want cc-uuid-held", out.HarnessSessionID)
	}
	if len(out.Sessions) != 1 || out.Sessions[0].SessionID != "br_held" {
		t.Fatalf("sessions = %+v, want one br_held", out.Sessions)
	}
	if out.Sessions[0].EventCount != 2 {
		t.Errorf("event_count = %d, want 2", out.Sessions[0].EventCount)
	}
}

// A harness session log-store has never seen is a 200 with an empty list, and
// the list must marshal as [] rather than null — a caller checking length on
// null still works, but one that iterates a decoded field does not.
func TestSessionsByHarnessIDUnknownIsEmptyList(t *testing.T) {
	srv, st := newTestServer(t)
	st.StoreEvent("br_held", "user_message", harnessBody(t, "cc-uuid-held"))

	rec, out := getByHarnessID(t, srv, "?harness_session_id=cc-uuid-never-seen")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if len(out.Sessions) != 0 {
		t.Errorf("sessions = %+v, want none", out.Sessions)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode raw: %v", err)
	}
	if string(raw["sessions"]) != "[]" {
		t.Errorf("sessions marshalled as %s, want []", raw["sessions"])
	}
}

// A missing id is a 400. Answering it as "no match" would tell an importer to
// import, which is the behaviour this endpoint exists to stop; answering it as
// a lookup for '' would report every id-less transcript on the host.
func TestSessionsByHarnessIDRequiresTheID(t *testing.T) {
	srv, st := newTestServer(t)
	st.StoreEvent("br_anonymous", "user_message", []byte(`{"text":"no harness id here"}`))

	for _, query := range []string{"", "?harness_session_id="} {
		rec, _ := getByHarnessID(t, srv, query)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("GET %q status = %d, want 400: %s", query, rec.Code, rec.Body.String())
		}
	}
}

// Registering the route as /sessions/by-harness-id/{id} would overlap
// /sessions/{id}/messages and panic ServeMux at construction. New() running at
// all is the assertion; the sibling routes still resolving is the rest of it.
func TestRoutesDoNotCollide(t *testing.T) {
	srv, st := newTestServer(t)
	st.StoreEvent("br_held", "user_message", harnessBody(t, "cc-uuid-held"))

	for _, path := range []string{
		"/api/v1/sessions/br_held/messages",
		"/api/v1/sessions/br_held/history",
		"/api/v1/sessions/br_held/turn-state",
		"/api/v1/sessions/search?q=hello",
		"/api/v1/sessions/by-harness-id?harness_session_id=cc-uuid-held",
	} {
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200: %s", path, rec.Code, rec.Body.String())
		}
	}
}
