package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/kayushkin/llm-bridge/msg"
	ls "github.com/kayushkin/log-store/internal/logstack"
	"github.com/kayushkin/log-store/internal/store"
)

// serverWithForwarder wires a Server whose forwarder points at logstackURL.
func serverWithForwarder(t *testing.T, logstackURL string) (*Server, *ls.Forwarder) {
	t.Helper()
	s, err := store.New(filepath.Join(t.TempDir(), "log-store.db"))
	if err != nil {
		t.Fatalf("store.New: %v", err)
	}
	t.Cleanup(func() { s.Close() })

	f := ls.NewForwarder(logstackURL)
	return New(s, f), f
}

func getHealth(t *testing.T, srv *Server) (int, map[string]any) {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decoding /health: %v (body %q)", err, rec.Body.String())
	}
	return rec.Code, body
}

func resultEvent() msg.Event {
	return msg.Event{
		Type:            msg.EventResult,
		Harness:         msg.HarnessClaudeCode,
		BridgeSessionID: "s1",
		Result:          &msg.ResultEvent{Text: "done"},
	}
}

// fakeLogstackReturning serves the client's POST route with a fixed status.
func fakeLogstackReturning(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/logs" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestHealthGoesDegradedWhenForwardingIsBroken is the endpoint half of the
// defect: log-store answered {"status":"ok"} for five weeks while dropping
// every result event it was given.
func TestHealthGoesDegradedWhenForwardingIsBroken(t *testing.T) {
	logstack := fakeLogstackReturning(t, http.StatusNotFound)
	srv, f := serverWithForwarder(t, logstack.URL)

	f.Forward(resultEvent())
	f.Wait()

	code, body := getHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("/health returned %d; a degraded service still answers", code)
	}
	if body["status"] != "degraded" {
		t.Fatalf("status = %v, want \"degraded\" — /health still says ok while every event is dropped", body["status"])
	}

	fwd, ok := body["logstack_forwarding"].(map[string]any)
	if !ok {
		t.Fatalf("/health carries no logstack_forwarding block: %v", body)
	}
	if fwd["target_url"] != logstack.URL {
		t.Fatalf("target_url = %v, want %q — a reader cannot fix a URL the report does not name", fwd["target_url"], logstack.URL)
	}
	if fwd["failed"] != float64(1) {
		t.Fatalf("failed = %v, want 1", fwd["failed"])
	}
	if fwd["consecutive_failures"] != float64(1) {
		t.Fatalf("consecutive_failures = %v, want 1", fwd["consecutive_failures"])
	}
	if fwd["last_error"] == nil || fwd["last_error"] == "" {
		t.Fatal("last_error is empty on a failing forwarder")
	}
	if fwd["last_success_at"] != nil {
		t.Fatalf("last_success_at = %v on a forwarder that never succeeded", fwd["last_success_at"])
	}
}

func TestHealthIsOKWhenForwardingWorks(t *testing.T) {
	logstack := fakeLogstackReturning(t, http.StatusOK)
	srv, f := serverWithForwarder(t, logstack.URL)

	f.Forward(resultEvent())
	f.Wait()

	_, body := getHealth(t, srv)
	if body["status"] != "ok" {
		t.Fatalf("status = %v, want \"ok\" — a working forwarder must not report degraded", body["status"])
	}

	fwd := body["logstack_forwarding"].(map[string]any)
	if fwd["succeeded"] != float64(1) {
		t.Fatalf("succeeded = %v, want 1", fwd["succeeded"])
	}
	if fwd["last_success_at"] == nil {
		t.Fatal("last_success_at is null after a successful forward")
	}
}

func TestHealthIsOKBeforeAnythingHasBeenForwarded(t *testing.T) {
	srv, _ := serverWithForwarder(t, "http://127.0.0.1:1")

	_, body := getHealth(t, srv)
	if body["status"] != "ok" {
		t.Fatalf("status = %v on a freshly started process, want \"ok\"", body["status"])
	}
}

// TestHealthAnswersWithoutAForwarder pins the nil receiver. newTestServer
// builds a Server with no forwarder, so a Health() that assumed one would turn
// /health into a panic for every caller of that helper.
func TestHealthAnswersWithoutAForwarder(t *testing.T) {
	srv, _ := newTestServer(t)

	code, body := getHealth(t, srv)
	if code != http.StatusOK {
		t.Fatalf("/health returned %d with no forwarder configured", code)
	}
	if body["status"] != "ok" {
		t.Fatalf("status = %v, want \"ok\"", body["status"])
	}
	fwd := body["logstack_forwarding"].(map[string]any)
	if fwd["target_url"] != "" {
		t.Fatalf("target_url = %v, want \"\" — an empty target is how a reader tells there is no forwarder", fwd["target_url"])
	}
}
