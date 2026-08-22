package logstack

import (
	"bytes"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/kayushkin/llm-bridge/msg"
)

// resultEvent is the only event shape Forward acts on.
func resultEvent(sessionID string) msg.Event {
	return msg.Event{
		Type:            msg.EventResult,
		Harness:         msg.HarnessClaudeCode,
		BridgeSessionID: sessionID,
		Result:          &msg.ResultEvent{Text: "done"},
	}
}

// fakeLogstack serves the one route the client POSTs to, answering with the
// given status, and counts what it received.
func fakeLogstack(t *testing.T, status int) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var received atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/logs" {
			// Answering every path alike would make a forwarder that POSTs
			// to the wrong route look healthy — which is the exact defect
			// this file exists to pin.
			http.NotFound(w, r)
			return
		}
		received.Add(1)
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv, &received
}

// TestTheFakeLogstackActuallyServes is a rig guard. Every test below concludes
// something from what the fake recorded; a fake that never answered would make
// all of them agree for the wrong reason.
func TestTheFakeLogstackActuallyServes(t *testing.T) {
	srv, received := fakeLogstack(t, http.StatusOK)

	resp, err := http.Post(srv.URL+"/api/v1/logs", "application/json", strings.NewReader("{}"))
	if err != nil {
		t.Fatalf("posting to the fake: %v", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("fake answered %d, want 200", resp.StatusCode)
	}
	if got := received.Load(); got != 1 {
		t.Fatalf("fake recorded %d posts, want 1", got)
	}
}

func TestAForwarderNobodyHasAskedToForwardIsNotDegraded(t *testing.T) {
	f := NewForwarder("http://127.0.0.1:1")

	h := f.Health()
	if h.Degraded() {
		t.Fatal("a forwarder with no attempts reports degraded; a freshly booted process would cry wolf")
	}
	if h.Attempted != 0 {
		t.Fatalf("Attempted = %d, want 0", h.Attempted)
	}
	if h.TargetURL != "http://127.0.0.1:1" {
		t.Fatalf("TargetURL = %q, want the configured URL", h.TargetURL)
	}
}

func TestASuccessfulForwardIsCountedAndLeavesTheForwarderHealthy(t *testing.T) {
	srv, received := fakeLogstack(t, http.StatusOK)
	f := NewForwarder(srv.URL)

	f.Forward(resultEvent("s1"))
	f.Wait()

	if got := received.Load(); got != 1 {
		t.Fatalf("logstack received %d entries, want 1 — the forward never reached it", got)
	}
	h := f.Health()
	if h.Attempted != 1 || h.Succeeded != 1 || h.Failed != 0 {
		t.Fatalf("attempted/succeeded/failed = %d/%d/%d, want 1/1/0", h.Attempted, h.Succeeded, h.Failed)
	}
	if h.Degraded() {
		t.Fatal("a forwarder whose only send succeeded reports degraded")
	}
	if h.LastSuccessAt == nil {
		t.Fatal("LastSuccessAt is nil after a success")
	}
	if h.LastErrorAt != nil {
		t.Fatalf("LastErrorAt is set after a success: %v", h.LastErrorAt)
	}
}

// TestAForwarderThatIsDroppingEveryEventReportsItself is the defect this whole
// change exists for: 287,106 events were POSTed at a service that 404s them and
// nothing outside the journal could tell.
func TestAForwarderThatIsDroppingEveryEventReportsItself(t *testing.T) {
	srv, _ := fakeLogstack(t, http.StatusNotFound)
	f := NewForwarder(srv.URL)

	for i := 0; i < 3; i++ {
		f.Forward(resultEvent("s1"))
		f.Wait()
	}

	h := f.Health()
	if !h.Degraded() {
		t.Fatal("three straight failures and Health still reports healthy — this is the silence the change removes")
	}
	if h.Attempted != 3 || h.Succeeded != 0 || h.Failed != 3 {
		t.Fatalf("attempted/succeeded/failed = %d/%d/%d, want 3/0/3", h.Attempted, h.Succeeded, h.Failed)
	}
	if h.ConsecutiveFailures != 3 {
		t.Fatalf("ConsecutiveFailures = %d, want 3", h.ConsecutiveFailures)
	}
	if !strings.Contains(h.LastError, "404") {
		t.Fatalf("LastError = %q, want the verbatim upstream error naming 404", h.LastError)
	}
	if h.LastErrorAt == nil {
		t.Fatal("LastErrorAt is nil after a failure")
	}
	if h.LastSuccessAt != nil {
		t.Fatal("LastSuccessAt is set on a forwarder that has never succeeded")
	}
}

func TestASuccessClearsTheFailureRunButNotTheTotals(t *testing.T) {
	var status atomic.Int64
	status.Store(http.StatusNotFound)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/logs" {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(srv.Close)

	f := NewForwarder(srv.URL)
	f.Forward(resultEvent("s1"))
	f.Wait()
	f.Forward(resultEvent("s2"))
	f.Wait()

	if f.Health().ConsecutiveFailures != 2 {
		t.Fatalf("ConsecutiveFailures = %d before recovery, want 2", f.Health().ConsecutiveFailures)
	}

	status.Store(http.StatusOK)
	f.Forward(resultEvent("s3"))
	f.Wait()

	h := f.Health()
	if h.Degraded() {
		t.Fatal("still degraded after a success — the run never reset, so a recovered forwarder would stay red forever")
	}
	if h.ConsecutiveFailures != 0 {
		t.Fatalf("ConsecutiveFailures = %d after a success, want 0", h.ConsecutiveFailures)
	}
	if h.Failed != 2 {
		t.Fatalf("Failed = %d, want the 2 historical failures kept", h.Failed)
	}
	if h.Succeeded != 1 || h.Attempted != 3 {
		t.Fatalf("succeeded/attempted = %d/%d, want 1/3", h.Succeeded, h.Attempted)
	}
	if h.LastError == "" {
		t.Fatal("LastError was cleared by a success; the last error is history and stays readable")
	}
}

func TestAnEventWithNoResultIsNotAForwardAttempt(t *testing.T) {
	srv, received := fakeLogstack(t, http.StatusOK)
	f := NewForwarder(srv.URL)

	f.Forward(msg.Event{Type: msg.EventStream, BridgeSessionID: "s1"})
	f.Wait()

	if got := received.Load(); got != 0 {
		t.Fatalf("logstack received %d entries for a non-result event, want 0", got)
	}
	if h := f.Health(); h.Attempted != 0 {
		t.Fatalf("Attempted = %d, want 0 — counting skipped events would dilute the failure rate", h.Attempted)
	}
}

// TestOnlyTheStartOfAFailureRunLogsTheEscalatedLine pins the reason the live
// defect went unseen: the per-failure line fired 287,106 times and read as
// background noise. The escalated line has to stay rare enough to be a signal.
func TestOnlyTheStartOfAFailureRunLogsTheEscalatedLine(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(nil); log.SetFlags(log.LstdFlags) })

	srv, _ := fakeLogstack(t, http.StatusNotFound)
	f := NewForwarder(srv.URL)

	for i := 0; i < 4; i++ {
		f.Forward(resultEvent("s1"))
		f.Wait()
	}

	if n := strings.Count(buf.String(), "STARTED FAILING"); n != 1 {
		t.Fatalf("escalated line logged %d times over 4 failures, want exactly 1\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), srv.URL) {
		t.Fatalf("escalated line does not name the target URL; a misconfigured URL is what it most often reports\n%s", buf.String())
	}
	if n := strings.Count(buf.String(), "[logstack-forward] failed:"); n != 4 {
		t.Fatalf("per-failure line logged %d times over 4 failures, want 4 — the existing loudness must not be reduced", n)
	}
}

func TestRecoveryIsLoggedWithTheSizeOfTheOutage(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(nil); log.SetFlags(log.LstdFlags) })

	var status atomic.Int64
	status.Store(http.StatusNotFound)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(int(status.Load()))
	}))
	t.Cleanup(srv.Close)

	f := NewForwarder(srv.URL)
	for i := 0; i < 2; i++ {
		f.Forward(resultEvent("s1"))
		f.Wait()
	}
	status.Store(http.StatusOK)
	f.Forward(resultEvent("s2"))
	f.Wait()

	if n := strings.Count(buf.String(), "RECOVERED"); n != 1 {
		t.Fatalf("recovery logged %d times, want 1\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "after 2 consecutive failures") {
		t.Fatalf("recovery line does not say how many events were lost\n%s", buf.String())
	}

	buf.Reset()
	f.Forward(resultEvent("s3"))
	f.Wait()
	if strings.Contains(buf.String(), "RECOVERED") {
		t.Fatalf("a success that recovered nothing logged RECOVERED\n%s", buf.String())
	}
}
