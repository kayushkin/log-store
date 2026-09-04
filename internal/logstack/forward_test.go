package logstack

import (
	"bytes"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
//
// ⚠️ This test previously asserted the OPPOSITE of its last check — that the
// per-failure line still fired once per result, "the existing loudness must not
// be reduced". That was a scope guard: the pass that added the escalated line
// chose to be purely additive. Card 0282145c then asked for the other half in
// so many words — "stop failing quietly: ... should say so once, loudly, not
// once per result forever" — so the per-result line is now gone and its absence
// is what this test pins. The signal it carried is not lost: the target URL and
// the error text moved into the escalated line, the count into Health, and a
// still-broken forwarder resurfaces on repeatSummaryInterval.
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
	if n := strings.Count(buf.String(), "[logstack-forward] failed:"); n != 0 {
		t.Fatalf("per-failure line logged %d times over 4 failures, want 0 — one line per dropped result is the noise that hid the outage", n)
	}
	// Four failures, one line. That ratio is the whole point.
	if n := strings.Count(buf.String(), "\n"); n != 1 {
		t.Fatalf("4 consecutive failures wrote %d log lines, want exactly 1\n%s", n, buf.String())
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

// A forwarder that stays broken must resurface on a timer. Announcing once and
// then going silent makes a two-day outage look identical to a recovery.
func TestAStillFailingForwarderResurfacesOnTheTimer(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(nil); log.SetFlags(log.LstdFlags) })

	srv, _ := fakeLogstack(t, http.StatusNotFound)
	f := NewForwarder(srv.URL)

	clock := time.Date(2026, 9, 4, 20, 0, 0, 0, time.UTC)
	f.now = func() time.Time { return clock }

	for i := 0; i < 5; i++ {
		f.Forward(resultEvent("s1"))
		f.Wait()
	}
	if n := strings.Count(buf.String(), "STILL FAILING"); n != 0 {
		t.Fatalf("roll-up fired %d times inside the window, want 0\n%s", n, buf.String())
	}

	clock = clock.Add(repeatSummaryInterval + time.Second)
	f.Forward(resultEvent("s1"))
	f.Wait()

	if n := strings.Count(buf.String(), "STILL FAILING"); n != 1 {
		t.Fatalf("roll-up fired %d times after the window, want 1\n%s", n, buf.String())
	}
	if !strings.Contains(buf.String(), "6 consecutive results dropped") {
		t.Fatalf("roll-up must carry the running count\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), srv.URL) {
		t.Fatalf("roll-up must name the target\n%s", buf.String())
	}
}

// The exact live misconfiguration: the URL answers, but it is not a logstack.
// bookstack on :8081 returned 404 to every POST for days.
func TestPreflightNamesAURLThatIsNotALogstack(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(nil); log.SetFlags(log.LstdFlags) })

	srv, _ := fakeLogstack(t, http.StatusNotFound)
	NewForwarder(srv.URL).Preflight()

	out := buf.String()
	if !strings.Contains(out, "STARTUP CHECK FAILED") {
		t.Fatalf("want a loud startup failure, got:\n%s", out)
	}
	if !strings.Contains(out, "does not point at a logstack") {
		t.Fatalf("a 404 at startup should name the misconfiguration, got:\n%s", out)
	}
	if !strings.Contains(out, srv.URL) {
		t.Fatalf("want the offending URL, got:\n%s", out)
	}
}

func TestPreflightReportsAnUnreachableTarget(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(nil); log.SetFlags(log.LstdFlags) })

	// Port 1 is reserved and never listening.
	NewForwarder("http://127.0.0.1:1").Preflight()

	out := buf.String()
	if !strings.Contains(out, "STARTUP CHECK FAILED") || !strings.Contains(out, "unreachable") {
		t.Fatalf("want a loud unreachable report, got:\n%s", out)
	}
}

func TestPreflightIsHappyAgainstARealLogstack(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	log.SetFlags(0)
	t.Cleanup(func() { log.SetOutput(nil); log.SetFlags(log.LstdFlags) })

	var probed string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probed = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	NewForwarder(srv.URL).Preflight()

	if probed != "/api/v1/health" {
		t.Fatalf("preflight probed %q, want /api/v1/health", probed)
	}
	if !strings.Contains(buf.String(), "startup check ok") {
		t.Fatalf("want the ok line, got:\n%s", buf.String())
	}
}

// Preflight must never take log-store down: storing events is the primary job
// and forwarding is secondary to it.
func TestPreflightSurvivesAnUnparseableURL(t *testing.T) {
	log.SetOutput(io.Discard)
	t.Cleanup(func() { log.SetOutput(nil) })
	NewForwarder("://not a url").Preflight()
}
