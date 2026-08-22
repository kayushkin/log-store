package logstack

import (
	"log"
	"sync"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
	logstackclient "github.com/kayushkin/logstack/client"
	"github.com/kayushkin/logstack/models"
)

// ForwardingHealth is a point-in-time snapshot of how the forwarder is faring
// against its configured logstack. It exists because a forwarder that fails
// every single send is otherwise invisible outside the journal: the failures
// are logged one line at a time, nothing counts them, and no endpoint reports
// them. Between 2026-07-15 and 2026-08-22 this forwarder POSTed 287,106 result
// events at the wrong port and not one of them landed.
type ForwardingHealth struct {
	// TargetURL is the logstack the forwarder was built to talk to. Reported
	// because the failure this type exists to surface was a misconfigured
	// URL, and a count of failures without the address is half an answer.
	TargetURL string `json:"target_url"`

	Attempted uint64 `json:"attempted"`
	Succeeded uint64 `json:"succeeded"`
	Failed    uint64 `json:"failed"`

	// ConsecutiveFailures counts failures since the last success. It resets
	// to zero on any success.
	ConsecutiveFailures uint64 `json:"consecutive_failures"`

	// LastError is the error text of the most recent failure, verbatim.
	LastError   string     `json:"last_error,omitempty"`
	LastErrorAt *time.Time `json:"last_error_at"`

	LastSuccessAt *time.Time `json:"last_success_at"`
}

// Degraded reports whether forwarding is currently broken.
//
// The predicate is deliberately "the most recent attempt failed" rather than a
// threshold of N failures: a threshold is an arbitrary number that has to be
// defended, whereas the last attempt having failed is simply what "not working
// right now" means. A single transient failure clears itself on the next
// success, so this does not latch.
//
// A forwarder that has never been asked to forward anything is not degraded —
// it is idle, and reporting an idle forwarder as broken would cry wolf on every
// freshly started process.
func (h ForwardingHealth) Degraded() bool {
	return h.ConsecutiveFailures > 0
}

// Forwarder sends summarized stats to logstack on result events.
type Forwarder struct {
	client    *logstackclient.Client
	targetURL string

	// inFlight tracks the goroutines Forward spawns so callers can wait for
	// them. Forward is asynchronous by design — ingest must not block on
	// logstack — which leaves no other way to observe that a send finished.
	inFlight sync.WaitGroup

	mu                  sync.Mutex
	attempted           uint64
	succeeded           uint64
	failed              uint64
	consecutiveFailures uint64
	lastError           string
	lastErrorAt         time.Time
	lastSuccessAt       time.Time
}

func NewForwarder(logstackURL string) *Forwarder {
	return &Forwarder{
		client:    logstackclient.New(logstackURL),
		targetURL: logstackURL,
	}
}

// Forward sends a result event's stats to logstack asynchronously.
func (f *Forwarder) Forward(ev msg.Event) {
	if ev.Result == nil {
		return
	}
	f.inFlight.Add(1)
	go func() {
		defer f.inFlight.Done()
		entry := buildLogEntry(ev)
		if err := f.client.Log(entry); err != nil {
			log.Printf("[logstack-forward] failed: %v", err)
			f.recordFailure(err)
			return
		}
		f.recordSuccess()
	}()
}

// Wait blocks until every forward already handed to Forward has finished.
//
// Without it the only way to observe an asynchronous send is to sleep, which
// makes a test either slow or flaky and cannot distinguish "not finished yet"
// from "never ran".
func (f *Forwarder) Wait() {
	f.inFlight.Wait()
}

// Health returns a snapshot of the forwarder's recent record.
//
// A nil receiver answers a zero snapshot rather than panicking. cmd/log-store
// always configures a forwarder, so nil means a Server was assembled without
// one — and an empty TargetURL in the reply says exactly that, which is more
// use to a reader than a crashed health endpoint.
func (f *Forwarder) Health() ForwardingHealth {
	if f == nil {
		return ForwardingHealth{}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	h := ForwardingHealth{
		TargetURL:           f.targetURL,
		Attempted:           f.attempted,
		Succeeded:           f.succeeded,
		Failed:              f.failed,
		ConsecutiveFailures: f.consecutiveFailures,
		LastError:           f.lastError,
	}
	if !f.lastErrorAt.IsZero() {
		at := f.lastErrorAt
		h.LastErrorAt = &at
	}
	if !f.lastSuccessAt.IsZero() {
		at := f.lastSuccessAt
		h.LastSuccessAt = &at
	}
	return h
}

func (f *Forwarder) recordFailure(err error) {
	f.mu.Lock()
	f.attempted++
	f.failed++
	f.consecutiveFailures++
	f.lastError = err.Error()
	f.lastErrorAt = time.Now()
	firstOfARun := f.consecutiveFailures == 1
	f.mu.Unlock()

	// The per-failure line above is one of hundreds of thousands of identical
	// lines and reads as background noise. This one fires only when working
	// forwarding stops working, so it is the line worth grepping for, and it
	// names the target because a misconfigured URL is what it is most likely
	// to be reporting.
	if firstOfARun {
		log.Printf("[logstack-forward] STARTED FAILING: target=%s err=%v — every result event is now being dropped", f.targetURL, err)
	}
}

func (f *Forwarder) recordSuccess() {
	f.mu.Lock()
	f.attempted++
	f.succeeded++
	recovered := f.consecutiveFailures
	f.consecutiveFailures = 0
	f.lastSuccessAt = time.Now()
	f.mu.Unlock()

	if recovered > 0 {
		log.Printf("[logstack-forward] RECOVERED: target=%s after %d consecutive failures", f.targetURL, recovered)
	}
}

func buildLogEntry(ev msg.Event) models.LogEntry {
	r := ev.Result

	stats := &models.TurnStats{
		InputTokens:         r.Usage.InputTokens,
		OutputTokens:        r.Usage.OutputTokens,
		CacheReadTokens:     r.Usage.CacheReadTokens,
		CacheCreationTokens: r.Usage.CacheWriteTokens,
		ContextTokens:       r.Usage.ContextTokens,
		ContextLimit:        r.Usage.ContextLimit,
		DurationMs:          r.DurationMS,
		DurationAPIMs:       r.DurationAPIMS,
		NumTurns:            r.NumTurns,
		Model:               r.Model,
		APICalls:            r.APICalls,
	}
	if r.Cost != nil {
		stats.Cost = r.Cost.TotalUSD
	}
	for _, u := range r.APICallUsages {
		stats.APICallUsages = append(stats.APICallUsages, models.APICallUsage{
			InputTokens:      u.InputTokens,
			OutputTokens:     u.OutputTokens,
			CacheReadTokens:  u.CacheReadTokens,
			CacheWriteTokens: u.CacheWriteTokens,
		})
	}
	for _, t := range r.ToolEvents {
		stats.Tools = append(stats.Tools, models.ToolEvent{
			Tool:       t.Tool,
			ToolInput:  t.Input,
			ToolOutput: t.Output,
		})
	}
	stats.ToolCalls = len(stats.Tools)

	ts := ev.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}

	sid := ev.BridgeSessionID
	return models.LogEntry{
		ID:           sid + "-" + ts.Format("20060102T150405"),
		Timestamp:    ts,
		Orchestrator: "llm-bridge",
		Agent:        string(ev.Harness),
		SessionID:    sid,
		Level:        models.LevelInfo,
		Type:         "outbound",
		Content:      map[string]interface{}{"text": r.Text},
		Stats:        stats,
	}
}
