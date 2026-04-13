package logstack

import (
	"log"
	"time"

	logstackclient "github.com/kayushkin/logstack/client"
	"github.com/kayushkin/logstack/models"
	"github.com/kayushkin/llm-bridge/msg"
)

// Forwarder sends summarized stats to logstack on result events.
type Forwarder struct {
	client *logstackclient.Client
}

func NewForwarder(logstackURL string) *Forwarder {
	return &Forwarder{client: logstackclient.New(logstackURL)}
}

// Forward sends a result event's stats to logstack asynchronously.
func (f *Forwarder) Forward(ev msg.Event) {
	if ev.Result == nil {
		return
	}
	go func() {
		entry := buildLogEntry(ev)
		if err := f.client.Log(entry); err != nil {
			log.Printf("[logstack-forward] failed: %v", err)
		}
	}()
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

	return models.LogEntry{
		ID:           ev.SessionID + "-" + ts.Format("20060102T150405"),
		Timestamp:    ts,
		Orchestrator: "llm-bridge",
		Agent:        string(ev.Harness),
		SessionID:    ev.SessionID,
		Level:        models.LevelInfo,
		Type:         "outbound",
		Content:      map[string]interface{}{"text": r.Text},
		Stats:        stats,
	}
}
