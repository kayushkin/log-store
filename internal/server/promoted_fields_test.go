package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/log-store/internal/store"
)

// Four scalars that used to be reachable only by digging back into `raw`.
//
// They are promoted so the default page can stop shipping `raw` at all. Measured
// 2026-08-25 on a real 30-message page: `raw` was 7.82 MB of 9.91 MB — 78.9% — and
// the Turns view never renders a byte of it (dash TurnList.tsx returns null for
// `view !== 'raw'`). Every consumer that reached into it wanted one small scalar.
//
// These cases pin the mapping BEFORE the projection lands. If a field here is wrong
// or empty, dropping `raw` silently breaks the consumer that depends on it — and for
// ToolID that failure is already on record: pairing a call to its result by tool id
// is what fixed history pages rendering every finished tool call as still running.

func TestPromotedFields_ToolIDPairsCallToResult(t *testing.T) {
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	rows := []storeRow{
		{id: 1, ev: msg.Event{Type: msg.EventToolCall, TurnID: "t", Timestamp: base,
			ToolCall: &msg.ToolCallEvent{ToolID: "toolu_01ABC", Name: "Bash", Input: json.RawMessage(`{"command":"ls"}`)}}},
		{id: 2, ev: msg.Event{Type: msg.EventToolResult, TurnID: "t", Timestamp: base.Add(time.Second),
			ToolResult: &msg.ToolResultEvent{ToolID: "toolu_01ABC", Name: "Bash", Output: "file.txt"}}},
	}
	m := buildTurnModel("sess", mkRows(t, rows), false)

	call := entryByEventID(t, m, 1)
	result := entryByEventID(t, m, 2)
	if call.ToolID != "toolu_01ABC" {
		t.Errorf("tool_call toolId = %q, want toolu_01ABC", call.ToolID)
	}
	if result.ToolID != "toolu_01ABC" {
		t.Errorf("tool_result toolId = %q, want toolu_01ABC", result.ToolID)
	}
	// The whole point: the pair is joinable without reading `raw`.
	if call.ToolID != result.ToolID {
		t.Errorf("call and result carry different tool ids — pairing is broken")
	}
}

func TestPromotedFields_ToolErrorDistinguishesFailureFromSilence(t *testing.T) {
	// `false` and "the field was never set" must not be the same answer, which is why
	// the failing case is asserted alongside the succeeding one rather than alone.
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	rows := []storeRow{
		{id: 1, ev: msg.Event{Type: msg.EventToolResult, TurnID: "t", Timestamp: base,
			ToolResult: &msg.ToolResultEvent{ToolID: "a", Name: "Bash", Output: "ok"}}},
		{id: 2, ev: msg.Event{Type: msg.EventToolResult, TurnID: "t", Timestamp: base.Add(time.Second),
			ToolResult: &msg.ToolResultEvent{ToolID: "b", Name: "Bash", Output: "boom", IsError: true}}},
	}
	m := buildTurnModel("sess", mkRows(t, rows), false)

	if entryByEventID(t, m, 1).ToolError {
		t.Errorf("a successful tool result reports toolError = true")
	}
	if !entryByEventID(t, m, 2).ToolError {
		t.Errorf("a failed tool result reports toolError = false")
	}
}

func TestPromotedFields_ClientRequestIDSurvivesToThePage(t *testing.T) {
	// What lets chat-core match an optimistic user row to the real user_message.
	// Without it the correlation falls back to comparing normalized text, which
	// cannot tell two identical prompts apart — send "ok" twice and one of them
	// disappears.
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	rows := []storeRow{
		{id: 1, ev: msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base,
			ClientRequestID: "cr_42", Result: &msg.ResultEvent{Text: "ok"}}},
	}
	m := buildTurnModel("sess", mkRows(t, rows), false)

	if got := entryByEventID(t, m, 1).ClientRequestID; got != "cr_42" {
		t.Errorf("clientRequestId = %q, want cr_42", got)
	}
}

func TestPromotedFields_EventTypeIsCarriedForEveryEvent(t *testing.T) {
	// The terminal-state selector reads it for the event types that have no Kind of
	// their own, so it has to be present on all of them and not just the ones with a
	// dedicated branch in the switch above.
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	rows := []storeRow{
		{id: 1, ev: msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "hi"}}},
		{id: 2, ev: msg.Event{Type: msg.EventToolCall, TurnID: "t", Timestamp: base.Add(time.Second),
			ToolCall: &msg.ToolCallEvent{ToolID: "a", Name: "Bash", Input: json.RawMessage(`{}`)}}},
		{id: 3, ev: msg.Event{Type: msg.EventSystem, TurnID: "t", Timestamp: base.Add(2 * time.Second),
			System: &msg.SystemEvent{Subtype: "info"}}},
		{id: 4, ev: msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base.Add(3 * time.Second),
			Result: &msg.ResultEvent{Text: "done"}}},
	}
	m := buildTurnModel("sess", mkRows(t, rows), false)

	for id, want := range map[int64]string{
		1: string(msg.EventUserMessage),
		2: string(msg.EventToolCall),
		3: string(msg.EventSystem),
		4: string(msg.EventResult),
	} {
		if got := entryByEventID(t, m, id).EventType; got != want {
			t.Errorf("event %d: eventType = %q, want %q", id, got, want)
		}
	}
}

func TestPromotedFields_AreReadableWithoutRaw(t *testing.T) {
	// The acceptance test for the whole exercise: strip `raw` from every entry — which
	// is exactly what the projected page will do — and check that nothing a consumer
	// needs went with it.
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	rows := []storeRow{
		{id: 1, ev: msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base,
			ClientRequestID: "cr_7", Result: &msg.ResultEvent{Text: "run it"}}},
		{id: 2, ev: msg.Event{Type: msg.EventToolCall, TurnID: "t", Timestamp: base.Add(time.Second),
			ToolCall: &msg.ToolCallEvent{ToolID: "toolu_9", Name: "Bash", Input: json.RawMessage(`{"command":"ls"}`)}}},
		{id: 3, ev: msg.Event{Type: msg.EventToolResult, TurnID: "t", Timestamp: base.Add(2 * time.Second),
			ToolResult: &msg.ToolResultEvent{ToolID: "toolu_9", Name: "Bash", Output: "boom", IsError: true}}},
	}
	m := buildTurnModel("sess", mkRows(t, rows), false)
	for id, e := range m.Entries {
		e.Raw = nil
		m.Entries[id] = e
	}

	if got := entryByEventID(t, m, 1).ClientRequestID; got != "cr_7" {
		t.Errorf("clientRequestId lost with raw: %q", got)
	}
	if got := entryByEventID(t, m, 2).ToolID; got != "toolu_9" {
		t.Errorf("tool_call toolId lost with raw: %q", got)
	}
	result := entryByEventID(t, m, 3)
	if result.ToolID != "toolu_9" || !result.ToolError {
		t.Errorf("tool_result toolId/toolError lost with raw: %q / %v", result.ToolID, result.ToolError)
	}
	if got := entryByEventID(t, m, 3).EventType; got != string(msg.EventToolResult) {
		t.Errorf("eventType lost with raw: %q", got)
	}
}

// --- helpers ---

type storeRow struct {
	id int64
	ev msg.Event
}

func mkRows(t *testing.T, rows []storeRow) []store.EventRow {
	t.Helper()
	out := make([]store.EventRow, 0, len(rows))
	for _, r := range rows {
		out = append(out, mkRow(t, r.id, r.ev))
	}
	return out
}

func entryByEventID(t *testing.T, m TurnModel, eventID int64) Entry {
	t.Helper()
	for _, e := range m.Entries {
		if e.EventID == eventID {
			return e
		}
	}
	t.Fatalf("no entry for event %d", eventID)
	return Entry{}
}
