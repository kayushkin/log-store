package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kayushkin/log-store/internal/store"
	"github.com/kayushkin/llm-bridge/msg"
)

// mkRow marshals a msg.Event into a store.EventRow with the given row id.
func mkRow(t *testing.T, id int64, ev msg.Event) store.EventRow {
	t.Helper()
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return store.EventRow{ID: id, Type: string(ev.Type), Data: data}
}

func otelExt() map[string]json.RawMessage {
	return map[string]json.RawMessage{"source": json.RawMessage(`"otel"`)}
}

// Every stored event must become exactly one Entry — nothing dropped (D9).
func TestBuildTurnModel_EmitsEveryEvent(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "turn1", Timestamp: base, Result: &msg.ResultEvent{Text: "hello"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventStream, TurnID: "turn1", Timestamp: base.Add(time.Second), Stream: &msg.HarnessStream{Delta: &msg.BlockDelta{Text: "hi"}}}),
		mkRow(t, 3, msg.Event{Type: msg.EventToolCall, TurnID: "turn1", Timestamp: base.Add(2 * time.Second), ToolCall: &msg.ToolCallEvent{ToolID: "t1", Name: "Bash", Input: json.RawMessage(`{"command":"ls"}`)}}),
		mkRow(t, 4, msg.Event{Type: msg.EventToolResult, TurnID: "turn1", Timestamp: base.Add(3 * time.Second), ToolResult: &msg.ToolResultEvent{ToolID: "t1", Name: "Bash", Output: "file.txt"}}),
		mkRow(t, 5, msg.Event{Type: msg.EventSystem, TurnID: "turn1", Timestamp: base.Add(4 * time.Second), System: &msg.SystemEvent{Subtype: "info"}}),
		mkRow(t, 6, msg.Event{Type: msg.EventResult, TurnID: "turn1", Timestamp: base.Add(5 * time.Second), Result: &msg.ResultEvent{Text: "done"}}),
	}
	m := buildTurnModel("sess", rows, false)

	if len(m.Entries) != len(rows) {
		t.Fatalf("expected %d entries (one per event), got %d", len(rows), len(m.Entries))
	}
	// Every event id must be reconstructable from the entries.
	for _, r := range rows {
		found := false
		for _, e := range m.Entries {
			if e.EventID == r.ID {
				found = true
				if e.Raw == nil {
					t.Errorf("entry for event %d has no raw payload", r.ID)
				}
				break
			}
		}
		if !found {
			t.Errorf("event %d missing from entries — payload is lossy", r.ID)
		}
	}
	if m.SessionID != "sess" {
		t.Errorf("sessionId = %q", m.SessionID)
	}
}

// The tool_call / tool_result / result / thinking atoms are shown in the
// collapsed view; stream deltas and non-boundary system events are retained but
// hidden (duplicate).
func TestBuildTurnModel_CollapsedVisibility(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "q"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventStream, TurnID: "t", Timestamp: base, Stream: &msg.HarnessStream{Delta: &msg.BlockDelta{Text: "partial"}}}),
		mkRow(t, 3, msg.Event{Type: msg.EventSystem, TurnID: "t", Timestamp: base, System: &msg.SystemEvent{Subtype: "info"}}),
		mkRow(t, 4, msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "a"}}),
	}
	m := buildTurnModel("s", rows, false)
	byEvent := map[int64]Entry{}
	for _, e := range m.Entries {
		byEvent[e.EventID] = e
	}
	if byEvent[2].Duplicate != true {
		t.Errorf("stream delta should be duplicate (hidden), got %+v", byEvent[2])
	}
	if byEvent[3].Duplicate != true {
		t.Errorf("non-boundary system should be duplicate (hidden), got %+v", byEvent[3])
	}
	if byEvent[4].Duplicate != false || byEvent[4].Primary != true {
		t.Errorf("result should be visible primary, got %+v", byEvent[4])
	}
	if byEvent[1].Duplicate != false || byEvent[1].Primary != true {
		t.Errorf("user_message should be visible primary, got %+v", byEvent[1])
	}
}

// The core dual-emit case, including the ~1s-late OTel copy landing AFTER the
// assistant reply (never adjacent). The OTel copy must be tagged duplicate and
// share a groupId with the harness copy; the harness copy stays primary. This
// is source+count, never positional.
func TestBuildTurnModel_OTelDualEmit_LateCopy(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		// harness user prompt
		mkRow(t, 10, msg.Event{Type: msg.EventUserMessage, TurnID: "turn", Timestamp: base, Result: &msg.ResultEvent{Text: "what is 2+2?"}}),
		// assistant reply arrives BEFORE the OTel prompt copy flushes
		mkRow(t, 11, msg.Event{Type: msg.EventResult, TurnID: "turn", Timestamp: base.Add(500 * time.Millisecond), Result: &msg.ResultEvent{Text: "4"}}),
		// the ~1s-late OTel copy of the SAME user prompt — distinct turn/message id, no shared id
		mkRow(t, 12, msg.Event{Type: msg.EventUserMessage, TurnID: "otelturn", Timestamp: base.Add(1 * time.Second), Extensions: otelExt(), Result: &msg.ResultEvent{Text: "what is 2+2?"}}),
	}
	m := buildTurnModel("s", rows, false)
	byEvent := map[int64]Entry{}
	for _, e := range m.Entries {
		byEvent[e.EventID] = e
	}
	harness := byEvent[10]
	otel := byEvent[12]
	if harness.Source != sourceHarness {
		t.Errorf("event 10 source = %q, want harness", harness.Source)
	}
	if otel.Source != sourceOTel {
		t.Errorf("event 12 source = %q, want otel", otel.Source)
	}
	if otel.Duplicate != true || otel.Primary != false {
		t.Errorf("late OTel copy should be duplicate/non-primary, got %+v", otel)
	}
	if harness.Duplicate != false || harness.Primary != true {
		t.Errorf("harness copy should be primary, got %+v", harness)
	}
	if harness.GroupID == "" || harness.GroupID != otel.GroupID {
		t.Errorf("harness/otel copies must share a groupId: %q vs %q", harness.GroupID, otel.GroupID)
	}
	// Nothing dropped.
	if len(m.Entries) != 3 {
		t.Errorf("expected 3 entries (all emitted), got %d", len(m.Entries))
	}
}

// A PTY-only / stream-json-dropped turn: only the OTel copy exists. It must
// still render (not duplicate) — that is the recovery.
func TestBuildTurnModel_OTelSurplus_RendersAsRecovery(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 20, msg.Event{Type: msg.EventUserMessage, TurnID: "o", Timestamp: base, Extensions: otelExt(), Result: &msg.ResultEvent{Text: "typed in pty"}}),
	}
	m := buildTurnModel("s", rows, false)
	e := m.Entries["e_20"]
	if e.Duplicate != false || e.Primary != true {
		t.Errorf("sole OTel copy must render (primary, not duplicate), got %+v", e)
	}
	if e.GroupID != "" {
		t.Errorf("singleton OTel copy should have no groupId, got %q", e.GroupID)
	}
}

// A genuine re-send of identical text must still show twice: the extra harness
// copy is surplus and stays visible (count-based, not set-based).
func TestBuildTurnModel_IdenticalResend_ShowsTwice(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "t1", Timestamp: base, Result: &msg.ResultEvent{Text: "again"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventUserMessage, TurnID: "t2", Timestamp: base.Add(time.Minute), Result: &msg.ResultEvent{Text: "again"}}),
		// one OTel copy — absorbs exactly one harness copy
		mkRow(t, 3, msg.Event{Type: msg.EventUserMessage, TurnID: "o", Timestamp: base.Add(time.Minute), Extensions: otelExt(), Result: &msg.ResultEvent{Text: "again"}}),
	}
	m := buildTurnModel("s", rows, false)
	visible := 0
	for _, e := range m.Entries {
		if e.Role == "user" && !e.Duplicate {
			visible++
		}
	}
	if visible != 2 {
		t.Errorf("two genuine harness prompts must stay visible, got %d", visible)
	}
	if !m.Entries["e_3"].Duplicate {
		t.Errorf("the single OTel copy should be absorbed (duplicate)")
	}
}

// Turns group by bridge TurnID; entryIds preserve eventId order.
func TestBuildTurnModel_TurnGrouping(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "A", Timestamp: base, Result: &msg.ResultEvent{Text: "q1"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventResult, TurnID: "A", Timestamp: base.Add(time.Second), Result: &msg.ResultEvent{Text: "a1"}}),
		mkRow(t, 3, msg.Event{Type: msg.EventUserMessage, TurnID: "B", Timestamp: base.Add(2 * time.Second), Result: &msg.ResultEvent{Text: "q2"}}),
		mkRow(t, 4, msg.Event{Type: msg.EventResult, TurnID: "B", Timestamp: base.Add(3 * time.Second), Result: &msg.ResultEvent{Text: "a2"}}),
	}
	m := buildTurnModel("s", rows, true)
	if len(m.Turns) != 2 {
		t.Fatalf("expected 2 turns, got %d", len(m.Turns))
	}
	if m.Turns[0].ID != "A" || len(m.Turns[0].EntryIDs) != 2 {
		t.Errorf("turn A: %+v", m.Turns[0])
	}
	if m.Turns[1].ID != "B" || len(m.Turns[1].EntryIDs) != 2 {
		t.Errorf("turn B: %+v", m.Turns[1])
	}
	if m.Turns[0].EntryIDs[0] != "e_1" || m.Turns[0].EntryIDs[1] != "e_2" {
		t.Errorf("turn A entry order wrong: %v", m.Turns[0].EntryIDs)
	}
	if !m.More {
		t.Errorf("more flag should be threaded through")
	}
}

// Timestamps must serialize as RFC3339 with offset, never naive.
func TestBuildTurnModel_RFC3339Timestamps(t *testing.T) {
	base := time.Date(2026, 7, 27, 14, 3, 11, 0, time.FixedZone("PDT", -7*3600))
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "x"}}),
	}
	m := buildTurnModel("s", rows, false)
	got := m.Entries["e_1"].Ts
	if got != "2026-07-27T14:03:11-07:00" {
		t.Errorf("ts = %q, want RFC3339+offset", got)
	}
}
