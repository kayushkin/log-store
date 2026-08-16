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

// recoveredBlock builds a recovered/OTel assistant text block exactly as
// llm-bridge-claudecode handler.go flushRecoveredAssistant emits it: an EventBlock text
// tagged extensions.source="otel", recovered=true, and (like the real emit) NO turn id.
func recoveredBlock(t *testing.T, id int64, ts time.Time, text string) store.EventRow {
	return mkRow(t, id, msg.Event{
		Type:      msg.EventBlock,
		Timestamp: ts,
		Block: &msg.BlockEvent{
			Index: 0,
			Block: &msg.ContentBlock{Type: msg.BlockText, Text: &msg.TextBlock{Text: text}},
		},
		Extensions: map[string]json.RawMessage{
			"source":    json.RawMessage(`"otel"`),
			"recovered": json.RawMessage(`true`),
		},
	})
}

// A1: a turn consisting only of a recovered OTel block must materialize to ONE
// visible (Primary) assistant entry — hiding it would lose the model's only message.
func TestBuildTurnModel_RecoveredOTelBlock_VisiblePrimary(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{recoveredBlock(t, 1, base, "final answer")}
	m := buildTurnModel("s", rows, false)

	visible := 0
	var got Entry
	for _, e := range m.Entries {
		if e.Role == "assistant" && e.Kind == "text" && !e.Duplicate {
			visible++
			got = e
		}
	}
	if visible != 1 {
		t.Fatalf("expected exactly 1 visible assistant entry, got %d", visible)
	}
	if !got.Primary || got.Duplicate {
		t.Errorf("recovered block must be primary/visible, got %+v", got)
	}
	if !got.Recovered {
		t.Errorf("recovered flag must be preserved, got %+v", got)
	}
	if got.Text != "final answer" {
		t.Errorf("text = %q", got.Text)
	}
	if got.Source != sourceOTel {
		t.Errorf("source = %q, want otel", got.Source)
	}
}

// A1 no-regression: when a Result supersedes it in the same turn, a recovered block
// stays hidden (duplicate) — exactly like a streamed block in a healthy turn.
func TestBuildTurnModel_RecoveredBlock_HiddenWhenResultSupersedes(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "turn1", Timestamp: base, Result: &msg.ResultEvent{Text: "q"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventResult, TurnID: "turn1", Timestamp: base.Add(time.Second), Result: &msg.ResultEvent{Text: "streamed answer"}}),
		// recovered block has no turn id → attaches to the open turn1
		recoveredBlock(t, 3, base.Add(2*time.Second), "duplicate-ish"),
	}
	m := buildTurnModel("s", rows, false)
	block := m.Entries["e_3"]
	if !block.Duplicate || block.Primary {
		t.Errorf("superseded recovered block should stay hidden, got %+v", block)
	}
	if !block.Recovered {
		t.Errorf("recovered flag must be preserved even when hidden, got %+v", block)
	}
}

// B2: error entries carry the canonical ErrorEvent fields (code/retryable/statusCode).
func TestBuildTurnModel_ErrorFields(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventError, TurnID: "t", Timestamp: base, Extensions: otelExt(),
			Error: &msg.ErrorEvent{Code: "api_error", Message: "Overloaded", Retryable: true, StatusCode: 529}}),
		mkRow(t, 2, msg.Event{Type: msg.EventError, TurnID: "t", Timestamp: base.Add(time.Second),
			Error: &msg.ErrorEvent{Code: "TURN_IDLE_TIMEOUT", Message: "no output for 5m"}}),
	}
	m := buildTurnModel("s", rows, false)
	api := m.Entries["e_1"]
	if api.Kind != "error" || api.Code != "api_error" || !api.Retryable || api.StatusCode != 529 {
		t.Errorf("api_error entry fields wrong: %+v", api)
	}
	if api.Text != "Overloaded" {
		t.Errorf("error text = %q", api.Text)
	}
	term := m.Entries["e_2"]
	if term.Code != "TURN_IDLE_TIMEOUT" || term.Retryable || term.StatusCode != 0 {
		t.Errorf("TURN_IDLE_TIMEOUT entry fields wrong: %+v", term)
	}
}

// C: a subagent_completed system event is surfaced on the settled path (visible, kind
// "system", carrying subtype), consistent with the live tail — never an error.
func TestBuildTurnModel_SubagentCompleted_Visible(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventSystem, TurnID: "t", Timestamp: base, Extensions: otelExt(),
			System: &msg.SystemEvent{Subtype: "subagent_completed", Message: "code-reviewer"}}),
		// an unknown/bookkeeping subtype stays hidden (meta)
		mkRow(t, 2, msg.Event{Type: msg.EventSystem, TurnID: "t", Timestamp: base.Add(time.Second),
			System: &msg.SystemEvent{Subtype: "mcp_server_connection"}}),
	}
	m := buildTurnModel("s", rows, false)
	sub := m.Entries["e_1"]
	if sub.Kind != "system" || sub.Duplicate || !sub.Primary {
		t.Errorf("subagent_completed should be visible kind system, got %+v", sub)
	}
	if sub.Subtype != "subagent_completed" {
		t.Errorf("subtype = %q, want subagent_completed", sub.Subtype)
	}
	if sub.Role == "assistant" && sub.Kind == "error" {
		t.Errorf("subagent progress must never be an error: %+v", sub)
	}
	if sub.Text != "code-reviewer" {
		t.Errorf("system message text = %q", sub.Text)
	}
	meta := m.Entries["e_2"]
	if !meta.Duplicate {
		t.Errorf("unknown system subtype should stay hidden (meta), got %+v", meta)
	}
}

// TestBuildTurnModel_CarriesSubagentFields is the regression for a settled page
// that knew a task existed and nothing else. Subtype was the only SystemEvent
// field materialized, so on reload every subagent looked permanently in flight:
// no id to name it, no status to close it, no summary to report, and no session
// to follow. The live stream carried all of it; only history threw it away.
func TestBuildTurnModel_CarriesSubagentFields(t *testing.T) {
	base := time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventSystem, TurnID: "t", Timestamp: base,
			System: &msg.SystemEvent{
				Subtype: "task_started", TaskID: "a1", ToolUseID: "toolu_1",
				TaskType: msg.TaskTypeLocalAgent, SubagentType: "Explore",
				SubagentSessionID: "br_sub", Description: "probe",
			}}),
		mkRow(t, 2, msg.Event{Type: msg.EventSystem, TurnID: "t", Timestamp: base.Add(time.Second),
			System: &msg.SystemEvent{
				Subtype: "task_notification", TaskID: "a1", TaskStatus: msg.TaskStatusCompleted,
				TaskSummary: "found it", SubagentSessionID: "br_sub",
			}}),
		// A subagent's own frame, kept on the parent by the server's fail-safe.
		// It must arrive labelled as somebody else's work.
		mkRow(t, 3, msg.Event{Type: msg.EventBlock, TurnID: "t", Timestamp: base.Add(2 * time.Second),
			HarnessParentID: "toolu_1",
			Block:           &msg.BlockEvent{Block: &msg.ContentBlock{Type: msg.BlockText, Text: &msg.TextBlock{Text: "subagent working"}}}}),
	}
	m := buildTurnModel("s", rows, false)

	started := m.Entries["e_1"]
	if started.TaskID != "a1" || started.ToolUseID != "toolu_1" {
		t.Errorf("task correlators lost: %+v", started)
	}
	if started.TaskType != msg.TaskTypeLocalAgent || started.SubagentType != "Explore" {
		t.Errorf("task kind lost: taskType=%q subagentType=%q", started.TaskType, started.SubagentType)
	}
	if started.SubagentSessionID != "br_sub" {
		t.Errorf("subagentSessionId = %q, want br_sub — without it a reloaded page has nothing to link to", started.SubagentSessionID)
	}

	done := m.Entries["e_2"]
	if done.TaskStatus != msg.TaskStatusCompleted {
		t.Errorf("taskStatus = %q, want completed — the only thing that ever says a subagent finished", done.TaskStatus)
	}
	if done.TaskSummary != "found it" {
		t.Errorf("taskSummary = %q, want %q", done.TaskSummary, "found it")
	}

	if got := m.Entries["e_3"].HarnessParentID; got != "toolu_1" {
		t.Errorf("harnessParentId = %q, want toolu_1 — a subagent's own row must say so", got)
	}
	if m.Entries["e_1"].HarnessParentID != "" {
		t.Error("the parent's own narration must not be labelled as a subagent's work")
	}

	// A task event is a SYSTEM event, and the settled page has to say so. It
	// used to answer kind "meta" for every subtype outside the visible list,
	// while the live reducer answered "system" for all of them. Anything asking
	// "is this a system event?" — the timeline, looking for a task's spawn and
	// finish — therefore found task rows on a live stream and none at all after
	// a reload.
	for _, id := range []string{"e_1", "e_2"} {
		if got := m.Entries[id].Kind; got != "system" {
			t.Errorf("%s kind = %q, want system; the live reducer says system for the same event", id, got)
		}
	}
	// Kind is not the visibility switch. These stay out of the COLLAPSED turns
	// view, which reads duplicate, so calling them system costs that view
	// nothing.
	if !m.Entries["e_1"].Duplicate {
		t.Error("a task event must stay out of the collapsed conversation view")
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

// Phase 2: an assistant result carrying usage meta populates Entry.usage,
// mapped from msg.TokenUsage (cacheWriteTokens ← CacheWriteTokens).
func TestBuildTurnModel_EntryUsage_Populated(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "q"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base.Add(time.Second), Result: &msg.ResultEvent{
			Text:  "a",
			Usage: msg.TokenUsage{InputTokens: 100, OutputTokens: 40, CacheReadTokens: 12, CacheWriteTokens: 7},
		}}),
	}
	m := buildTurnModel("s", rows, false)

	// The user prompt has no usage meta → omitted (nil).
	if u := m.Entries["e_1"].Usage; u != nil {
		t.Errorf("user prompt entry should have no usage, got %+v", u)
	}
	got := m.Entries["e_2"].Usage
	if got == nil {
		t.Fatalf("result entry should carry usage")
	}
	if got.InputTokens != 100 || got.OutputTokens != 40 || got.CacheReadTokens != 12 || got.CacheWriteTokens != 7 {
		t.Errorf("usage mapped wrong: %+v", got)
	}

	// It must serialize under the camelCase `usage` key.
	blob, err := json.Marshal(m.Entries["e_2"])
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	var probe struct {
		Usage *EntryUsage `json:"usage"`
	}
	if err := json.Unmarshal(blob, &probe); err != nil {
		t.Fatalf("unmarshal entry: %v", err)
	}
	if probe.Usage == nil || probe.Usage.CacheWriteTokens != 7 {
		t.Errorf("usage did not round-trip under camelCase key: %s", blob)
	}
}

// Phase 2: an all-zero result usage is omitted (nil), not emitted as {}.
func TestBuildTurnModel_EntryUsage_OmittedWhenZero(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "a"}}),
	}
	m := buildTurnModel("s", rows, false)
	if u := m.Entries["e_1"].Usage; u != nil {
		t.Errorf("zero-usage result should omit usage, got %+v", u)
	}
	blob, _ := json.Marshal(m.Entries["e_1"])
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(blob, &raw)
	if _, present := raw["usage"]; present {
		t.Errorf("usage key must be absent for zero usage: %s", blob)
	}
}

// Phase 2: an api_spend_total event + a context-bearing usage_total event
// populate TurnModel.aggregates (totalUsd/byModel/byQuerySource + context state).
func TestBuildTurnModel_Aggregates_Populated(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "q"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base.Add(time.Second), Result: &msg.ResultEvent{Text: "a"}}),
		// latest spend total in the window
		mkRow(t, 3, msg.Event{Type: msg.EventAPISpendTotal, TurnID: "t", Timestamp: base.Add(2 * time.Second), APISpendTotal: &msg.APISpendTotalEvent{
			TotalUSD:      0.0704,
			Calls:         3,
			ByModel:       map[string]float64{"opus": 0.07, "haiku": 0.0004},
			ByQuerySource: map[string]float64{"main": 0.07, "generate_session_title": 0.0004},
		}}),
		// latest context-bearing usage state
		mkRow(t, 4, msg.Event{Type: msg.EventUsageTotal, TurnID: "t", Timestamp: base.Add(3 * time.Second), UsageTotal: &msg.UsageTotalEvent{
			Usage: msg.TokenUsage{ContextTokens: 34000, ContextLimit: 200000},
			Turns: 1,
		}}),
	}
	m := buildTurnModel("s", rows, false)
	agg := m.Aggregates
	if agg == nil {
		t.Fatalf("aggregates should be populated")
	}
	if agg.TotalUSD != 0.0704 {
		t.Errorf("totalUsd = %v, want 0.0704", agg.TotalUSD)
	}
	if agg.ByModel["opus"] != 0.07 || agg.ByModel["haiku"] != 0.0004 {
		t.Errorf("byModel wrong: %+v", agg.ByModel)
	}
	if agg.ByQuerySource["main"] != 0.07 || agg.ByQuerySource["generate_session_title"] != 0.0004 {
		t.Errorf("byQuerySource wrong: %+v", agg.ByQuerySource)
	}
	if agg.ContextTokens != 34000 || agg.ContextLimit != 200000 {
		t.Errorf("context state wrong: tokens=%d limit=%d", agg.ContextTokens, agg.ContextLimit)
	}

	// aggregates must serialize under the camelCase key with camelCase fields.
	blob, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal model: %v", err)
	}
	var probe struct {
		Aggregates *TurnAggregates `json:"aggregates"`
	}
	if err := json.Unmarshal(blob, &probe); err != nil {
		t.Fatalf("unmarshal model: %v", err)
	}
	if probe.Aggregates == nil || probe.Aggregates.TotalUSD != 0.0704 || probe.Aggregates.ContextLimit != 200000 {
		t.Errorf("aggregates did not round-trip under camelCase: %s", blob)
	}
}

// Phase 2: totalUsd falls back to the sum of byModel when TotalUSD is zero but a
// per-model breakdown exists.
func TestBuildTurnModel_Aggregates_TotalUSDFallsBackToByModel(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventAPISpendTotal, TurnID: "t", Timestamp: base, APISpendTotal: &msg.APISpendTotalEvent{
			ByModel: map[string]float64{"opus": 0.05, "haiku": 0.001},
		}}),
	}
	m := buildTurnModel("s", rows, false)
	if m.Aggregates == nil {
		t.Fatalf("aggregates should be populated")
	}
	if got := m.Aggregates.TotalUSD; got < 0.0509 || got > 0.0511 {
		t.Errorf("totalUsd fallback = %v, want ~0.051", got)
	}
}

// Phase 2 no-regression: a session with no spend/usage/context events yields no
// aggregates and no per-entry usage — the field is omitted and the legacy array
// path is byte-identical to before.
func TestBuildTurnModel_Aggregates_OmittedWhenAbsent(t *testing.T) {
	base := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base, Result: &msg.ResultEvent{Text: "q"}}),
		mkRow(t, 2, msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base.Add(time.Second), Result: &msg.ResultEvent{Text: "a"}}),
	}
	m := buildTurnModel("s", rows, false)
	if m.Aggregates != nil {
		t.Errorf("aggregates must be nil when no spend/context events present, got %+v", m.Aggregates)
	}
	// The model must serialize WITHOUT the aggregates key (legacy byte-identity).
	blob, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal model: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(blob, &raw); err != nil {
		t.Fatalf("unmarshal model: %v", err)
	}
	if _, present := raw["aggregates"]; present {
		t.Errorf("aggregates key must be absent in a no-cost session: %s", blob)
	}
	for _, e := range m.Entries {
		if e.Usage != nil {
			t.Errorf("no entry should carry usage in a no-usage session, got %+v", e.Usage)
		}
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

// harnessTextBlock builds an ordinary harness-sourced assistant text block exactly
// as llm-bridge-claudecode forwards one for a SUBAGENT: text under
// block.block.text_block, no turn id, no extensions (so neither recovered nor
// OTel-sourced). Shape taken from a real stored row — session
// br_1786568331006216317, event 1581328 — not from reading the encoder.
func harnessTextBlock(t *testing.T, id int64, ts time.Time, text string) store.EventRow {
	t.Helper()
	return mkRow(t, id, msg.Event{
		Type:            msg.EventBlock,
		Timestamp:       ts,
		HarnessParentID: "toolu_01NP3H6oeGGV3Vowvy3MUhQ1",
		Block: &msg.BlockEvent{
			Index: 0,
			Block: &msg.ContentBlock{Type: "text", Text: &msg.TextBlock{Text: text}},
		},
	})
}

// A subagent session emits no result event of its own — that is the premise of the
// subagent-settling design — so nothing ever supersedes its block text. Marking the
// blocks superseded anyway left the collapsed view with nothing to show, and every
// surface that renders it (the dashv2 timeline's subagent dropdown, chat-core's
// TranscriptBody) reported a session with real prose as empty. 1,445 subagent
// sessions on this host were affected.
func TestBuildTurnModel_BlocksStayVisibleWhenTurnHasNoResult(t *testing.T) {
	base := time.Date(2026, 8, 12, 20, 58, 52, 0, time.UTC)
	rows := []store.EventRow{
		harnessTextBlock(t, 1, base, "I'll run the demo steps in order."),
		mkRow(t, 2, msg.Event{Type: msg.EventToolCall, Timestamp: base.Add(time.Second), ToolCall: &msg.ToolCallEvent{ToolID: "t1", Name: "Bash", Input: json.RawMessage(`{"command":"ls"}`)}}),
		mkRow(t, 3, msg.Event{Type: msg.EventToolResult, Timestamp: base.Add(2 * time.Second), ToolResult: &msg.ToolResultEvent{ToolID: "t1", Name: "Bash", Output: "x.ts"}}),
		harnessTextBlock(t, 4, base.Add(3*time.Second), "BETA finished, counted 34 TypeScript files."),
	}
	m := buildTurnModel("br_sub", rows, false)

	for _, id := range []string{"e_1", "e_4"} {
		e := m.Entries[id]
		if e.Duplicate || !e.Primary {
			t.Errorf("%s (%q) is hidden — a turn with no result has nothing to supersede its blocks; got primary=%v duplicate=%v",
				id, e.Text, e.Primary, e.Duplicate)
		}
	}
	// The collapsed view must actually contain prose, which is the user-visible claim.
	var prose int
	for _, e := range m.Entries {
		if !e.Duplicate && e.Primary && e.Kind == "text" && e.Text != "" {
			prose++
		}
	}
	if prose != 2 {
		t.Errorf("collapsed view shows %d prose entries, want 2", prose)
	}
}

// No-regression, and it is the load-bearing half: every NORMAL session has a result
// per turn, so its blocks must stay hidden exactly as before. This is the assertion
// that stops the fix from doubling the text of all 24,128 turns that do have one.
func TestBuildTurnModel_BlocksStayHiddenWhenResultSupersedes(t *testing.T) {
	base := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventUserMessage, TurnID: "turn1", Timestamp: base, Result: &msg.ResultEvent{Text: "q"}}),
		harnessTextBlock(t, 2, base.Add(time.Second), "the answer"),
		mkRow(t, 3, msg.Event{Type: msg.EventResult, TurnID: "turn1", Timestamp: base.Add(2 * time.Second), Result: &msg.ResultEvent{Text: "the answer"}}),
	}
	m := buildTurnModel("s", rows, false)
	if e := m.Entries["e_2"]; !e.Duplicate || e.Primary {
		t.Errorf("block must stay hidden when its turn carries a result; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
	if e := m.Entries["e_3"]; e.Duplicate || !e.Primary {
		t.Errorf("result must stay visible; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
}

// A result with EMPTY text supersedes nothing a reader can see, so the blocks are
// still the only record of what was said. Follows the precedent already set by
// supersedesRecovered, which requires non-empty text before it calls an entry
// authoritative. Without this the fix would go dark on any turn whose result
// carried no text.
func TestBuildTurnModel_BlocksStayVisibleWhenResultTextIsEmpty(t *testing.T) {
	base := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		harnessTextBlock(t, 1, base, "said out loud"),
		mkRow(t, 2, msg.Event{Type: msg.EventResult, TurnID: "", Timestamp: base.Add(time.Second), Result: &msg.ResultEvent{Text: ""}}),
	}
	m := buildTurnModel("s", rows, false)
	if e := m.Entries["e_1"]; e.Duplicate || !e.Primary {
		t.Errorf("block must stay visible when the turn's result carries no text; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
}

// Stream deltas are FRAGMENTS, not whole messages, so they are deliberately NOT
// promoted: rendering them would put one bubble per delta in the collapsed view.
// Measured over this host's 26,038 stored (session, turn) groups: 1,817 have a block
// and no result, 93 have a stream and no result, and ZERO have both — so scoping the
// promotion to blocks leaves the block population whole. The stream population is
// filed separately rather than guessed at here.
func TestBuildTurnModel_StreamDeltasStayHiddenEvenWithNoResult(t *testing.T) {
	base := time.Date(2026, 4, 13, 6, 8, 30, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{Type: msg.EventStream, Timestamp: base, Stream: &msg.HarnessStream{Delta: &msg.BlockDelta{Text: "Hel"}}}),
		mkRow(t, 2, msg.Event{Type: msg.EventStream, Timestamp: base.Add(time.Second), Stream: &msg.HarnessStream{Delta: &msg.BlockDelta{Text: "lo"}}}),
	}
	m := buildTurnModel("s", rows, false)
	for _, id := range []string{"e_1", "e_2"} {
		if e := m.Entries[id]; !e.Duplicate || e.Primary {
			t.Errorf("%s: a stream delta is a fragment and must stay hidden; got primary=%v duplicate=%v", id, e.Primary, e.Duplicate)
		}
	}
}

// When a turn carries BOTH a harness block and the OTel copy of the same message and
// no result, exactly one of them may render — the harness block. Otherwise the fix
// trades a blank session for a doubled one. A1 already declines to promote the OTel
// copy in this case (a harness block supersedes it); this pins that the two passes
// agree rather than both firing.
func TestBuildTurnModel_HarnessBlockWinsOverOTelCopyWhenNoResult(t *testing.T) {
	base := time.Date(2026, 8, 12, 20, 0, 0, 0, time.UTC)
	rows := []store.EventRow{
		harnessTextBlock(t, 1, base, "the only message"),
		recoveredBlock(t, 2, base.Add(time.Second), "the only message"),
	}
	m := buildTurnModel("s", rows, false)
	if e := m.Entries["e_1"]; e.Duplicate || !e.Primary {
		t.Errorf("harness block must render; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
	if e := m.Entries["e_2"]; !e.Duplicate || e.Primary {
		t.Errorf("OTel copy must stay hidden when the harness block renders; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
}

// A block carrying a THINKING payload classifies to kind "text" as well, because
// messageText falls through to Thinking.Text. It is reasoning, not prose, so the
// promotion must skip it — otherwise a subagent's collapsed view gains the model's
// private deliberation. Measured on this host: block payloads are 82,997 thinking
// against 55,538 text, so this is the majority case, not an edge one. The real
// session the card cites (br_1786568331006216317) is 5 text and 4 thinking, which is
// exactly its "9 of kind text, 5 carrying real prose".
func TestBuildTurnModel_ThinkingBlocksAreNotPromotedAsProse(t *testing.T) {
	base := time.Date(2026, 8, 12, 20, 58, 52, 0, time.UTC)
	thinking := mkRow(t, 2, msg.Event{
		Type:      msg.EventBlock,
		Timestamp: base.Add(time.Second),
		Block: &msg.BlockEvent{
			Index: 0,
			Block: &msg.ContentBlock{Type: "thinking", Thinking: &msg.ThinkingBlock{Text: "let me consider the options"}},
		},
	})
	rows := []store.EventRow{
		harnessTextBlock(t, 1, base, "I'll run the demo steps in order."),
		thinking,
	}
	m := buildTurnModel("br_sub", rows, false)

	if e := m.Entries["e_1"]; e.Duplicate || !e.Primary {
		t.Errorf("the text block must render; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
	if e := m.Entries["e_2"]; !e.Duplicate || e.Primary {
		t.Errorf("a thinking block is reasoning, not prose, and must stay hidden; got primary=%v duplicate=%v text=%q", e.Primary, e.Duplicate, e.Text)
	}
}

// The promotion must not make a turn's thinking block supersede its text block: a
// turn that is all thinking has no prose to show, and one that has both must show
// only the prose. This is the pair the previous test cannot ask on its own.
func TestBuildTurnModel_ThinkingOnlyTurnPromotesNothing(t *testing.T) {
	base := time.Date(2026, 8, 12, 20, 58, 52, 0, time.UTC)
	rows := []store.EventRow{
		mkRow(t, 1, msg.Event{
			Type:      msg.EventBlock,
			Timestamp: base,
			Block: &msg.BlockEvent{
				Index: 0,
				Block: &msg.ContentBlock{Type: "thinking", Thinking: &msg.ThinkingBlock{Text: "deliberating"}},
			},
		}),
	}
	m := buildTurnModel("s", rows, false)
	if e := m.Entries["e_1"]; !e.Duplicate || e.Primary {
		t.Errorf("a thinking-only turn must promote nothing; got primary=%v duplicate=%v", e.Primary, e.Duplicate)
	}
}
