package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// The projection is what stops the default page shipping ten times what it renders.
// Measured 2026-08-25 on one real 30-message page: 9.91 MB served, 0.97 MB rendered,
// with `raw` alone accounting for 78.9% and the hidden OTel duplicates for 56.9%.
//
// These cases pin the two things it drops, the one thing it must NOT lose in the
// process (which sources reported each dedup group), and the bookkeeping that keeps
// the result renderable.

func TestProject_DropsRawFromEverySurvivingEntry(t *testing.T) {
	m := TurnModel{
		Entries: map[string]Entry{
			"a": {ID: "a", TurnID: "t", Raw: json.RawMessage(`{"type":"stream"}`), Primary: true},
			"b": {ID: "b", TurnID: "t", Raw: json.RawMessage(`{"type":"result"}`), Primary: true},
		},
		Turns: []Turn{{ID: "t", EntryIDs: []string{"a", "b"}}},
	}

	got := projectForReading(m)

	if len(got.Entries) != 2 {
		t.Fatalf("expected both entries to survive, got %d", len(got.Entries))
	}
	for id, e := range got.Entries {
		if e.Raw != nil {
			t.Errorf("entry %s still carries raw", id)
		}
	}
}

func TestProject_DropsDuplicateEntriesAndPrunesTheTurn(t *testing.T) {
	// A turn referencing an entry that is no longer in the map is not cosmetic:
	// chat-core's collapsed-view selector looks each id up and reads `.duplicate` off
	// the result, so a missing entry reads as "not a duplicate" and the view tries to
	// render nothing.
	m := TurnModel{
		Entries: map[string]Entry{
			"keep": {ID: "keep", TurnID: "t", Source: "harness", Primary: true, GroupID: "g1"},
			"dupe": {ID: "dupe", TurnID: "t", Source: "otel", Duplicate: true, GroupID: "g1"},
		},
		Turns: []Turn{{ID: "t", EntryIDs: []string{"keep", "dupe"}}},
	}

	got := projectForReading(m)

	if _, ok := got.Entries["dupe"]; ok {
		t.Errorf("duplicate entry survived the projection")
	}
	if _, ok := got.Entries["keep"]; !ok {
		t.Errorf("non-duplicate entry was dropped")
	}
	if len(got.Turns) != 1 || len(got.Turns[0].EntryIDs) != 1 || got.Turns[0].EntryIDs[0] != "keep" {
		t.Errorf("turn entryIds = %v, want [keep]", got.Turns[0].EntryIDs)
	}
}

func TestProject_CarriesTheSourcesOfADroppedDuplicate(t *testing.T) {
	// The reason duplicates can be dropped at all. dash's Turns badge answers "how many
	// sources reported this message?" by reading `groupId` and `source` off every entry
	// INCLUDING the hidden copies — so with the copies gone the answer has to travel
	// some other way, or the badge silently reports one source for every message.
	m := TurnModel{
		Entries: map[string]Entry{
			"h": {ID: "h", TurnID: "t", Source: "harness", Primary: true, GroupID: "g1"},
			"o": {ID: "o", TurnID: "t", Source: "otel", Duplicate: true, GroupID: "g1"},
		},
		Turns: []Turn{{ID: "t", EntryIDs: []string{"h", "o"}}},
	}

	got := projectForReading(m)

	sources := got.SourceGroups["g1"]
	if len(sources) != 2 {
		t.Fatalf("sourceGroups[g1] = %v, want both sources", sources)
	}
	if !containsString(sources, "harness") || !containsString(sources, "otel") {
		t.Errorf("sourceGroups[g1] = %v, want harness and otel", sources)
	}
}

func TestProject_OmitsSourceGroupsWhenNothingWasGrouped(t *testing.T) {
	// A session with no dual-emitted content has no groups, and an empty map on the
	// wire would be a field claiming to describe something. `omitempty` plus this.
	m := TurnModel{
		Entries: map[string]Entry{"a": {ID: "a", TurnID: "t", Source: "harness", Primary: true}},
		Turns:   []Turn{{ID: "t", EntryIDs: []string{"a"}}},
	}

	if got := projectForReading(m); got.SourceGroups != nil {
		t.Errorf("sourceGroups = %v on a model with no dedup groups", got.SourceGroups)
	}
}

func TestProject_KeepsATurnWhoseEveryEntryWasADuplicate(t *testing.T) {
	// It is still a turn that happened. Dropping it leaves a hole in the transcript
	// rather than a collapsed row.
	m := TurnModel{
		Entries: map[string]Entry{
			"d": {ID: "d", TurnID: "t", Source: "otel", Duplicate: true, GroupID: "g"},
		},
		Turns: []Turn{{ID: "t", EntryIDs: []string{"d"}}},
	}

	got := projectForReading(m)

	if len(got.Turns) != 1 {
		t.Fatalf("turn count = %d, want the turn to survive", len(got.Turns))
	}
	if len(got.Turns[0].EntryIDs) != 0 {
		t.Errorf("entryIds = %v, want empty", got.Turns[0].EntryIDs)
	}
}

func TestProject_DoesNotMutateItsArgument(t *testing.T) {
	// Callers hand this a freshly materialized model today. A projection that quietly
	// edited its argument would be a trap for the next caller that does not — and the
	// bundle handler loops, so a shared backing array is one refactor away.
	m := TurnModel{
		Entries: map[string]Entry{
			"keep": {ID: "keep", TurnID: "t", Source: "harness", Primary: true, Raw: json.RawMessage(`{"a":1}`)},
			"dupe": {ID: "dupe", TurnID: "t", Source: "otel", Duplicate: true},
		},
		Turns: []Turn{{ID: "t", EntryIDs: []string{"keep", "dupe"}}},
	}

	_ = projectForReading(m)

	if len(m.Entries) != 2 {
		t.Errorf("input entries were mutated: %d left", len(m.Entries))
	}
	if m.Entries["keep"].Raw == nil {
		t.Errorf("input entry lost its raw")
	}
	if len(m.Turns[0].EntryIDs) != 2 {
		t.Errorf("input turn was pruned: %v", m.Turns[0].EntryIDs)
	}
}

func TestProject_LeavesTheRestOfTheModelAlone(t *testing.T) {
	// The projection is about payload, not about meaning. Validator, `more` and the
	// cost/context roll-up all describe the session rather than the bytes, and a reader
	// that lost them would page wrongly or report the wrong spend.
	m := TurnModel{
		SessionID:  "sess",
		Entries:    map[string]Entry{"a": {ID: "a", TurnID: "t", Primary: true}},
		Turns:      []Turn{{ID: "t", EntryIDs: []string{"a"}}},
		Validator:  Validator{MaxEventID: 42, EventCount: 7, UpdatedAt: "2026-08-25T12:00:00Z"},
		More:       true,
		Aggregates: &TurnAggregates{TotalUSD: 1.25},
	}

	got := projectForReading(m)

	if got.SessionID != "sess" || got.More != true {
		t.Errorf("sessionId/more changed: %q / %v", got.SessionID, got.More)
	}
	if got.Validator.MaxEventID != 42 || got.Validator.EventCount != 7 {
		t.Errorf("validator changed: %+v", got.Validator)
	}
	if got.Aggregates == nil || got.Aggregates.TotalUSD != 1.25 {
		t.Errorf("aggregates changed: %+v", got.Aggregates)
	}
}

func TestProject_RealShapedPageShrinksAndKeepsWhatTurnsRenders(t *testing.T) {
	// End to end over a materialized model rather than a hand-built one, so the
	// annotation that decides what is a duplicate is the real one.
	base := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
	big := make([]byte, 4000)
	for i := range big {
		big[i] = 'x'
	}
	rows := mkRows(t, []storeRow{
		{id: 1, ev: msg.Event{Type: msg.EventUserMessage, TurnID: "t", Timestamp: base,
			Result: &msg.ResultEvent{Text: "run it"}}},
		{id: 2, ev: msg.Event{Type: msg.EventToolCall, TurnID: "t", Timestamp: base.Add(time.Second),
			ToolCall: &msg.ToolCallEvent{ToolID: "toolu_1", Name: "Bash", Input: json.RawMessage(`{"command":"ls"}`)}}},
		{id: 3, ev: msg.Event{Type: msg.EventToolResult, TurnID: "t", Timestamp: base.Add(2 * time.Second),
			ToolResult: &msg.ToolResultEvent{ToolID: "toolu_1", Name: "Bash", Output: string(big)}}},
		{id: 4, ev: msg.Event{Type: msg.EventResult, TurnID: "t", Timestamp: base.Add(3 * time.Second),
			Result: &msg.ResultEvent{Text: "done"}}},
	})
	full := buildTurnModel("sess", rows, false)
	projected := projectForReading(full)

	fullBytes, err := json.Marshal(full)
	if err != nil {
		t.Fatalf("marshal full: %v", err)
	}
	projectedBytes, err := json.Marshal(projected)
	if err != nil {
		t.Fatalf("marshal projected: %v", err)
	}
	if len(projectedBytes) >= len(fullBytes) {
		t.Errorf("projection did not shrink the page: %d -> %d bytes", len(fullBytes), len(projectedBytes))
	}

	// The content the Turns view actually draws has to still be there.
	var sawToolResult, sawAnswer bool
	for _, e := range projected.Entries {
		if e.Kind == "tool_result" && e.ToolID == "toolu_1" {
			sawToolResult = true
		}
		if e.Kind == "result" && e.Text == "done" {
			sawAnswer = true
		}
	}
	if !sawToolResult {
		t.Errorf("projected page lost the tool result the Turns view renders")
	}
	if !sawAnswer {
		t.Errorf("projected page lost the final answer")
	}
}
