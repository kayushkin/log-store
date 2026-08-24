//go:build realdata

package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/log-store/internal/store"
)

// Step-1 verification from dash docs/dashv2-turns-per-message.md, run against
// the REAL event log rather than fixtures: for the measured turn
// (turn_01M0TPBZVZBX02T…, session br_1787601675531668005), the materialized
// model must (a) stamp messageId on every message-bearing entry, agreeing with
// the raw event's message_id, and (b) return the narration blocks primary.
// Build-tagged out of the normal suite: it reads ~/.config/log-store/events.db.
func TestRealTurn_NarrationVisibleAndMessageIDAgrees(t *testing.T) {
	home, _ := os.UserHomeDir()
	st, err := store.New(filepath.Join(home, ".config/log-store/events.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rows, _, err := st.EventPage("br_1787601675531668005", 100000, 0)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var turnRows []store.EventRow
	for _, r := range rows {
		var ev msg.Event
		if json.Unmarshal(r.Data, &ev) == nil && len(ev.TurnID) > 19 && ev.TurnID[:19] == "turn_01M0TPBZVZBX02" {
			turnRows = append(turnRows, r)
		}
	}
	if len(turnRows) == 0 {
		t.Fatal("the measured turn is not in the store")
	}
	m := buildTurnModel("br_1787601675531668005", turnRows, false)

	var narrationPrimary, textBlocks int
	for _, e := range m.Entries {
		var ev msg.Event
		if err := json.Unmarshal(e.Raw, &ev); err != nil {
			continue
		}
		if e.MessageID != ev.MessageID {
			t.Errorf("entry %s messageId %q disagrees with raw event %q", e.ID, e.MessageID, ev.MessageID)
		}
		if ev.Type == msg.EventBlock && ev.Block != nil && ev.Block.Block != nil && ev.Block.Block.Text != nil {
			textBlocks++
			if e.Primary && !e.Duplicate {
				narrationPrimary++
			}
		}
	}
	t.Logf("turn rows=%d entries=%d textBlocks=%d primary=%d", len(turnRows), len(m.Entries), textBlocks, narrationPrimary)
	// The measurement: 31 text-bearing messages, 30 of them tooled (narration),
	// 1 answer (superseded by the result). So primary text blocks = 30.
	if narrationPrimary != 30 {
		t.Errorf("primary text blocks = %d, want 30 (the turn's narration count)", narrationPrimary)
	}
}
