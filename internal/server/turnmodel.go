package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/kayushkin/log-store/internal/store"
	"github.com/kayushkin/llm-bridge/msg"
)

// The wire types below mirror chat-core/src/net/types.ts EXACTLY (camelCase
// field names, RFC3339+offset timestamps). They are the settled-history
// counterpart of the client's live-tail TurnModel: log-store owns dedup
// annotation for events that have stopped moving. See docs/WIRE.md.
//
// The cardinal rule (D9): materialization is NON-DESTRUCTIVE. Every stored
// event becomes exactly one Entry — nothing is dropped. Dedup and view
// selection are expressed purely as annotation (duplicate / primary / groupId),
// so the raw Timeline view can reconstruct every stored event from the payload.

// Entry is one renderable atom mapped 1:1 to a stored event.
type Entry struct {
	ID      string `json:"id"`
	TurnID  string `json:"turnId"`
	Role    string `json:"role"`
	Kind    string `json:"kind"`
	Source  string `json:"source"`
	EventID int64  `json:"eventId"`
	Ts      string `json:"ts"`

	Text       string          `json:"text,omitempty"`
	ToolName   string          `json:"toolName,omitempty"`
	ToolInput  json.RawMessage `json:"toolInput,omitempty"`
	ToolResult json.RawMessage `json:"toolResult,omitempty"`
	Raw        json.RawMessage `json:"raw,omitempty"`

	Duplicate bool   `json:"duplicate"`
	Primary   bool   `json:"primary"`
	GroupID   string `json:"groupId,omitempty"`
}

// Turn groups entries into one ordered unit; entryIds are ordered by eventId.
type Turn struct {
	ID       string   `json:"id"`
	Role     string   `json:"role"`
	Ts       string   `json:"ts"`
	EntryIDs []string `json:"entryIds"`
}

// Validator is the cheap staleness currency (see store.SessionValidator).
type Validator struct {
	MaxEventID int64  `json:"maxEventId"`
	EventCount int    `json:"eventCount"`
	UpdatedAt  string `json:"updatedAt"`
}

// TurnModel is the render-ready, fully-annotated model for one session's tail.
type TurnModel struct {
	SessionID string           `json:"sessionId"`
	Turns     []Turn           `json:"turns"`
	Entries   map[string]Entry `json:"entries"`
	Validator Validator        `json:"validator"`
	More      bool             `json:"more"`
}

// MessagesResponse is the /sessions/{id}/messages?limit= wire shape.
type MessagesResponse struct {
	Model TurnModel `json:"model"`
}

const (
	sourceHarness = "harness"
	sourceOTel    = "otel"
)

// eventSource reports whether an event is the OTel copy of a dual-emitted
// signal. llm-bridge-claudecode tags the OTel copy `extensions.source = "otel"`
// (otel.go tagOTelSource); everything else is harness-sourced. This is the only
// reliable discriminator — the two copies carry distinct message_ids and
// turn_ids, so there is no shared id to correlate them by.
func eventSource(ev *msg.Event) string {
	if raw, ok := ev.Extensions["source"]; ok {
		var s string
		if json.Unmarshal(raw, &s) == nil && s == sourceOTel {
			return sourceOTel
		}
	}
	return sourceHarness
}

// classify maps an event to its wire (role, kind) and whether it is a
// "conversation" atom that belongs in the collapsed Turns view. Bookkeeping and
// superseded-partial events (stream deltas, blocks, session_state, api_call,
// …) are kept in the payload — nothing is dropped — but annotated so the
// collapsed view (entries where !duplicate) shows only conversation.
func classify(ev *msg.Event) (role, kind string, conversation bool) {
	switch ev.Type {
	case msg.EventUserMessage:
		return "user", "text", true
	case msg.EventResult:
		return "assistant", "result", true
	case msg.EventThinking:
		return "assistant", "thinking", true
	case msg.EventToolCall:
		return "assistant", "tool_call", true
	case msg.EventToolResult:
		return "tool", "tool_result", true
	case msg.EventError:
		return "assistant", "error", true
	case msg.EventSystem:
		// A compact boundary is a real conversation marker; other system
		// events are bookkeeping.
		if ev.System != nil && ev.System.Subtype == "compact_boundary" {
			return "system", "system", true
		}
		return "system", "meta", false
	case msg.EventStream, msg.EventBlock:
		// Streaming partials / per-block echoes are superseded by the message's
		// result in the collapsed view, but retained for the raw Timeline.
		return "assistant", "text", false
	default:
		return "assistant", "meta", false
	}
}

// entryText extracts the logical text used both for display and as the dedup
// key. Only user_message and result carry dedup-relevant text.
func entryText(ev *msg.Event) string {
	if ev.Result != nil {
		return ev.Result.Text
	}
	return ""
}

// buildTurnModel groups an ordered (by eventId ASC) slice of event rows into a
// non-destructive, fully-annotated TurnModel. `more` is threaded through from
// the pager (older turns remain beyond this page).
func buildTurnModel(sessionID string, rows []store.EventRow, more bool) TurnModel {
	entries := make(map[string]Entry, len(rows))
	var order []string // entry ids in eventId order

	// First pass: build one Entry per event, classified but not yet deduped.
	type dedupKey struct {
		class string // "user" | "assistant"
		text  string
	}
	// For each dedup key, collect harness- and otel-sourced entry ids in
	// eventId order so we can pair them count-wise (never positionally).
	harnessByKey := map[dedupKey][]string{}
	otelByKey := map[dedupKey][]string{}

	var maxEventID int64

	for _, r := range rows {
		if r.ID > maxEventID {
			maxEventID = r.ID
		}
		var ev msg.Event
		if err := json.Unmarshal(r.Data, &ev); err != nil {
			// Unparseable event: still emit it as an opaque meta entry so the
			// raw Timeline can show it. Nothing is dropped.
			id := fmt.Sprintf("e_%d", r.ID)
			entries[id] = Entry{
				ID: id, TurnID: "", Role: "system", Kind: "meta",
				Source: sourceHarness, EventID: r.ID, Ts: "",
				Raw: append(json.RawMessage(nil), r.Data...), Duplicate: true,
			}
			order = append(order, id)
			continue
		}

		role, kind, conversation := classify(&ev)
		source := eventSource(&ev)
		id := fmt.Sprintf("e_%d", r.ID)

		e := Entry{
			ID:      id,
			TurnID:  ev.TurnID,
			Role:    role,
			Kind:    kind,
			Source:  source,
			EventID: r.ID,
			Ts:      formatTS(ev.Timestamp),
			Raw:     append(json.RawMessage(nil), r.Data...),
		}
		switch ev.Type {
		case msg.EventUserMessage, msg.EventResult, msg.EventThinking, msg.EventStream, msg.EventBlock, msg.EventError:
			e.Text = messageText(&ev)
		case msg.EventToolCall:
			if ev.ToolCall != nil {
				e.ToolName = ev.ToolCall.Name
				e.ToolInput = ev.ToolCall.Input
			}
		case msg.EventToolResult:
			if ev.ToolResult != nil {
				e.ToolName = ev.ToolResult.Name
				e.ToolResult = json.RawMessage(quoteJSON(ev.ToolResult.Output))
			}
		}

		// Default annotation. Conversation atoms are shown (primary, not
		// duplicate); everything else is retained but hidden from the collapsed
		// view. Dedup-eligible entries are re-annotated in the second pass.
		if conversation {
			e.Primary = true
			e.Duplicate = false
		} else {
			e.Primary = false
			e.Duplicate = true
		}

		entries[id] = e
		order = append(order, id)

		// Register dedup-eligible entries (user prompts + assistant results),
		// keyed by exact text, bucketed by source.
		if conversation && (ev.Type == msg.EventUserMessage || ev.Type == msg.EventResult) {
			t := entryText(&ev)
			if t != "" {
				class := "assistant"
				if ev.Type == msg.EventUserMessage {
					class = "user"
				}
				k := dedupKey{class: class, text: t}
				if source == sourceOTel {
					otelByKey[k] = append(otelByKey[k], id)
				} else {
					harnessByKey[k] = append(harnessByKey[k], id)
				}
			}
		}
	}

	// Second pass: pair OTel copies against harness copies count-wise. The i-th
	// OTel copy of a given text is absorbed by the i-th harness copy — they share
	// a groupId, the harness copy stays primary, the OTel copy is marked
	// duplicate (hidden in the collapsed view, still present for raw Timeline).
	// Surplus copies of EITHER source remain standalone and visible: a genuine
	// re-send of identical text still shows twice (extra harness copy), and a
	// PTY-only or stream-json-dropped turn whose only record is the OTel copy
	// still renders (surplus OTel copy). This is the source+count model, never
	// positional — the OTel exporter batches ~1s so its copy can land after the
	// reply.
	for k, hIDs := range harnessByKey {
		oIDs := otelByKey[k]
		n := len(hIDs)
		if len(oIDs) < n {
			n = len(oIDs)
		}
		for i := 0; i < n; i++ {
			gid := "g_" + hIDs[i]
			h := entries[hIDs[i]]
			h.GroupID = gid
			h.Primary = true
			h.Duplicate = false
			entries[hIDs[i]] = h

			o := entries[oIDs[i]]
			o.GroupID = gid
			o.Primary = false
			o.Duplicate = true
			entries[oIDs[i]] = o
		}
	}

	turns := buildTurns(order, entries)

	// Validator is derived from the events actually present. maxEventID is the
	// high-water row id in this page; the count endpoint (/validators) reports
	// the whole-session totals used for staleness — here we report the page's
	// own high-water id and its entry count so a tail response is self-describing.
	validator := Validator{
		MaxEventID: maxEventID,
		EventCount: len(rows),
	}
	if len(rows) > 0 {
		if last, ok := entries[order[len(order)-1]]; ok {
			validator.UpdatedAt = last.Ts
		}
	}

	return TurnModel{
		SessionID: sessionID,
		Turns:     turns,
		Entries:   entries,
		Validator: validator,
		More:      more,
	}
}

// buildTurns groups entries into turns. A turn is one bridge TurnID's worth of
// events (a user_message through its terminating result and everything in
// between). Events with no TurnID (bookkeeping, legacy) attach to the current
// turn, or open a synthetic one if none is open yet. entryIds preserve eventId
// order; turns preserve first-appearance order.
func buildTurns(order []string, entries map[string]Entry) []Turn {
	var turns []Turn
	index := map[string]int{}
	lastTurnID := ""

	for _, eid := range order {
		e := entries[eid]
		turnID := e.TurnID
		if turnID == "" {
			if lastTurnID != "" {
				turnID = lastTurnID
			} else {
				turnID = fmt.Sprintf("t_%d", e.EventID)
			}
		}
		lastTurnID = turnID

		idx, ok := index[turnID]
		if !ok {
			role := e.Role
			// Prefer the user role as the turn's role when a prompt opens it;
			// otherwise use the opening entry's role.
			turns = append(turns, Turn{ID: turnID, Role: role, Ts: e.Ts})
			idx = len(turns) - 1
			index[turnID] = idx
		}
		turns[idx].EntryIDs = append(turns[idx].EntryIDs, eid)
		if entries[eid].TurnID != turnID {
			// Normalize the entry's turnId so client-side turn lookups match the
			// synthesized/carried-forward turn id.
			en := entries[eid]
			en.TurnID = turnID
			entries[eid] = en
		}
	}
	return turns
}

// messageText returns the display text for a text-bearing event.
func messageText(ev *msg.Event) string {
	switch ev.Type {
	case msg.EventUserMessage, msg.EventResult:
		if ev.Result != nil {
			return ev.Result.Text
		}
	case msg.EventThinking:
		if ev.Thinking != nil {
			return ev.Thinking.Text
		}
	case msg.EventError:
		if ev.Error != nil {
			return ev.Error.Message
		}
	case msg.EventStream:
		if ev.Stream != nil && ev.Stream.Delta != nil {
			if ev.Stream.Delta.Text != "" {
				return ev.Stream.Delta.Text
			}
			return ev.Stream.Delta.Thinking
		}
	case msg.EventBlock:
		if ev.Block != nil && ev.Block.Block != nil {
			b := ev.Block.Block
			if b.Text != nil {
				return b.Text.Text
			}
			if b.Thinking != nil {
				return b.Thinking.Text
			}
		}
	}
	return ""
}

// formatTS renders an event timestamp as RFC3339 with offset, never naive. A
// zero time yields "" (the field is required on the wire but a zero timestamp
// is meaningless; the client tolerates an empty ts on malformed events).
func formatTS(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// quoteJSON wraps a plain string tool-result payload as a JSON string so it can
// ride in the toolResult json.RawMessage field. Tool outputs are stored as
// plain strings on ToolResultEvent.Output.
func quoteJSON(s string) []byte {
	b, err := json.Marshal(s)
	if err != nil {
		return []byte(`""`)
	}
	return b
}
