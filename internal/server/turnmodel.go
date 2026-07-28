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

	// Usage is the per-message token usage for assistant/result entries,
	// mapped 1:1 from the terminating ResultEvent.Usage (msg.TokenUsage). Nil
	// (omitted) for entries with no usage meta — user prompts, tool calls,
	// legacy/no-cost turns. Additive: legacy consumers ignore it.
	Usage *EntryUsage `json:"usage,omitempty"`

	// Provenance / kind-specific fields, mapped 1:1 from the canonical event
	// (extensions / msg.ErrorEvent / msg.SystemEvent) — never invented. Mirror
	// chat-core src/net/types.ts Entry.
	Recovered  bool   `json:"recovered,omitempty"`  // OTel assistant text recovered after a dropped stream
	Code       string `json:"code,omitempty"`       // ErrorEvent.Code (TURN_IDLE_TIMEOUT, api_error, …)
	Retryable  bool   `json:"retryable,omitempty"`  // ErrorEvent.Retryable
	StatusCode int    `json:"statusCode,omitempty"` // ErrorEvent.StatusCode
	Subtype    string `json:"subtype,omitempty"`    // SystemEvent.Subtype (subagent_completed, …)

	Duplicate bool   `json:"duplicate"`
	Primary   bool   `json:"primary"`
	GroupID   string `json:"groupId,omitempty"`
}

// EntryUsage is the per-message token usage carried on an assistant/result
// Entry. Fields mirror msg.TokenUsage (llm-bridge/msg/usage.go) exactly, mapped
// to the chat-core camelCase wire names. Every field is omitempty so an
// all-zero usage still serializes to `{}` only when explicitly attached (the
// pointer itself is omitted when there is no usage meta at all).
//
// NOTE ON NAMES: cacheCreationTokens is sourced from msg.TokenUsage.CacheWriteTokens
// (json `cache_write_tokens`) — the canonical struct calls cache-creation
// "write". There is NO CacheCreationTokens field on TokenUsage; the
// per-api-call APICallEvent uses that name, but the message-level TokenUsage
// does not. We map the concept, not a same-named field.
type EntryUsage struct {
	InputTokens         int `json:"inputTokens,omitempty"`
	OutputTokens        int `json:"outputTokens,omitempty"`
	CacheReadTokens     int `json:"cacheReadTokens,omitempty"`
	CacheCreationTokens int `json:"cacheCreationTokens,omitempty"`
}

// TurnAggregates carries session-level cost and context state for the chat UI's
// cost chip and context% bar. Fields map from the canonical msg events:
//   - totalUsd/byModel/bySource ← the LATEST APISpendTotalEvent in the window
//     (msg.APISpendTotalEvent: TotalUSD, ByModel, ByQuerySource). bySource is
//     ByQuerySource renamed for the wire.
//   - contextTokens/contextLimit ← the LATEST usage-bearing event carrying them
//     (msg.TokenUsage.ContextTokens/ContextLimit; last-value-wins state).
//
// Because /messages is paginated, these are computed only from the events on
// the returned page. If the spend/context events fall outside the page, the
// corresponding fields stay zero (omitted) and the whole struct may be nil —
// the client falls back to its live-tail values.
type TurnAggregates struct {
	TotalUSD      float64            `json:"totalUsd,omitempty"`
	ByModel       map[string]float64 `json:"byModel,omitempty"`
	BySource      map[string]float64 `json:"bySource,omitempty"`
	ContextTokens int                `json:"contextTokens,omitempty"`
	ContextLimit  int                `json:"contextLimit,omitempty"`
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

	// Aggregates carries session cost/context state computed from the events on
	// this page. Nil (omitted) when no api_spend_total or context-bearing usage
	// event is present in the window — legacy/no-cost sessions are unaffected.
	Aggregates *TurnAggregates `json:"aggregates,omitempty"`
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

// isVisibleSystemSubtype reports whether a system event's subtype is a
// conversation-visible progress marker (shown in the collapsed view), as opposed
// to bookkeeping. Kept consistent with the live tail: the settled path surfaces the
// same known progress subtypes the tail does. subagent_completed is progress, never
// an error.
func isVisibleSystemSubtype(subtype string) bool {
	switch subtype {
	case "compact_boundary", "subagent_completed":
		return true
	default:
		return false
	}
}

// isRecovered reports whether an event carries extensions.recovered=true — the OTel
// assistant text llm-bridge-claudecode surfaces when the live stream produced nothing
// that turn (handler.go flushRecoveredAssistant).
func isRecovered(ev *msg.Event) bool {
	if raw, ok := ev.Extensions["recovered"]; ok {
		var b bool
		if json.Unmarshal(raw, &b) == nil && b {
			return true
		}
	}
	return false
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
		// A compact boundary and known progress subtypes (e.g. subagent_completed)
		// are real conversation markers, surfaced on the settled path exactly as the
		// live tail shows them — visible, kind "system", carrying the subtype. Other
		// system events are bookkeeping. A known progress subtype is never an error.
		if ev.System != nil && isVisibleSystemSubtype(ev.System.Subtype) {
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

	// Aggregate accumulators (Phase 2). Computed from the events actually read
	// on this page — if the spend/context events fall outside the page these
	// stay nil/zero and the client falls back to its live-tail values. Rows are
	// iterated in eventId-ASC order, so a later assignment is the LATEST value.
	var latestSpend *msg.APISpendTotalEvent
	var ctxTokens, ctxLimit int
	var haveContext bool

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
		case msg.EventUserMessage, msg.EventResult, msg.EventThinking, msg.EventStream, msg.EventBlock, msg.EventError, msg.EventSystem:
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
		// Kind-specific fields, mapped straight from the canonical event.
		if ev.Type == msg.EventError && ev.Error != nil {
			e.Code = ev.Error.Code
			e.Retryable = ev.Error.Retryable
			e.StatusCode = ev.Error.StatusCode
		}
		if ev.Type == msg.EventSystem && ev.System != nil {
			e.Subtype = ev.System.Subtype
		}
		if isRecovered(&ev) {
			e.Recovered = true
		}
		// Per-message usage: the assistant's terminating result carries the
		// turn's TokenUsage. Attach it when non-empty; omit otherwise.
		if ev.Type == msg.EventResult && ev.Result != nil {
			e.Usage = entryUsageFromTokens(ev.Result.Usage)
		}

		// Aggregate sources (last-value-wins over the page). The latest
		// api_spend_total gives totalUsd/byModel/bySource; the latest usage
		// carrying context state gives contextTokens/contextLimit.
		if ev.Type == msg.EventAPISpendTotal && ev.APISpendTotal != nil {
			latestSpend = ev.APISpendTotal
		}
		if tks, lim, ok := contextFromEvent(&ev); ok {
			ctxTokens, ctxLimit, haveContext = tks, lim, true
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

	// Surface recovered/OTel assistant text (A1). A recovered EventBlock text —
	// the OTel copy llm-bridge-claudecode forwards when the live stream produced
	// nothing that turn — is classified as a superseded streaming block (hidden) by
	// default. But in a dropped turn there is no Result or harness assistant text to
	// supersede it, so hiding it would lose the model's only final message. Promote
	// such a block to the visible/primary conversation atom when nothing in its turn
	// supersedes it. Normal turns carry no recovered/OTel block (the healthy OTel copy
	// is buffered and dropped upstream), so their streamed blocks stay hidden — no
	// regression. The recovered flag is preserved either way.
	surfaceRecoveredBlocks(turns, entries)

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
		SessionID:  sessionID,
		Turns:      turns,
		Entries:    entries,
		Validator:  validator,
		More:       more,
		Aggregates: buildAggregates(latestSpend, ctxTokens, ctxLimit, haveContext),
	}
}

// entryUsageFromTokens maps a canonical msg.TokenUsage to the wire EntryUsage,
// returning nil when there is no usage to report (all mapped fields zero) so the
// Entry.usage field is omitted. cacheCreationTokens is sourced from
// TokenUsage.CacheWriteTokens — see EntryUsage's doc comment.
func entryUsageFromTokens(u msg.TokenUsage) *EntryUsage {
	if u.InputTokens == 0 && u.OutputTokens == 0 && u.CacheReadTokens == 0 && u.CacheWriteTokens == 0 {
		return nil
	}
	return &EntryUsage{
		InputTokens:         u.InputTokens,
		OutputTokens:        u.OutputTokens,
		CacheReadTokens:     u.CacheReadTokens,
		CacheCreationTokens: u.CacheWriteTokens,
	}
}

// contextFromEvent extracts ContextTokens/ContextLimit from whichever
// usage-bearing variant an event carries (usage_total, result, api_spend_total).
// ContextTokens/ContextLimit are last-value-wins state on msg.TokenUsage. ok is
// false when the event carries no context state (neither field set), so the
// caller only advances its latest value on real context-bearing events.
func contextFromEvent(ev *msg.Event) (tokens, limit int, ok bool) {
	var u msg.TokenUsage
	switch {
	case ev.UsageTotal != nil:
		u = ev.UsageTotal.Usage
	case ev.APISpendTotal != nil:
		u = ev.APISpendTotal.Usage
	case ev.Type == msg.EventResult && ev.Result != nil:
		u = ev.Result.Usage
	default:
		return 0, 0, false
	}
	if u.ContextTokens == 0 && u.ContextLimit == 0 {
		return 0, 0, false
	}
	return u.ContextTokens, u.ContextLimit, true
}

// buildAggregates assembles the TurnModel.aggregates block from the latest spend
// event and latest context state seen on the page. Returns nil when neither is
// present so the field is omitted (legacy/no-cost sessions unaffected). totalUsd
// uses APISpendTotalEvent.TotalUSD, falling back to the sum of ByModel when the
// total is zero but a per-model breakdown exists. bySource is ByQuerySource.
func buildAggregates(spend *msg.APISpendTotalEvent, ctxTokens, ctxLimit int, haveContext bool) *TurnAggregates {
	if spend == nil && !haveContext {
		return nil
	}
	agg := &TurnAggregates{}
	if spend != nil {
		total := spend.TotalUSD
		if total == 0 && len(spend.ByModel) > 0 {
			for _, v := range spend.ByModel {
				total += v
			}
		}
		agg.TotalUSD = total
		agg.ByModel = spend.ByModel
		agg.BySource = spend.ByQuerySource
	}
	if haveContext {
		agg.ContextTokens = ctxTokens
		agg.ContextLimit = ctxLimit
	}
	return agg
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

// isRecoveredOrOTelAssistantText reports whether an entry is a recovered/OTel-sourced
// assistant text atom — the candidate A1 must keep visible when unsuperseded. Stream
// deltas are harness-sourced so they never match; only the forwarded OTel block does.
func isRecoveredOrOTelAssistantText(e Entry) bool {
	return e.Role == "assistant" && e.Kind == "text" && e.Text != "" &&
		(e.Recovered || e.Source == sourceOTel)
}

// supersedesRecovered reports whether an entry is an authoritative assistant message
// that would supersede a recovered/OTel block in the same turn: a Result, or a normal
// (harness, non-recovered) assistant text. When present, the recovered block stays
// hidden exactly like a streamed block in a healthy turn.
func supersedesRecovered(e Entry) bool {
	if e.Text == "" {
		return false
	}
	if e.Kind == "result" && e.Role == "assistant" {
		return true
	}
	if e.Kind == "text" && e.Role == "assistant" && e.Source == sourceHarness && !e.Recovered {
		return true
	}
	return false
}

// surfaceRecoveredBlocks promotes recovered/OTel assistant text blocks to visible
// primary atoms in any turn where nothing supersedes them (A1). Mutates `entries` in
// place; non-destructive (only flips duplicate/primary annotation, drops nothing).
func surfaceRecoveredBlocks(turns []Turn, entries map[string]Entry) {
	for _, turn := range turns {
		superseded := false
		hasCandidate := false
		for _, eid := range turn.EntryIDs {
			e := entries[eid]
			if isRecoveredOrOTelAssistantText(e) {
				hasCandidate = true
				continue
			}
			if supersedesRecovered(e) {
				superseded = true
			}
		}
		if !hasCandidate || superseded {
			continue
		}
		for _, eid := range turn.EntryIDs {
			e := entries[eid]
			if !isRecoveredOrOTelAssistantText(e) {
				continue
			}
			e.Primary = true
			e.Duplicate = false
			entries[eid] = e
		}
	}
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
	case msg.EventSystem:
		if ev.System != nil {
			return ev.System.Message
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
