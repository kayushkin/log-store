package server

import (
	"encoding/json"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// Canonical types — defined in llm-bridge/msg/server.go.
// DO NOT define new API response types here. Add them to msg/ instead,
// then run generate-ts.sh so the TypeScript frontend stays in sync.
type (
	MaterializedMessage = msg.MaterializedMessage
	MaterializedTool    = msg.MaterializedTool
)

// materializeMessages flattens raw events into chat messages.
//
// Modern events carry a canonical bridge MessageID (assigned by
// llm-bridge-server's manager). When present, we group strictly by that id —
// no inference needed. For legacy events written before MessageID existed,
// we fall back to boundary inference (user_message / result mark turn edges).
func materializeMessages(rawEvents []json.RawMessage) []MaterializedMessage {
	var msgs []MaterializedMessage
	idIndex := make(map[string]int) // bridge MessageID → msgs[] index

	// Inference state for legacy events with no MessageID.
	var current *MaterializedMessage
	var currentEvents []json.RawMessage
	flushLegacy := func() {
		if current != nil {
			current.Done = true
			current.Events = currentEvents
			msgs = append(msgs, *current)
			current = nil
			currentEvents = nil
		}
	}

	for _, raw := range rawEvents {
		var ev msg.Event
		if json.Unmarshal(raw, &ev) != nil {
			continue
		}

		// Modern path: every chat event carries a canonical MessageID.
		if ev.MessageID != "" {
			flushLegacy() // close any straggling legacy bubble
			idx, ok := idIndex[ev.MessageID]
			if !ok {
				role := "assistant"
				if ev.Type == msg.EventUserMessage {
					role = "user"
				}
				msgs = append(msgs, MaterializedMessage{
					ID:               ev.MessageID,
					HarnessMessageID: ev.HarnessMessageID,
					Role:             role,
					Timestamp:        ev.Timestamp.Format(time.RFC3339),
				})
				idx = len(msgs) - 1
				idIndex[ev.MessageID] = idx
			}
			applyToMessage(&msgs[idx], &ev, raw)
			continue
		}

		// Legacy path: infer boundaries from event types.
		switch ev.Type {
		case msg.EventUserMessage:
			flushLegacy()
			text := ""
			if ev.Result != nil {
				text = ev.Result.Text
			}
			msgs = append(msgs, MaterializedMessage{
				Role:      "user",
				Content:   text,
				Timestamp: ev.Timestamp.Format(time.RFC3339),
				Events:    []json.RawMessage{raw},
				Done:      true,
			})

		case msg.EventResult:
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			applyAssistantPayload(current, &ev)
			flushLegacy()

		default:
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			applyAssistantPayload(current, &ev)
		}
	}

	flushLegacy()

	if msgs == nil {
		msgs = []MaterializedMessage{}
	}
	return msgs
}

// applyToMessage applies a single event to the message it belongs to (modern,
// MessageID-grouped path). Sets Done on result/error, accumulates text/tools.
func applyToMessage(m *MaterializedMessage, ev *msg.Event, raw json.RawMessage) {
	m.Events = append(m.Events, raw)
	if m.HarnessMessageID == "" && ev.HarnessMessageID != "" {
		m.HarnessMessageID = ev.HarnessMessageID
	}
	switch ev.Type {
	case msg.EventUserMessage:
		if ev.Result != nil {
			m.Content = ev.Result.Text
		}
		m.Done = true
	case msg.EventResult:
		applyAssistantPayload(m, ev)
		m.Done = true
	case msg.EventError:
		m.Done = true
	default:
		applyAssistantPayload(m, ev)
	}
}

// applyAssistantPayload merges an assistant-side event's payload into the
// running materialized message. Shared between modern and legacy paths.
func applyAssistantPayload(m *MaterializedMessage, ev *msg.Event) {
	switch ev.Type {
	case msg.EventResult:
		if ev.Result != nil {
			if ev.Result.Text != "" {
				m.Content = ev.Result.Text
			}
			m.Meta = ev.Result
		}
	case msg.EventStream:
		if ev.Stream != nil && ev.Stream.Delta != nil {
			switch ev.Stream.Delta.Type {
			case msg.DeltaText:
				m.Content += ev.Stream.Delta.Text
			case msg.DeltaThinking:
				m.Thinking += ev.Stream.Delta.Thinking
			}
		}
	case msg.EventThinking:
		if ev.Thinking != nil {
			m.Thinking += ev.Thinking.Text
		}
	case msg.EventToolCall:
		if ev.ToolCall != nil {
			m.Tools = append(m.Tools, MaterializedTool{
				ToolID: ev.ToolCall.ToolID,
				Tool:   ev.ToolCall.Name,
				Input:  ev.ToolCall.Input,
			})
		}
	case msg.EventToolResult:
		if ev.ToolResult != nil {
			matched := false
			for i := len(m.Tools) - 1; i >= 0; i-- {
				t := &m.Tools[i]
				if ev.ToolResult.ToolID != "" && t.ToolID == ev.ToolResult.ToolID {
					t.Output = ev.ToolResult.Output
					t.Error = ev.ToolResult.IsError
					matched = true
					break
				}
			}
			if !matched {
				for i := len(m.Tools) - 1; i >= 0; i-- {
					t := &m.Tools[i]
					if t.Tool == ev.ToolResult.Name && t.Output == "" {
						t.Output = ev.ToolResult.Output
						t.Error = ev.ToolResult.IsError
						break
					}
				}
			}
		}
	}
}

func newAssistantMsg(ts time.Time) *MaterializedMessage {
	return &MaterializedMessage{
		Role:      "assistant",
		Timestamp: ts.Format(time.RFC3339),
	}
}
