package server

import (
	"encoding/json"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// MaterializedMessage is an assembled message for the /messages endpoint.
// Meta is the full ResultEvent — no fields stripped.
// Events contains every raw event that contributed to this message.
type MaterializedMessage struct {
	Role      string            `json:"role"`
	Content   string            `json:"content"`
	Thinking  string            `json:"thinking,omitempty"`
	Tools     []MaterializedTool `json:"tools,omitempty"`
	Meta      *msg.ResultEvent  `json:"meta,omitempty"`
	Events    []json.RawMessage `json:"events,omitempty"`
	Timestamp string            `json:"timestamp"`
	Done      bool              `json:"done"`
}

// MaterializedTool preserves tool call data without narrowing.
type MaterializedTool struct {
	ToolID string          `json:"tool_id"`
	Tool   string          `json:"tool"`
	Input  json.RawMessage `json:"input,omitempty"`
	Output string          `json:"output,omitempty"`
	Error  bool            `json:"error,omitempty"`
}

func materializeMessages(rawEvents []json.RawMessage) []MaterializedMessage {
	var msgs []MaterializedMessage
	var current *MaterializedMessage
	var currentEvents []json.RawMessage

	flushAssistant := func() {
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

		switch ev.Type {
		case "user_message":
			flushAssistant()
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

		case "result":
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			if ev.Result != nil {
				if ev.Result.Text != "" {
					current.Content = ev.Result.Text
				}
				current.Meta = ev.Result
			}
			flushAssistant()

		case "stream":
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			// Accumulate text from stream deltas
			if ev.Stream != nil && ev.Stream.Delta != nil {
				switch ev.Stream.Delta.Type {
				case msg.DeltaText:
					current.Content += ev.Stream.Delta.Text
				case msg.DeltaThinking:
					current.Thinking += ev.Stream.Delta.Thinking
				}
			}

		case "thinking":
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			if ev.Thinking != nil {
				current.Thinking += ev.Thinking.Text
			}

		case "tool_call":
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			if ev.ToolCall != nil {
				current.Tools = append(current.Tools, MaterializedTool{
					ToolID: ev.ToolCall.ToolID,
					Tool:   ev.ToolCall.Name,
					Input:  ev.ToolCall.Input,
				})
			}

		case "tool_result":
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
			if ev.ToolResult != nil {
				// Match by ToolID first, fall back to name
				matched := false
				for i := len(current.Tools) - 1; i >= 0; i-- {
					t := &current.Tools[i]
					if ev.ToolResult.ToolID != "" && t.ToolID == ev.ToolResult.ToolID {
						t.Output = ev.ToolResult.Output
						t.Error = ev.ToolResult.IsError
						matched = true
						break
					}
				}
				if !matched {
					for i := len(current.Tools) - 1; i >= 0; i-- {
						t := &current.Tools[i]
						if t.Tool == ev.ToolResult.Name && t.Output == "" {
							t.Output = ev.ToolResult.Output
							t.Error = ev.ToolResult.IsError
							break
						}
					}
				}
			}

		default:
			// System, error, session_state, plan, approval — all stored as events on current message
			if current == nil {
				current = newAssistantMsg(ev.Timestamp)
			}
			currentEvents = append(currentEvents, raw)
		}
	}

	// Flush any in-progress assistant message (still streaming)
	if current != nil {
		current.Done = false
		current.Events = currentEvents
		msgs = append(msgs, *current)
	}

	if msgs == nil {
		msgs = []MaterializedMessage{}
	}
	return msgs
}

func newAssistantMsg(ts time.Time) *MaterializedMessage {
	return &MaterializedMessage{
		Role:      "assistant",
		Timestamp: ts.Format(time.RFC3339),
	}
}
