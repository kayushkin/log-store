package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
)

// Client pushes events to log-store.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 5 * time.Second,
		},
	}
}

// PushEvent sends a single event to log-store. Returns the stored row ID.
func (c *Client) PushEvent(ev msg.Event) (int64, error) {
	data, err := json.Marshal(ev)
	if err != nil {
		return 0, fmt.Errorf("marshal event: %w", err)
	}

	resp, err := c.httpClient.Post(
		c.baseURL+"/api/v1/events",
		"application/json",
		bytes.NewReader(data),
	)
	if err != nil {
		return 0, fmt.Errorf("post event: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("log-store error: %s - %s", resp.Status, string(body))
	}

	var result struct {
		ID int64 `json:"id"`
	}
	json.NewDecoder(resp.Body).Decode(&result)
	return result.ID, nil
}

// TurnState mirrors store.TurnState — the in-flight turn snapshot for a
// session derived from event boundaries.
type TurnState struct {
	LastUserMessageEventID int64 `json:"last_user_message_event_id"`
	LastTerminatorEventID  int64 `json:"last_terminator_event_id"`
	InFlight               bool  `json:"in_flight"`
}

// GetTurnState fetches the per-session turn state.
func (c *Client) GetTurnState(sessionID string) (TurnState, error) {
	var ts TurnState
	url := fmt.Sprintf("%s/api/v1/sessions/%s/turn-state", c.baseURL, sessionID)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return ts, fmt.Errorf("get turn-state: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return ts, fmt.Errorf("log-store error: %s - %s", resp.Status, string(body))
	}
	if err := json.NewDecoder(resp.Body).Decode(&ts); err != nil {
		return ts, fmt.Errorf("decode turn-state: %w", err)
	}
	return ts, nil
}

// ListEvents fetches raw events for a session, optionally filtered by event
// type and/or after a row ID. Pass afterID == 0 to read from the beginning,
// types == nil to disable type filtering.
func (c *Client) ListEvents(sessionID string, afterID int64, types []string) ([]json.RawMessage, error) {
	url := fmt.Sprintf("%s/api/v1/sessions/%s/events?after=%d", c.baseURL, sessionID, afterID)
	if len(types) > 0 {
		url += "&types=" + joinTypes(types)
	}
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get events: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("log-store error: %s - %s", resp.Status, string(body))
	}
	var out []json.RawMessage
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode events: %w", err)
	}
	return out, nil
}

func joinTypes(types []string) string {
	out := ""
	for i, t := range types {
		if i > 0 {
			out += ","
		}
		out += t
	}
	return out
}
