package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
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

// escapePathSegment makes a value safe to paste into a URL path as ONE segment.
//
// A session id is not this client's own value — every caller receives it from
// the bridge or reads it back out of a stored record, and nothing checks it.
// Pasted raw, an id holding "/" addresses a different endpoint, one holding "?"
// turns the rest of the path into a query, and one holding "#" drops the rest of
// the path entirely. None of those fail; they succeed, against the wrong URL.
//
// neturl.PathEscape, not neturl.QueryEscape: the two differ on a space, which
// QueryEscape writes as "+" and a path reads back as a literal plus. The query
// value in SessionsHoldingHarnessSessionID is QueryEscape'd and correctly so —
// which escaper is right depends on the position, not on the value.
func escapePathSegment(segment string) string {
	return neturl.PathEscape(segment)
}

// GetTurnState fetches the per-session turn state.
func (c *Client) GetTurnState(sessionID string) (TurnState, error) {
	var ts TurnState
	url := fmt.Sprintf("%s/api/v1/sessions/%s/turn-state", c.baseURL, escapePathSegment(sessionID))
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
	url := fmt.Sprintf("%s/api/v1/sessions/%s/events?after=%d", c.baseURL, escapePathSegment(sessionID), afterID)
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

// HeldSession mirrors store.HeldSession — one log-store session that already
// holds a transcript for the harness session id that was asked about.
type HeldSession struct {
	SessionID  string `json:"session_id"`
	EventCount int    `json:"event_count"`
	LastActive string `json:"last_active"`
}

// SessionsHoldingHarnessSessionID asks log-store whether it already holds a
// transcript for a harness-native session id, and returns every log-store
// session that does, newest activity first.
//
// This is the dedupe key for an import. Asking the local database instead is
// what let a throwaway gateway with a temporary database re-import 2,863
// transcripts into the production store: the check lived in a database the
// store could not see.
//
// An empty id is refused here rather than sent, because ” is a real stored
// value (every session whose events name no harness id) and would match
// thousands of unrelated rows.
func (c *Client) SessionsHoldingHarnessSessionID(harnessSessionID string) ([]HeldSession, error) {
	if harnessSessionID == "" {
		return nil, fmt.Errorf("SessionsHoldingHarnessSessionID: harness_session_id is required")
	}
	url := fmt.Sprintf(
		"%s/api/v1/sessions/by-harness-id?harness_session_id=%s",
		c.baseURL, neturl.QueryEscape(harnessSessionID),
	)
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("get sessions by harness id: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("log-store error: %s - %s", resp.Status, string(body))
	}
	var out struct {
		Sessions []HeldSession `json:"sessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode sessions by harness id: %w", err)
	}
	return out.Sessions, nil
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
