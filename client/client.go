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
