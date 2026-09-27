package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/log-store/internal/store"
)

// A 1x1 PNG, base64.
const onePixelPNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="

func imageToolResult(mediaType string) msg.Event {
	return msg.Event{
		Type: msg.EventToolResult, TurnID: "turn1", Timestamp: time.Now(),
		ToolResult: &msg.ToolResultEvent{ToolID: "t1", Name: "Read", Output: "read 1 image", Content: []msg.ContentBlock{
			{Type: msg.BlockImage, Image: &msg.ImageBlock{Source: msg.MediaSource{Kind: msg.MediaBase64, MediaType: mediaType, Data: onePixelPNG}}},
		}},
	}
}

func TestAToolResultEntryListsItsImagesWithoutTheirBytes(t *testing.T) {
	m := buildTurnModel("sess", []store.EventRow{mkRow(t, 1, imageToolResult("image/png"))}, false)
	entry := entryOfEvent(t, m, 1)
	if len(entry.ToolResultImages) != 1 || entry.ToolResultImages[0] != (ToolResultImage{Index: 0, MediaType: "image/png"}) {
		t.Fatalf("toolResultImages = %+v, want one png at index 0", entry.ToolResultImages)
	}
	projected, err := json.Marshal(struct {
		Images []ToolResultImage `json:"toolResultImages"`
	}{entry.ToolResultImages})
	if err != nil {
		t.Fatal(err)
	}
	if len(projected) > 100 {
		t.Errorf("the image reference is %d bytes; it must not carry the image", len(projected))
	}
}

func TestASharedFileIsConversationSaidByWhoeverSharedIt(t *testing.T) {
	for sharedBy, wantRole := range map[msg.SessionFileSharer]string{
		msg.SessionFileSharedByUser:  "user",
		msg.SessionFileSharedByAgent: "assistant",
	} {
		file := &msg.SessionFile{FileID: "file_000001", Filename: "chart.png", MediaType: "image/png", SharedBy: sharedBy}
		m := buildTurnModel("sess", []store.EventRow{mkRow(t, 1, msg.Event{Type: msg.EventSessionFile, Timestamp: time.Now(), SessionFile: file})}, false)
		entry := entryOfEvent(t, m, 1)
		if entry.Role != wantRole || entry.Kind != "file" || entry.SessionFile == nil || entry.SessionFile.FileID != "file_000001" {
			t.Errorf("shared by %s: entry = role %q kind %q file %+v", sharedBy, entry.Role, entry.Kind, entry.SessionFile)
		}
	}
}

func entryOfEvent(t *testing.T, m TurnModel, eventID int64) Entry {
	t.Helper()
	for _, entry := range m.Entries {
		if entry.EventID == eventID {
			return entry
		}
	}
	t.Fatalf("no entry for event %d", eventID)
	return Entry{}
}

func storeEvent(t *testing.T, s *store.Store, sessionID string, ev msg.Event) int64 {
	t.Helper()
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.StoreEvent(sessionID, string(ev.Type), data)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestTheImageRouteServesTheBytesWithTheSafetyHeaders(t *testing.T) {
	srv, s := newTestServer(t)
	png := storeEvent(t, s, "br_1", imageToolResult("image/png"))
	svg := storeEvent(t, s, "br_1", imageToolResult("image/svg+xml"))

	get := func(sessionID string, eventID int64, index int) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		path := "/api/v1/sessions/" + sessionID + "/entries/" + strconv.FormatInt(eventID, 10) + "/images/" + strconv.Itoa(index)
		srv.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}

	answer := get("br_1", png, 0)
	if answer.Code != http.StatusOK || answer.Header().Get("Content-Type") != "image/png" {
		t.Fatalf("png: %d %q", answer.Code, answer.Header().Get("Content-Type"))
	}
	if answer.Body.Len() == 0 || answer.Body.Bytes()[1] != 'P' {
		t.Errorf("png body is not the decoded image")
	}
	if answer.Header().Get("X-Content-Type-Options") != "nosniff" || answer.Header().Get("Content-Security-Policy") == "" {
		t.Errorf("png is missing its safety headers: %v", answer.Header())
	}

	// A type a browser runs is a download, whatever the tool said it was.
	answer = get("br_1", svg, 0)
	if answer.Header().Get("Content-Type") != "application/octet-stream" || answer.Header().Get("Content-Disposition") != "attachment" {
		t.Errorf("svg served as %q / %q", answer.Header().Get("Content-Type"), answer.Header().Get("Content-Disposition"))
	}

	if code := get("br_1", png, 1).Code; code != http.StatusNotFound {
		t.Errorf("index past the images: %d, want 404", code)
	}
	// An event is read only under its own session.
	if code := get("br_2", png, 0).Code; code != http.StatusNotFound {
		t.Errorf("another session's event: %d, want 404", code)
	}
}
