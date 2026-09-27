package server

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/kayushkin/llm-bridge/msg"
)

// drawableImageTypes are the image types served as themselves. Anything else a
// tool result claims to be an image is served as a download: SVG in particular
// is a document that runs script, and the type comes from whatever produced
// the tool result, not from this service.
var drawableImageTypes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/gif":  true,
	"image/webp": true,
}

// handleEntryImage serves the bytes of one image a tool result carries:
// GET /api/v1/sessions/{id}/entries/{eventId}/images/{index}, where index is the
// image's position in ToolResultEvent.Content (Entry.toolResultImages says which
// positions hold one).
//
// The headers are file-store's for the same reason: the bytes came from a tool,
// and they are shown from the dashboard's origin. A stored event never changes,
// so the answer may be cached for good.
func (s *Server) handleEntryImage(w http.ResponseWriter, r *http.Request) {
	sessionID := r.PathValue("id")
	eventID, err := strconv.ParseInt(r.PathValue("eventId"), 10, 64)
	if err != nil || eventID <= 0 {
		http.Error(w, "eventId must be a positive integer", http.StatusBadRequest)
		return
	}
	index, err := strconv.Atoi(r.PathValue("index"))
	if err != nil || index < 0 {
		http.Error(w, "index must be a non-negative integer", http.StatusBadRequest)
		return
	}
	events, err := s.store.EventsByID(sessionID, []int64{eventID})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, found := events[eventID]
	if !found {
		http.Error(w, "no such event", http.StatusNotFound)
		return
	}
	var event msg.Event
	if err := json.Unmarshal(data, &event); err != nil {
		http.Error(w, "stored event does not parse: "+err.Error(), http.StatusInternalServerError)
		return
	}
	if event.ToolResult == nil || index >= len(event.ToolResult.Content) {
		http.Error(w, "no such image", http.StatusNotFound)
		return
	}
	block := event.ToolResult.Content[index]
	if block.Type != msg.BlockImage || block.Image == nil {
		http.Error(w, "no such image", http.StatusNotFound)
		return
	}
	source := block.Image.Source
	if source.Kind != msg.MediaBase64 {
		// A URL source names bytes this service does not hold; the client has
		// the URL on the live event and nothing here would be truer than it.
		http.Error(w, "image is held by reference ("+string(source.Kind)+"), not stored here", http.StatusNotFound)
		return
	}
	imageBytes, err := base64.StdEncoding.DecodeString(source.Data)
	if err != nil {
		http.Error(w, "stored image is not valid base64: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "sandbox; default-src 'none'")
	w.Header().Set("Cache-Control", "private, max-age=31536000, immutable")
	if drawableImageTypes[source.MediaType] {
		w.Header().Set("Content-Type", source.MediaType)
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Disposition", "attachment")
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(imageBytes)))
	w.Write(imageBytes)
}
