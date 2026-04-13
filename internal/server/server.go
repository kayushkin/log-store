package server

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"

	ls "github.com/kayushkin/log-store/internal/logstack"
	"github.com/kayushkin/log-store/internal/store"
	"github.com/kayushkin/llm-bridge/msg"
)

type Server struct {
	store     *store.Store
	forwarder *ls.Forwarder
	mux       *http.ServeMux
}

func New(s *store.Store, forwarder *ls.Forwarder) *Server {
	srv := &Server{store: s, forwarder: forwarder, mux: http.NewServeMux()}
	srv.mux.HandleFunc("POST /api/v1/events", srv.handleIngestEvent)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/messages", srv.handleMessages)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/history", srv.handleHistory)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/events", srv.handleEvents)
	srv.mux.HandleFunc("GET /health", srv.handleHealth)
	return srv
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

// handleIngestEvent accepts a single msg.Event, stores it verbatim, and forwards result events to logstack.
func (s *Server) handleIngestEvent(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error":"read body"}`, http.StatusBadRequest)
		return
	}

	// Parse just enough to extract session_id and type for indexing
	var ev msg.Event
	if err := json.Unmarshal(body, &ev); err != nil {
		http.Error(w, `{"error":"invalid event JSON"}`, http.StatusBadRequest)
		return
	}

	if ev.SessionID == "" {
		http.Error(w, `{"error":"missing session_id"}`, http.StatusBadRequest)
		return
	}

	// Store the raw body verbatim — no re-serialization
	rowID, err := s.store.StoreEvent(ev.SessionID, string(ev.Type), body)
	if err != nil {
		log.Printf("[log-store] store error: %v", err)
		http.Error(w, `{"error":"store failed"}`, http.StatusInternalServerError)
		return
	}

	// Forward result events to logstack
	if ev.Type == msg.EventResult && ev.Result != nil {
		s.forwarder.Forward(ev)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]int64{"id": rowID})
}

// handleMessages returns materialized messages for a session.
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rawEvents, err := s.store.ListEvents(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	msgs := materializeMessages(rawEvents)
	writeJSON(w, msgs)
}

// handleHistory returns raw stored events for a session.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	events, err := s.store.ListEvents(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	writeJSON(w, events)
}

// handleEvents returns events after a given row ID (for polling/reconnection).
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	afterStr := r.URL.Query().Get("after")
	afterID := 0
	if afterStr != "" {
		afterID, _ = strconv.Atoi(afterStr)
	}

	events, err := s.store.ListEventsSinceID(id, afterID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	writeJSON(w, events)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
