package server

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"

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
	srv.mux.HandleFunc("GET /api/v1/sessions/search", srv.handleSearch)
	srv.mux.HandleFunc("GET /api/v1/sessions/aggregates", srv.handleAggregates)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/messages", srv.handleMessages)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/history", srv.handleHistory)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/events", srv.handleEvents)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/turn-state", srv.handleTurnState)
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

	if ev.BridgeSessionID == "" {
		http.Error(w, `{"error":"missing bridge_session_id"}`, http.StatusBadRequest)
		return
	}
	storeID := ev.BridgeSessionID

	// Store the raw body verbatim — no re-serialization
	rowID, err := s.store.StoreEvent(storeID, string(ev.Type), body)
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

// handleMessages returns materialized messages for a session. Materialization
// always reads the full event stream — no types filter applied.
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rawEvents, err := s.store.ListEvents(id, nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	msgs := materializeMessages(rawEvents)
	writeJSON(w, msgs)
}

// handleHistory returns raw stored events for a session, optionally filtered
// by ?types=foo,bar.
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	events, err := s.store.ListEvents(id, parseTypes(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	writeJSON(w, events)
}

// handleEvents returns events after a given row ID (for polling/reconnection),
// optionally filtered by ?types=foo,bar.
func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	afterStr := r.URL.Query().Get("after")
	afterID := 0
	if afterStr != "" {
		afterID, _ = strconv.Atoi(afterStr)
	}

	events, err := s.store.ListEventsSinceID(id, afterID, parseTypes(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if events == nil {
		events = []json.RawMessage{}
	}
	writeJSON(w, events)
}

// handleTurnState returns the in-flight turn state for a session, derived from
// the latest user_message and result/error events.
func (s *Server) handleTurnState(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	ts, err := s.store.TurnState(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, ts)
}

// parseTypes splits the ?types= query into a non-empty slice of event types,
// or returns nil if the parameter is absent / empty (callers treat nil as
// "no filter").
func parseTypes(r *http.Request) []string {
	raw := r.URL.Query().Get("types")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// handleSearch returns session IDs whose events contain the query substring.
func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("q")
	if q == "" {
		writeJSON(w, []store.SearchHit{})
		return
	}
	limit := 100
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	hits, err := s.store.SearchSessions(q, limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if hits == nil {
		hits = []store.SearchHit{}
	}
	writeJSON(w, hits)
}

// handleAggregates returns per-session token/cost totals summed from the
// stored result events.
func (s *Server) handleAggregates(w http.ResponseWriter, r *http.Request) {
	rows, err := s.store.ListSessionAggregates()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]msg.SessionAggregate, 0, len(rows))
	for _, r := range rows {
		out = append(out, msg.SessionAggregate{
			SessionID:    r.SessionID,
			Turns:        r.Turns,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			CostUSD:      r.CostUSD,
			DurationMS:   r.DurationMS,
			Model:        r.Model,
		})
	}
	writeJSON(w, out)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
