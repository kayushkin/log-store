package server

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

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
	// The unprojected model, for the Raw pane and for audit. Its own route rather
	// than a query param on the line above so the two answers cache separately and
	// a caller cannot ask for the 10x payload by accident. See project.go.
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/messages/raw", srv.handleMessagesRaw)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/history", srv.handleHistory)
	// chat-page endpoints — turn-model materialization + validators.
	srv.mux.HandleFunc("GET /api/v1/sessions/validators", srv.handleValidators)
	srv.mux.HandleFunc("GET /api/v1/sessions/bundle", srv.handleBundle)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/events", srv.handleEvents)
	srv.mux.HandleFunc("GET /api/v1/sessions/{id}/turn-state", srv.handleTurnState)
	// Deliberately a sibling of search/aggregates/validators/bundle rather
	// than /sessions/by-harness-id/{id}: a five-segment pattern with a
	// literal in the fourth position overlaps /sessions/{id}/messages, and
	// Go's ServeMux PANICS at registration on an ambiguous pair.
	srv.mux.HandleFunc("GET /api/v1/sessions/by-harness-id", srv.handleSessionsByHarnessID)
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

// handleMessages returns materialized messages for a session.
//
// Two shapes, selected additively by query params so existing callers are
// unaffected:
//   - No `limit`/`before`: the legacy shape — a []MaterializedMessage array
//     over the FULL event stream. Byte-for-byte the prior behavior; existing
//     consumers (bridge-ui BridgeSessions) still work.
//   - `limit` and/or `before` present: the chat-page shape — a bounded, annotated
//     TurnModel ({ model }). Default returns the last `limit` turns; `before`
//     pages older. NEVER unbounded (see store.maxEventsPerPage) — the legacy
//     full-stream materialize is what produced 306MB/85s for one session.
func (s *Server) handleMessages(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()
	limitStr := q.Get("limit")
	beforeStr := q.Get("before")

	if limitStr == "" && beforeStr == "" {
		// Legacy path — unchanged.
		rawEvents, err := s.store.ListEvents(id, nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		msgs := materializeMessages(rawEvents)
		writeJSON(w, msgs)
		return
	}

	limit := 30
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
			limit = n
		}
	}
	var before int64
	if beforeStr != "" {
		if n, err := strconv.ParseInt(beforeStr, 10, 64); err == nil && n > 0 {
			before = n
		}
	}
	model, err := s.materializeTail(id, limit, before)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, MessagesResponse{Model: projectForReading(model)})
}

// handleMessagesRaw serves the UNPROJECTED turn model: every entry the window
// holds, duplicates included, each carrying its full `raw` source event.
//
// This is what `/messages` used to serve, and it is what the Raw pane and any audit
// reader needs — a page it can reconstruct the event stream from. It is a separate
// route because it is roughly ten times the size (measured 9.91 MB against 0.92 MB on
// one real session), so asking for it has to be deliberate.
//
// Same bounds as `/messages`: `limit` defaults to 30 and `before` pages older. There
// is no unbounded shape here — the legacy full-stream materialize is what produced
// 306 MB and 85 seconds for one session, and it is not reachable from this route.
func (s *Server) handleMessagesRaw(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	q := r.URL.Query()

	limit := 30
	if n, err := strconv.Atoi(q.Get("limit")); err == nil && n > 0 {
		limit = n
	}
	var before int64
	if n, err := strconv.ParseInt(q.Get("before"), 10, 64); err == nil && n > 0 {
		before = n
	}

	model, err := s.materializeTail(id, limit, before)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, MessagesResponse{Model: model})
}

// materializeTail assembles the bounded, annotated TurnModel for a session's
// tail (or a page older than `before`). This is the settled-history dedup owner
// (D4/D9): it groups events into turns and annotates duplicate/primary/source,
// but emits every event in the window.
func (s *Server) materializeTail(id string, limit int, before int64) (TurnModel, error) {
	rows, more, err := s.store.EventPage(id, limit, before)
	if err != nil {
		return TurnModel{}, err
	}
	model := buildTurnModel(id, rows, more)
	// Overlay the whole-session validator so the client can staleness-check the
	// tail against a cheap /validators sweep (page-local counts are not
	// comparable to the session-wide validator).
	if vs, verr := s.store.Validators([]string{id}); verr == nil {
		if v, ok := vs[id]; ok {
			model.Validator = toWireValidator(v)
		}
	}
	return model, nil
}

// handleValidators returns the per-session validator for a comma-separated set
// of ids: GET /api/v1/sessions/validators?ids=a,b,c → { [id]: Validator }.
func (s *Server) handleValidators(w http.ResponseWriter, r *http.Request) {
	ids := splitIDs(r.URL.Query().Get("ids"))
	if len(ids) == 0 {
		writeJSON(w, map[string]Validator{})
		return
	}
	vs, err := s.store.Validators(ids)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make(map[string]Validator, len(vs))
	for id, v := range vs {
		out[id] = toWireValidator(v)
	}
	writeJSON(w, out)
}

// handleBundle returns a materialized tail TurnModel for each requested session
// in one response: GET /api/v1/sessions/bundle?ids=a,b,c&turns=30 →
// { [id]: TurnModel }. llm-bridge-server assembles the summary+model recent
// bundle on top of this.
func (s *Server) handleBundle(w http.ResponseWriter, r *http.Request) {
	ids := splitIDs(r.URL.Query().Get("ids"))
	turns := 30
	if t := r.URL.Query().Get("turns"); t != "" {
		if n, err := strconv.Atoi(t); err == nil && n > 0 {
			turns = n
		}
	}
	out := make(map[string]TurnModel, len(ids))
	for _, id := range ids {
		model, err := s.materializeTail(id, turns, 0)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		// Projected for the same reason `/messages` is, and it matters more here: the
		// bundle is the COLD-BOOT payload and it multiplies by the session count.
		// Measured 2026-08-25, `recent-bundle?n=20&turns=30` was 29.1 MB in a single
		// response before this.
		out[id] = projectForReading(model)
	}
	writeJSON(w, out)
}

// toWireValidator maps a store validator to the wire shape (RFC3339+offset).
func toWireValidator(v store.SessionValidator) Validator {
	updated := ""
	if !v.UpdatedAt.IsZero() {
		updated = v.UpdatedAt.Format(time.RFC3339)
	}
	return Validator{MaxEventID: v.MaxEventID, EventCount: v.EventCount, UpdatedAt: updated}
}

// splitIDs parses a comma-separated id list, trimming blanks.
func splitIDs(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
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
	// Optional RFC3339 window. A malformed bound is a 400, never a silently
	// unbounded scan: the caller asked for a window precisely because the
	// unbounded scan takes ~50s on this host, and handing them that scan in
	// place of an error would look like a hang.
	var since, until time.Time
	if v := r.URL.Query().Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, `{"error":"since must be RFC3339"}`, http.StatusBadRequest)
			return
		}
		since = t
	}
	if v := r.URL.Query().Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			http.Error(w, `{"error":"until must be RFC3339"}`, http.StatusBadRequest)
			return
		}
		until = t
	}
	hits, err := s.store.SearchSessionsInWindow(q, limit, since, until)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if hits == nil {
		hits = []store.SearchHit{}
	}
	writeJSON(w, hits)
}

// handleSessionsByHarnessID answers whether log-store already holds a
// transcript for a harness-native session id, and under which of its own
// session ids.
//
// It exists for llm-bridge-server's discovery import, which today asks its
// own database whether a session is new and writes the answer's consequence
// here. A caller can now put the question to the store that owns the write.
//
// A missing or empty harness_session_id is a 400, not an empty list: '' is
// what every session whose events name no harness id carries, so answering
// it would be answering a different question.
func (s *Server) handleSessionsByHarnessID(w http.ResponseWriter, r *http.Request) {
	harnessSessionID := r.URL.Query().Get("harness_session_id")
	if harnessSessionID == "" {
		http.Error(w, `{"error":"harness_session_id is required"}`, http.StatusBadRequest)
		return
	}
	held, err := s.store.SessionsHoldingHarnessSessionID(harnessSessionID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if held == nil {
		held = []store.HeldSession{}
	}
	writeJSON(w, map[string]any{
		"harness_session_id": harnessSessionID,
		"sessions":           held,
	})
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
