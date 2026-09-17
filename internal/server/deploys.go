package server

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/kayushkin/log-store/internal/store"
)

// handleIngestDeploy records one deploy run. Called by deploy-log.sh, sourced
// into every repo's deploy.sh, from an EXIT trap so the row is written on
// success and on failure alike. log-store stores what it is told and measures
// nothing itself — the git facts and the installed revision are gathered in
// the repo being deployed.
func (s *Server) handleIngestDeploy(w http.ResponseWriter, r *http.Request) {
	var d store.Deploy
	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		http.Error(w, `{"error":"invalid deploy JSON"}`, http.StatusBadRequest)
		return
	}
	if d.Repo == "" {
		http.Error(w, `{"error":"missing repo"}`, http.StatusBadRequest)
		return
	}
	if d.Status == "" {
		http.Error(w, `{"error":"missing status"}`, http.StatusBadRequest)
		return
	}
	rowID, err := s.store.InsertDeploy(d)
	if err != nil {
		http.Error(w, `{"error":"store failed"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]int64{"id": rowID})
}

// handleListDeploys returns recorded deploys newest-first, filterable by repo
// and branch. This is what the deploy-drift guard and its judge read to decide
// whether the running code matches the last deploy that actually happened.
func (s *Server) handleListDeploys(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 0
	if l := q.Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil {
			limit = n
		}
	}
	deploys, err := s.store.ListDeploys(q.Get("repo"), q.Get("branch"), limit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, deploys)
}
