package server

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/log-store/internal/store"
)

// The stored read path for /messages and /bundle.
//
// A page used to be built from raw events on every request. Now each turn is built
// ONCE by the same builder (buildTurnModel + projectForReading), its output stored
// in turns / turn_entries (store/turn_index.go), and a page is read back from those
// rows. A turn is rebuilt only when it holds an event newer than its stored copy,
// when the rules changed (materializerVersion), or when a dual-emitted copy that
// landed later — possibly in another turn — changed what its copies pair with
// (dedup.go).
//
// There is one builder and one set of rules. The stored rows are its output, never a
// second implementation of it, so a change to the rules reaches every page the
// moment the version is bumped.

// materializerVersion names the rules the stored turns were built by. A turn stored
// under any other version is rebuilt on its next read.
//
// ⚠️ BUMP IT whenever turnmodel.go, project.go, dedup.go or this file changes what a built
// turn contains. TestMaterializerVersionTracksTheRules fails until you do: it
// fingerprints those files and compares against materializerRulesFingerprint.
const materializerVersion = 1

// toolPayloadPreviewBytes is the most of any one tool string a preview page carries.
const toolPayloadPreviewBytes = 2048

// payloadMode chooses how tool input and output come back on a page.
type payloadMode int

const (
	// payloadFull puts the complete tool input and output on every entry, read
	// back from the events. What every caller got before previews existed.
	payloadFull payloadMode = iota
	// payloadPreview carries at most toolPayloadPreviewBytes of each tool string
	// and marks what was cut. The full entry is one request away.
	payloadPreview
)

func parsePayloadMode(raw string) (payloadMode, error) {
	switch raw {
	case "", "full":
		return payloadFull, nil
	case "preview":
		return payloadPreview, nil
	}
	return 0, fmt.Errorf("payload must be full or preview, got %q", raw)
}

// storedTail returns the projected page of a session's newest turns — or of the
// turns older than the one holding `before` — read from stored turns, building any
// that are missing or stale first.
func (s *Server) storedTail(sessionID string, limit int, before int64, mode payloadMode) (TurnModel, error) {
	if err := s.store.BuildTurnIndex(sessionID); err != nil {
		return TurnModel{}, fmt.Errorf("index turns of %s: %w", sessionID, err)
	}
	window, more, err := s.store.TurnWindow(sessionID, limit, before)
	if err != nil {
		return TurnModel{}, fmt.Errorf("choose turns of %s: %w", sessionID, err)
	}

	model := TurnModel{SessionID: sessionID, Turns: []Turn{}, Entries: map[string]Entry{}, More: more}
	if len(window) > 0 {
		pairing, err := s.sessionPairing(sessionID)
		if err != nil {
			return TurnModel{}, err
		}
		for i, turn := range window {
			if !turn.NeedsMaterializing(materializerVersion, pairing.fingerprint(turn.Seq)) {
				continue
			}
			built, err := s.materializeStoredTurn(sessionID, turn, pairing)
			if err != nil {
				return TurnModel{}, err
			}
			window[i].TurnJSON = built.TurnJSON
			window[i].SourceGroupsJSON = built.SourceGroupsJSON
			window[i].AggregateSourcesJSON = built.AggregateSourcesJSON
		}

		stored, err := s.store.StoredEntries(sessionID, window[0].Seq, window[len(window)-1].Seq)
		if err != nil {
			return TurnModel{}, fmt.Errorf("read stored entries of %s: %w", sessionID, err)
		}
		if err := assembleStoredPage(&model, window, stored); err != nil {
			return TurnModel{}, fmt.Errorf("assemble page of %s: %w", sessionID, err)
		}
		switch mode {
		case payloadFull:
			if err := s.expandToolPayloads(sessionID, model.Entries); err != nil {
				return TurnModel{}, err
			}
		case payloadPreview:
			// The stored entries already are previews.
		}
	}

	validators, err := s.store.Validators([]string{sessionID})
	if err != nil {
		return TurnModel{}, fmt.Errorf("validator for %s: %w", sessionID, err)
	}
	model.Validator = toWireValidator(validators[sessionID])
	return model, nil
}

// assembleStoredPage turns stored turn and entry rows into a projected TurnModel.
func assembleStoredPage(model *TurnModel, window []store.StoredTurn, stored []store.StoredEntry) error {
	var latest aggregateSources
	var haveSpend, haveContext bool
	for _, turn := range window {
		var t Turn
		if err := json.Unmarshal([]byte(turn.TurnJSON), &t); err != nil {
			return fmt.Errorf("stored turn %d: %w", turn.Seq, err)
		}
		model.Turns = append(model.Turns, t)

		if turn.SourceGroupsJSON != "" {
			var groups SourceGroups
			if err := json.Unmarshal([]byte(turn.SourceGroupsJSON), &groups); err != nil {
				return fmt.Errorf("stored source groups of turn %d: %w", turn.Seq, err)
			}
			if model.SourceGroups == nil && len(groups) > 0 {
				model.SourceGroups = SourceGroups{}
			}
			for id, sources := range groups {
				model.SourceGroups[id] = sources
			}
		}

		var sources aggregateSources
		if err := json.Unmarshal([]byte(turn.AggregateSourcesJSON), &sources); err != nil {
			return fmt.Errorf("stored aggregate sources of turn %d: %w", turn.Seq, err)
		}
		// Latest by event id across the page, as buildTurnModel takes it over events.
		if sources.Spend != nil && sources.SpendEventID > latest.SpendEventID {
			latest.SpendEventID, latest.Spend, haveSpend = sources.SpendEventID, sources.Spend, true
		}
		if sources.ContextEventID > latest.ContextEventID {
			latest.ContextEventID, latest.ContextTokens, latest.ContextLimit = sources.ContextEventID, sources.ContextTokens, sources.ContextLimit
			haveContext = true
		}
	}
	for _, row := range stored {
		var e Entry
		if err := json.Unmarshal([]byte(row.EntryJSON), &e); err != nil {
			return fmt.Errorf("stored entry for event %d: %w", row.EventID, err)
		}
		model.Entries[e.ID] = e
	}
	if !haveSpend {
		latest.Spend = nil
	}
	model.Aggregates = buildAggregates(latest.Spend, latest.ContextTokens, latest.ContextLimit, haveContext)
	return nil
}

// sessionPairing is a session's dual-emit pairs and which candidates sit in which
// turn — what a turn is built with, and what says a stored turn's pairs moved.
type sessionPairing struct {
	pairs            dedupPairs
	candidatesByTurn map[int64][]int64
}

func (p sessionPairing) fingerprint(turnSeq int64) string {
	return pairsFingerprint(p.candidatesByTurn[turnSeq], p.pairs)
}

func (s *Server) sessionPairing(sessionID string) (sessionPairing, error) {
	candidates, err := s.store.DedupCandidates(sessionID)
	if err != nil {
		return sessionPairing{}, fmt.Errorf("read dual-emit candidates of %s: %w", sessionID, err)
	}
	p := sessionPairing{pairs: pairDualEmits(candidates), candidatesByTurn: map[int64][]int64{}}
	for _, c := range candidates {
		p.candidatesByTurn[c.TurnSeq] = append(p.candidatesByTurn[c.TurnSeq], c.EventID)
	}
	return p, nil
}

// materializeStoredTurn builds one turn from its events and stores the result.
func (s *Server) materializeStoredTurn(sessionID string, turn store.StoredTurn, pairing sessionPairing) (store.MaterializedTurn, error) {
	rows, err := s.store.TurnEvents(sessionID, turn)
	if err != nil {
		return store.MaterializedTurn{}, fmt.Errorf("read events of turn %d of %s: %w", turn.Seq, sessionID, err)
	}
	if len(rows) == 0 {
		return store.MaterializedTurn{}, fmt.Errorf("turn %d of %s has no events", turn.Seq, sessionID)
	}
	full, sources := buildTurnModelWithAggregateSources(sessionID, rows, false, pairing.pairs)
	projected := projectForReading(full)
	if len(projected.Turns) != 1 {
		return store.MaterializedTurn{}, fmt.Errorf("turn %d of %s built into %d turns, want 1", turn.Seq, sessionID, len(projected.Turns))
	}

	built := store.MaterializedTurn{Seq: turn.Seq, Version: materializerVersion, DedupFingerprint: pairing.fingerprint(turn.Seq)}
	for _, r := range rows {
		if r.ID > built.ThroughEventID {
			built.ThroughEventID = r.ID
		}
	}
	turnJSON, err := json.Marshal(projected.Turns[0])
	if err != nil {
		return store.MaterializedTurn{}, err
	}
	built.TurnJSON = string(turnJSON)
	if len(projected.SourceGroups) > 0 {
		groupsJSON, err := json.Marshal(projected.SourceGroups)
		if err != nil {
			return store.MaterializedTurn{}, err
		}
		built.SourceGroupsJSON = string(groupsJSON)
	}
	sourcesJSON, err := json.Marshal(sources)
	if err != nil {
		return store.MaterializedTurn{}, err
	}
	built.AggregateSourcesJSON = string(sourcesJSON)

	for _, entryID := range projected.Turns[0].EntryIDs {
		e := projected.Entries[entryID]
		previewToolPayloads(&e)
		entryJSON, err := json.Marshal(e)
		if err != nil {
			return store.MaterializedTurn{}, err
		}
		built.Entries = append(built.Entries, store.StoredEntry{EventID: e.EventID, EntryJSON: string(entryJSON)})
	}

	if err := s.store.ReplaceMaterializedTurn(sessionID, built); err != nil {
		return store.MaterializedTurn{}, fmt.Errorf("store turn %d of %s: %w", turn.Seq, sessionID, err)
	}
	return built, nil
}

// previewToolPayloads shortens an entry's tool input and output to what a preview
// page carries, recording each original's size and whether it was cut.
//
// The stored entry never holds a full payload: the event already does, and a
// second copy is exactly the duplication this table must not add.
func previewToolPayloads(e *Entry) {
	if len(e.ToolInput) > 0 {
		e.ToolInputBytes = len(e.ToolInput)
		if len(e.ToolInput) > toolPayloadPreviewBytes {
			e.ToolInput, e.ToolInputTruncated = previewJSONValue(e.ToolInput)
		}
	}
	if len(e.ToolResult) > 0 {
		var output string
		if err := json.Unmarshal(e.ToolResult, &output); err == nil {
			e.ToolResultBytes = len(output)
			if len(output) > toolPayloadPreviewBytes {
				e.ToolResult = json.RawMessage(quoteJSON(truncateUTF8(output, toolPayloadPreviewBytes)))
				e.ToolResultTruncated = true
			}
		}
	}
}

// previewJSONValue shortens every string inside a JSON value to the preview size
// and keeps the structure, so a renderer can still read `command` or `file_path`
// off a shortened tool input. A value that is not valid JSON is left alone — the
// builder copies whatever the harness sent, and the preview must not invent a shape.
func previewJSONValue(raw json.RawMessage) (json.RawMessage, bool) {
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return raw, false
	}
	shortened, cut := shortenStrings(value)
	if !cut {
		return raw, false
	}
	out, err := json.Marshal(shortened)
	if err != nil {
		return raw, false
	}
	return out, true
}

func shortenStrings(value any) (any, bool) {
	switch v := value.(type) {
	case string:
		if len(v) > toolPayloadPreviewBytes {
			return truncateUTF8(v, toolPayloadPreviewBytes), true
		}
		return v, false
	case map[string]any:
		cut := false
		for k, inner := range v {
			shortened, c := shortenStrings(inner)
			v[k] = shortened
			cut = cut || c
		}
		return v, cut
	case []any:
		cut := false
		for i, inner := range v {
			shortened, c := shortenStrings(inner)
			v[i] = shortened
			cut = cut || c
		}
		return v, cut
	}
	return value, false
}

// truncateUTF8 cuts s to at most n bytes without splitting a character.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n]
}

// expandToolPayloads replaces every shortened tool payload with the full one, read
// from its event, and clears the preview marks — the page a full-payload caller
// has always received.
func (s *Server) expandToolPayloads(sessionID string, entries map[string]Entry) error {
	var ids []int64
	for _, e := range entries {
		if e.ToolInputTruncated || e.ToolResultTruncated {
			ids = append(ids, e.EventID)
		}
	}
	bodies, err := s.store.EventsByID(sessionID, ids)
	if err != nil {
		return fmt.Errorf("read full tool payloads of %s: %w", sessionID, err)
	}
	for id, e := range entries {
		if e.ToolInputTruncated || e.ToolResultTruncated {
			body, ok := bodies[e.EventID]
			if !ok {
				return fmt.Errorf("event %d of %s is gone; its stored entry cannot be expanded", e.EventID, sessionID)
			}
			var ev msg.Event
			if err := json.Unmarshal(body, &ev); err != nil {
				return fmt.Errorf("event %d of %s: %w", e.EventID, sessionID, err)
			}
			e.ToolInput, e.ToolResult = toolPayloads(&ev)
		}
		e.ToolInputBytes, e.ToolInputTruncated = 0, false
		e.ToolResultBytes, e.ToolResultTruncated = 0, false
		entries[id] = e
	}
	return nil
}

// fullStoredEntry returns one stored entry with its complete tool payloads.
func (s *Server) fullStoredEntry(sessionID string, eventID int64) (Entry, bool, error) {
	if err := s.store.BuildTurnIndex(sessionID); err != nil {
		return Entry{}, false, err
	}
	turn, found, err := s.store.TurnOfEvent(sessionID, eventID)
	if err != nil || !found {
		return Entry{}, false, err
	}
	pairing, err := s.sessionPairing(sessionID)
	if err != nil {
		return Entry{}, false, err
	}
	if turn.NeedsMaterializing(materializerVersion, pairing.fingerprint(turn.Seq)) {
		if _, err := s.materializeStoredTurn(sessionID, turn, pairing); err != nil {
			return Entry{}, false, err
		}
	}
	stored, err := s.store.StoredEntries(sessionID, turn.Seq, turn.Seq)
	if err != nil {
		return Entry{}, false, err
	}
	for _, row := range stored {
		if row.EventID != eventID {
			continue
		}
		var e Entry
		if err := json.Unmarshal([]byte(row.EntryJSON), &e); err != nil {
			return Entry{}, false, err
		}
		entries := map[string]Entry{e.ID: e}
		if err := s.expandToolPayloads(sessionID, entries); err != nil {
			return Entry{}, false, err
		}
		return entries[e.ID], true, nil
	}
	// The event exists but the projection hid it (a duplicate copy). Not an entry
	// of the reading page.
	return Entry{}, false, nil
}

// turnMaterializer builds the newest stale turns of sessions whose turns just
// ended, so the first read after a turn finds it stored. Reads never wait for it:
// a read that finds a stale turn builds that turn itself.
type turnMaterializer struct {
	server  *Server
	mu      sync.Mutex
	pending map[string]struct{}
	wake    chan struct{}
}

// turnEndingEventTypes are the events after which a turn is settled enough to build.
// Building on every event would rebuild a live turn once per stream frame.
var turnEndingEventTypes = map[msg.EventType]bool{
	msg.EventTurnComplete: true,
	msg.EventResult:       true,
	msg.EventError:        true,
}

func newTurnMaterializer(server *Server) *turnMaterializer {
	m := &turnMaterializer{server: server, pending: map[string]struct{}{}, wake: make(chan struct{}, 1)}
	go m.run()
	return m
}

func (m *turnMaterializer) noteEvent(sessionID string, eventType msg.EventType) {
	if !turnEndingEventTypes[eventType] {
		return
	}
	m.mu.Lock()
	m.pending[sessionID] = struct{}{}
	m.mu.Unlock()
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// staleTurnsPerSession bounds how many turns one pass builds for a session. A turn
// that ended usually leaves one stale turn; the bound keeps an old session that
// resumes from rebuilding its whole history here instead of page by page on read.
const staleTurnsPerSession = 3

// materializeNewestStaleTurns builds whichever of a session's `limit` newest turns
// are stale.
func (s *Server) materializeNewestStaleTurns(sessionID string, limit int) error {
	turns, err := s.store.NewestTurns(sessionID, limit)
	if err != nil {
		return err
	}
	pairing, err := s.sessionPairing(sessionID)
	if err != nil {
		return err
	}
	for _, turn := range turns {
		if !turn.NeedsMaterializing(materializerVersion, pairing.fingerprint(turn.Seq)) {
			continue
		}
		if _, err := s.materializeStoredTurn(sessionID, turn, pairing); err != nil {
			return err
		}
	}
	return nil
}

func (m *turnMaterializer) run() {
	for range m.wake {
		// Let the rest of a turn's closing events (result, usage, turn_complete)
		// land before building it once.
		time.Sleep(500 * time.Millisecond)
		m.mu.Lock()
		sessions := m.pending
		m.pending = map[string]struct{}{}
		m.mu.Unlock()
		for sessionID := range sessions {
			current, err := m.server.store.TurnIndexCurrent(sessionID)
			if err != nil {
				log.Printf("[log-store] turn materializer: %s: %v", sessionID, err)
				continue
			}
			if !current {
				continue // the index backfill has not reached it; a read builds it on demand
			}
			if err := m.server.materializeNewestStaleTurns(sessionID, staleTurnsPerSession); err != nil {
				log.Printf("[log-store] turn materializer: %s: %v", sessionID, err)
			}
		}
	}
}

// BackfillTurnIndex indexes every session that has events but no turn index, most
// recently active first, and returns when none is left. It builds the INDEX only;
// turns are built when first read. A read that reaches an unindexed session first
// indexes that session itself.
func (s *Server) BackfillTurnIndex() error {
	started := time.Now()
	indexed := 0
	for {
		sessions, err := s.store.SessionsAwaitingTurnIndex(200)
		if err != nil {
			return fmt.Errorf("list sessions awaiting a turn index: %w", err)
		}
		if len(sessions) == 0 {
			log.Printf("[log-store] turn index backfill done: %d sessions in %s", indexed, time.Since(started).Round(time.Second))
			return nil
		}
		for _, sessionID := range sessions {
			if err := s.store.BuildTurnIndex(sessionID); err != nil {
				return fmt.Errorf("index turns of %s: %w", sessionID, err)
			}
			indexed++
			if indexed%1000 == 0 {
				log.Printf("[log-store] turn index backfill: %d sessions in %s", indexed, time.Since(started).Round(time.Second))
			}
		}
	}
}
