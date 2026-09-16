package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/kayushkin/log-store/internal/store"
)

// ingestJSON stores an event body through the store, as the ingest handler does.
func ingestJSON(t *testing.T, s *store.Store, sessionID, typ string, body map[string]any) int64 {
	t.Helper()
	body["type"] = typ
	body["bridge_session_id"] = sessionID
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.StoreEvent(sessionID, typ, data)
	if err != nil {
		t.Fatalf("StoreEvent: %v", err)
	}
	return id
}

// seedConversation writes three turns with an OTel echo, a narration block, a tool
// call with a large input and a large result, spend and context readings.
func seedConversation(t *testing.T, s *store.Store, sessionID string) (bigResultEventID int64) {
	big := strings.Repeat("grep hit\n", 1000) // 9 KB
	for i, turn := range []string{"turn-1", "turn-2", "turn-3"} {
		q := "question " + turn
		ingestJSON(t, s, sessionID, "user_message", map[string]any{"turn_id": turn, "message_id": "m-u-" + turn, "result": map[string]any{"text": q}})
		ingestJSON(t, s, sessionID, "user_message", map[string]any{"turn_id": turn, "result": map[string]any{"text": q}, "extensions": map[string]any{"source": "otel"}})
		ingestJSON(t, s, sessionID, "block", map[string]any{"turn_id": turn, "message_id": "m-a-" + turn, "block": map[string]any{"block": map[string]any{"type": "text", "text": "let me look"}}})
		ingestJSON(t, s, sessionID, "tool_call", map[string]any{"turn_id": turn, "message_id": "m-a-" + turn, "tool_call": map[string]any{"tool_id": "tool-" + turn, "name": "Bash", "input": map[string]any{"command": "grep -r x", "description": big}}})
		id := ingestJSON(t, s, sessionID, "tool_result", map[string]any{"turn_id": turn, "message_id": "m-t-" + turn, "tool_result": map[string]any{"tool_id": "tool-" + turn, "name": "Bash", "output": big}})
		if i == 1 {
			bigResultEventID = id
		}
		ingestJSON(t, s, sessionID, "api_spend_total", map[string]any{"turn_id": turn, "api_spend_total": map[string]any{"total_usd": float64(i + 1)}})
		ingestJSON(t, s, sessionID, "result", map[string]any{"turn_id": turn, "message_id": "m-r-" + turn, "result": map[string]any{"text": "answer " + turn, "usage": map[string]any{"context_tokens": 1000 * (i + 1), "context_limit": 200000}}})
		ingestJSON(t, s, sessionID, "turn_complete", map[string]any{"turn_id": turn})
	}
	return bigResultEventID
}

// jsonDiff lists the JSON paths where two values differ, with short excerpts.
func jsonDiff(t *testing.T, got, want any) []string {
	t.Helper()
	toGeneric := func(v any) any {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		var g any
		if err := json.Unmarshal(b, &g); err != nil {
			t.Fatal(err)
		}
		return g
	}
	var diffs []string
	excerpt := func(v any) string {
		b, _ := json.Marshal(v)
		if len(b) > 120 {
			return string(b[:120]) + "…"
		}
		return string(b)
	}
	var walk func(path string, g, w any)
	walk = func(path string, g, w any) {
		if len(diffs) >= 20 {
			return
		}
		gm, gok := g.(map[string]any)
		wm, wok := w.(map[string]any)
		if gok && wok {
			for k := range wm {
				walk(path+"."+k, gm[k], wm[k])
			}
			for k := range gm {
				if _, ok := wm[k]; !ok {
					walk(path+"."+k, gm[k], nil)
				}
			}
			return
		}
		ga, gok := g.([]any)
		wa, wok := w.([]any)
		if gok && wok && len(ga) == len(wa) {
			for i := range wa {
				walk(path+"["+itoa(int64(i))+"]", ga[i], wa[i])
			}
			return
		}
		if excerpt(g) != excerpt(w) || canonical(g) != canonical(w) {
			diffs = append(diffs, path+": got "+excerpt(g)+" want "+excerpt(w))
		}
	}
	walk("", toGeneric(got), toGeneric(want))
	return diffs
}

func canonical(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

// The stored page with full payloads is exactly what building the same window
// from events produces. This is the equivalence the whole stored path rests on.
func TestStoredTail_FullPayloadMatchesBuildingFromEvents(t *testing.T) {
	srv, s := newTestServer(t)
	seedConversation(t, s, "conv")

	fromEvents, err := srv.materializeTail("conv", 30, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := projectForReading(fromEvents)

	for pass := 0; pass < 2; pass++ { // first read builds the turns, second reads them back
		got, err := srv.storedTail("conv", 30, 0, payloadFull)
		if err != nil {
			t.Fatal(err)
		}
		if diffs := jsonDiff(t, got, want); len(diffs) > 0 {
			t.Fatalf("pass %d: stored page differs from building from events:\n%s", pass, strings.Join(diffs, "\n"))
		}
	}
}

func TestStoredTail_PreviewShortensToolPayloadsAndEntryRouteExpandsThem(t *testing.T) {
	srv, s := newTestServer(t)
	bigID := seedConversation(t, s, "conv")

	page, err := srv.storedTail("conv", 30, 0, payloadPreview)
	if err != nil {
		t.Fatal(err)
	}
	var result Entry
	for _, e := range page.Entries {
		if e.EventID == bigID {
			result = e
		}
	}
	if !result.ToolResultTruncated || result.ToolResultBytes != 9000 {
		t.Fatalf("preview result: truncated=%v bytes=%d; want true, 9000", result.ToolResultTruncated, result.ToolResultBytes)
	}
	var preview string
	if err := json.Unmarshal(result.ToolResult, &preview); err != nil || len(preview) != toolPayloadPreviewBytes {
		t.Fatalf("preview length %d (%v), want %d", len(preview), err, toolPayloadPreviewBytes)
	}
	for _, e := range page.Entries {
		if e.Kind == "tool_call" {
			var input map[string]any
			if err := json.Unmarshal(e.ToolInput, &input); err != nil {
				t.Fatalf("shortened input is not JSON: %v", err)
			}
			if input["command"] != "grep -r x" || !e.ToolInputTruncated {
				t.Fatalf("shortened input lost its short fields or its mark: %v %v", input["command"], e.ToolInputTruncated)
			}
		}
	}

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/conv/entries/"+itoa(bigID), nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("entry route: %d %s", rec.Code, rec.Body.String())
	}
	var full Entry
	if err := json.Unmarshal(rec.Body.Bytes(), &full); err != nil {
		t.Fatal(err)
	}
	var output string
	if err := json.Unmarshal(full.ToolResult, &output); err != nil || len(output) != 9000 || full.ToolResultTruncated {
		t.Fatalf("full entry: %d bytes truncated=%v (%v)", len(output), full.ToolResultTruncated, err)
	}

	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/v1/sessions/conv/messages?limit=30&payload=everything", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown payload mode: %d, want 400", rec.Code)
	}
}

// A turn read while it is still running is rebuilt when a later event lands in it.
func TestStoredTail_RebuildsATurnThatGrew(t *testing.T) {
	srv, s := newTestServer(t)
	ingestJSON(t, s, "live", "user_message", map[string]any{"turn_id": "t", "result": map[string]any{"text": "go"}})
	if _, err := srv.storedTail("live", 30, 0, payloadFull); err != nil {
		t.Fatal(err)
	}
	ingestJSON(t, s, "live", "result", map[string]any{"turn_id": "t", "result": map[string]any{"text": "done"}})
	page, err := srv.storedTail("live", 30, 0, payloadFull)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Turns) != 1 || len(page.Turns[0].EntryIDs) != 2 {
		t.Fatalf("grown turn not rebuilt: %+v", page.Turns)
	}
}

// A turn that ends is built in the background, before anyone reads it.
func TestTurnMaterializer_BuildsTurnsWhenTheyEnd(t *testing.T) {
	srv, s := newTestServer(t)
	ingestJSON(t, s, "bg", "user_message", map[string]any{"turn_id": "t", "result": map[string]any{"text": "go"}})
	ingestJSON(t, s, "bg", "result", map[string]any{"turn_id": "t", "result": map[string]any{"text": "done"}})
	srv.materializer.noteEvent("bg", "result")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		stale, err := s.StoredTurnsNeedingMaterializing("bg", materializerVersion, 5)
		if err != nil {
			t.Fatal(err)
		}
		if len(stale) == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("ended turn was not built in the background")
}

// materializerRulesFingerprint is the sha256 of the files that decide what a stored
// turn contains, as of materializerVersion == fingerprintedMaterializerVersion.
//
// When this test fails you changed those rules. If the change alters what a built
// turn holds, bump materializerVersion in stored_turns.go so every stored turn is
// rebuilt on its next read. Either way, then update both constants below.
const (
	fingerprintedMaterializerVersion = 1
	materializerRulesFingerprint     = "94b3cabcb52340b8f5b6a50ad329caaddbbdbb6b4648d0648453415c6c78394d"
)

func TestMaterializerVersionTracksTheRules(t *testing.T) {
	h := sha256.New()
	for _, name := range []string{"turnmodel.go", "project.go", "stored_turns.go"} {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		h.Write([]byte(name))
		h.Write(b)
	}
	got := hex.EncodeToString(h.Sum(nil))
	if materializerVersion != fingerprintedMaterializerVersion || got != materializerRulesFingerprint {
		t.Fatalf("the turn-building rules changed (fingerprint %s, version %d).\n"+
			"If what a built turn holds changed, bump materializerVersion in stored_turns.go.\n"+
			"Then set fingerprintedMaterializerVersion = %d and materializerRulesFingerprint = %q in this file.",
			got, materializerVersion, materializerVersion, got)
	}
}

func itoa(n int64) string {
	b, _ := json.Marshal(n)
	return string(b)
}
