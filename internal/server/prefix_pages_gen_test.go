//go:build pagegen

package server

// Generates the EXACT materialized page this server would serve after each event
// prefix of a captured session — for chat-core's live-replay duplication test.
// Run: go test -tags pagegen -run TestGeneratePrefixPages ./internal/server \
//        -events <events.json> -out <pages.json>
import (
	"encoding/json"
	"flag"
	"os"
	"testing"

	"github.com/kayushkin/log-store/internal/store"
)

var (
	eventsPath = flag.String("events", "", "captured events JSON (array, each with event_id)")
	outPath    = flag.String("out", "", "output: array of TurnModel, index k = page after k+1 events")
)

func TestGeneratePrefixPages(t *testing.T) {
	if *eventsPath == "" || *outPath == "" {
		t.Skip("pass -events and -out")
	}
	raw, err := os.ReadFile(*eventsPath)
	if err != nil {
		t.Fatal(err)
	}
	var events []map[string]any
	if err := json.Unmarshal(raw, &events); err != nil {
		t.Fatal(err)
	}
	rows := make([]store.EventRow, 0, len(events))
	for _, ev := range events {
		id := int64(ev["event_id"].(float64))
		data, err := json.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, store.EventRow{ID: id, Type: ev["type"].(string), Data: data})
	}
	pages := make([]TurnModel, 0, len(rows))
	for k := 1; k <= len(rows); k++ {
		pages = append(pages, buildTurnModel("br_1787615605129568013", rows[:k], false))
	}
	out, err := json.Marshal(pages)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(*outPath, out, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %d prefix pages", len(pages))
}
