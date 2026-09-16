package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/kayushkin/llm-bridge/msg"
	"github.com/kayushkin/log-store/internal/store"
)

// Pairing of dual-emitted copies.
//
// Claude Code reports a prompt and a reply twice: once through the harness stream
// and once through OTel. The two copies share no id. The OTel copy is matched to
// its harness twin by class (prompt or reply) and exact text, count-wise: the i-th
// OTel copy of a text pairs with the i-th harness copy of it, never positionally.
// A surplus copy of either source stays visible — a genuine re-send still shows
// twice, and a turn whose only record is the OTel copy still renders.
//
// The copies do not always share a turn. Measured on this host's events: of the
// OTel copies whose harness twin carries a DIFFERENT turn_id, 2,404 were in July,
// 102 in August and 34 in September 2026. So pairing must see a whole session, not
// one turn — which is why the facts it needs are kept in the turn index
// (dedup_candidates) and the pairing below runs over all of them.

// dedupCandidateVersion names the rule dedupCandidateOf applies. Changing what it
// returns changes the stored candidates, so bump it: every session's turn index is
// then rebuilt (store.TurnIndexVersion folds it in).
const dedupCandidateVersion = 1

// dedupCandidateOf reports whether an event is a copy that can pair with its twin,
// and if so the hash of its pairing key and its source.
func dedupCandidateOf(eventType string, data []byte) (keyHash, source string, ok bool, err error) {
	if eventType != string(msg.EventUserMessage) && eventType != string(msg.EventResult) {
		return "", "", false, nil
	}
	var ev msg.Event
	if err := json.Unmarshal(data, &ev); err != nil {
		// The builder emits an unparseable event as an opaque meta entry, which
		// never pairs.
		return "", "", false, nil
	}
	return dedupCandidateOfEvent(&ev)
}

func dedupCandidateOfEvent(ev *msg.Event) (keyHash, source string, ok bool, err error) {
	if ev.Type != msg.EventUserMessage && ev.Type != msg.EventResult {
		return "", "", false, nil
	}
	text := entryText(ev)
	if text == "" {
		return "", "", false, nil
	}
	class := "assistant"
	if ev.Type == msg.EventUserMessage {
		class = "user"
	}
	sum := sha256.Sum256([]byte(class + "\x00" + text))
	return hex.EncodeToString(sum[:]), eventSource(ev), true, nil
}

// dedupPairs maps each paired event id to its twin's id, in both directions.
type dedupPairs map[int64]int64

// pairDualEmits pairs candidates count-wise per key. Candidates must be in event
// order.
func pairDualEmits(candidates []store.DedupCandidate) dedupPairs {
	type queues struct{ harness, otel []int64 }
	byKey := map[string]*queues{}
	var keys []string
	for _, c := range candidates {
		q, ok := byKey[c.KeyHash]
		if !ok {
			q = &queues{}
			byKey[c.KeyHash] = q
			keys = append(keys, c.KeyHash)
		}
		if c.Source == sourceOTel {
			q.otel = append(q.otel, c.EventID)
		} else {
			q.harness = append(q.harness, c.EventID)
		}
	}
	pairs := dedupPairs{}
	for _, k := range keys {
		q := byKey[k]
		n := len(q.harness)
		if len(q.otel) < n {
			n = len(q.otel)
		}
		for i := 0; i < n; i++ {
			pairs[q.harness[i]] = q.otel[i]
			pairs[q.otel[i]] = q.harness[i]
		}
	}
	return pairs
}

// pagePairs pairs the copies within a page of rows — the scope a builder given
// only those rows can see.
func pagePairs(rows []store.EventRow) dedupPairs {
	var candidates []store.DedupCandidate
	for _, r := range rows {
		keyHash, source, ok, _ := dedupCandidateOf(r.Type, r.Data)
		if ok {
			candidates = append(candidates, store.DedupCandidate{EventID: r.ID, KeyHash: keyHash, Source: source})
		}
	}
	return pairDualEmits(candidates)
}

// pairsFingerprint names the pairing a turn was built with: each of its candidate
// events and the twin it paired with, or none. A turn stored under a different
// fingerprint is stale — a copy that landed later, possibly in another turn, has
// changed what it pairs with.
func pairsFingerprint(candidateEventIDs []int64, pairs dedupPairs) string {
	if len(candidateEventIDs) == 0 {
		return ""
	}
	ids := append([]int64(nil), candidateEventIDs...)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var b strings.Builder
	for _, id := range ids {
		b.WriteString(strconv.FormatInt(id, 10))
		b.WriteByte(':')
		b.WriteString(strconv.FormatInt(pairs[id], 10))
		b.WriteByte(';')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
