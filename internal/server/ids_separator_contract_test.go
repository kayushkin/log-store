package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// The `ids` query parameter carries a SET of session ids in one string, and
// two separate rules have to agree for that to work:
//
//   - the writer escapes the joined string, so a character inside an id can
//     never act as a query separator (llm-bridge-server does this with
//     url.Values{"ids": {…}}.Encode() at both of its call sites); and
//   - the reader decodes the parameter and THEN splits it on "," (splitIDs,
//     below the handlers in this package).
//
// The writer's half is pinned in llm-bridge-server. The reader's half was
// pinned nowhere: `splitIDs` is named by no test in this repo, so nothing here
// held the decode-then-split order that the writer's escaping depends on.
// These pins are the reader's half.
//
// ⚠️ They are pinned OVER HTTP, through the real route, on purpose. Calling
// splitIDs directly cannot see the URL decoding — and the decoding is half the
// contract. A direct unit test of splitIDs is structurally incapable of
// disagreeing with the escaping, so it would look like a second opinion and be
// none.

// idsQuery builds the query string exactly as llm-bridge-server builds it:
// join the id set on "," and escape the whole joined value once. This is the
// wire contract under test, not a convenience.
func idsQuery(ids ...string) string {
	return "?" + url.Values{"ids": {strings.Join(ids, ",")}}.Encode()
}

// resolvedIDs drives GET /api/v1/sessions/validators and returns the ids the
// handler actually resolved. store.Validators emits one entry per non-empty id
// whether or not that session exists, so the response keys ARE the split.
func resolvedIDs(t *testing.T, srv *Server, query string) []string {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sessions/validators"+query, nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("validators returned %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]Validator
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	got := make([]string, 0, len(out))
	for id := range out {
		got = append(got, id)
	}
	return got
}

// assertResolved compares as a SET. A case wanting zero ids must say so with
// an explicit empty want; every other case is guarded against silently
// resolving to nothing, which would pass a subset check.
func assertResolved(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(want) > 0 && len(got) == 0 {
		t.Fatalf("resolved NO ids; a vacuous pass — want %q", want)
	}
	gotSet := map[string]bool{}
	for _, g := range got {
		gotSet[g] = true
	}
	if len(got) != len(want) {
		t.Errorf("resolved %d ids, want %d:\n  got:  %q\n  want: %q", len(got), len(want), got, want)
		return
	}
	for _, w := range want {
		if !gotSet[w] {
			t.Errorf("id %q did not survive the round trip; got %q", w, got)
		}
	}
}

// The ordinary case, and the one every other case is measured against.
func TestAnEscapedCommaStillSeparatesTheIDSet(t *testing.T) {
	srv, _ := newTestServer(t)
	// url.Values escapes the join comma to %2C. The reader has to decode it
	// back BEFORE splitting or the whole set arrives as one id.
	q := idsQuery("br_alpha", "br_beta", "br_gamma")
	if !strings.Contains(q, "%2C") {
		t.Fatalf("fixture is not exercising the escape: %q", q)
	}
	assertResolved(t, resolvedIDs(t, srv, q), "br_alpha", "br_beta", "br_gamma")
}

// What the writer's escaping buys: a `&` inside an id cannot add a parameter
// to this request, and the id keeps its own text.
func TestAnAmpersandInsideAnIDDoesNotBecomeASecondParameter(t *testing.T) {
	srv, _ := newTestServer(t)
	assertResolved(t,
		resolvedIDs(t, srv, idsQuery("br_alpha&turns=1", "br_beta")),
		"br_alpha&turns=1", "br_beta")
}

func TestAnEqualsInsideAnIDSurvivesIntact(t *testing.T) {
	srv, _ := newTestServer(t)
	assertResolved(t,
		resolvedIDs(t, srv, idsQuery("br_alpha=1", "br_beta")),
		"br_alpha=1", "br_beta")
}

// The silent one. url.Values encodes a literal `+` as %2B and a SPACE as `+`.
// A reader that decoded wrongly would hand back the other character, and the
// only symptom upstream is "no such session" — which names nothing.
func TestAPlusAndASpaceInsideAnIDDoNotTradePlaces(t *testing.T) {
	srv, _ := newTestServer(t)
	assertResolved(t, resolvedIDs(t, srv, idsQuery("br_a+b")), "br_a+b")
	// A space survives as a space. splitIDs trims only the OUTER whitespace of
	// each comma-separated part, so an interior space is part of the id.
	assertResolved(t, resolvedIDs(t, srv, idsQuery("br_a b")), "br_a b")
}

// ⚠️ The limit of the escaping repair, pinned as the constraint it is rather
// than left for a reader to discover.
//
// The comma is the separator itself, so escaping cannot protect it: the writer
// escapes it, the reader decodes it back to a literal comma, and then splits
// on it. An id CONTAINING a comma is therefore indistinguishable on this wire
// from two ids, and no amount of escaping at either end changes that.
//
// This is latent, not live. llm-bridge-server's handleCreateSession accepts a
// caller-minted session_id with no character constraint (only a collision
// check), so nothing upstream forbids the comma — but measured 2026-08-29 over
// log-store's own events.db, 0 of 26,969 distinct stored session ids contain
// one. Whether to constrain the id or change the encoding is a live decision;
// this pin only stops the behaviour being rediscovered as a surprise.
func TestAnIDContainingACommaIsIndistinguishableFromTwoIDs(t *testing.T) {
	srv, _ := newTestServer(t)
	assertResolved(t, resolvedIDs(t, srv, idsQuery("br_alpha,beta")), "br_alpha", "beta")
}

// A trailing or doubled comma cannot manufacture a lookup.
//
// ⚠️ Note what this does NOT attribute. The property is held TWICE: splitIDs
// drops a blank part, and store.Validators independently skips an empty id.
// Removing either guard alone leaves this test green (control arm A5), so no
// pin at the HTTP level can say which layer earns it — it reddens only when
// both are gone (A5b). The name says "cannot manufacture a lookup" rather than
// "splitIDs drops blanks" for exactly that reason.
func TestATrailingOrDoubledCommaDoesNotManufactureALookup(t *testing.T) {
	srv, _ := newTestServer(t)
	assertResolved(t, resolvedIDs(t, srv, idsQuery("br_alpha", "", "  ", "br_beta")),
		"br_alpha", "br_beta")
	assertResolved(t, resolvedIDs(t, srv, idsQuery("  br_alpha  ")), "br_alpha")
}

// The vacuity guard for this file: an empty id set answers an empty map and
// 200, so "resolved nothing" is a real answer here and every other case's
// non-empty assertion is meaningful.
func TestAnEmptyIDSetResolvesToNoIDsAndNotAnError(t *testing.T) {
	srv, _ := newTestServer(t)
	assertResolved(t, resolvedIDs(t, srv, idsQuery("")))
}
