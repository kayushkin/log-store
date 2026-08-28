package client

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/kayushkin/llm-bridge/msg"
)

// A log-store session id is one path segment. Nothing in this client makes it one.
//
// GetTurnState and ListEvents paste the id straight into a URL with fmt.Sprintf.
// The id is not this client's own value — every caller receives it from the
// bridge or reads it back out of a stored record, and nothing checks it. An id
// carrying "/", "?" or "#" does not produce an error; it produces a request to a
// different endpoint that answers perfectly well.
//
// ListEvents is the sharper of the two: its URL already carries a query, so an
// id holding "?" does not merely add one — it makes `after` part of the id's own
// query string and the server reads the page from the beginning.
//
// The assertions check the property rather than comparing against url.PathEscape's
// output: a test computing its expectation with the same call as the code shares
// the code's opinion of what escaping means and cannot fail when that opinion is
// wrong. What is asserted is that the path is exactly the expected segments, that
// the id segment decodes back to what the caller passed, and that the query is
// exactly the query this client meant to send.

var probeIDs = []struct {
	name string
	id   string
}{
	{"plain", "sess-a"},
	{"extra segment", "sess-a/turn-state"},
	{"traversal", "../sessions/sess-b"},
	{"query", "sess-a?after=999999"},
	{"fragment", "sess-a#frag"},
	{"space", "sess a"},
	{"already encoded", "sess-a%2Fevents"},
	{"ampersand", "sess-a&types=x"},
}

type recorder struct {
	*httptest.Server
	uris []string
}

func newRecorder(t *testing.T, body string) *recorder {
	t.Helper()
	rec := &recorder{}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// RequestURI is the raw bytes off the wire. r.URL.Path has already been
		// unescaped, so it cannot tell "%2F" from "/" — the difference under test.
		rec.uris = append(rec.uris, r.RequestURI)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(body))
	}))
	t.Cleanup(rec.Server.Close)
	return rec
}

// assertSessionPath states the contract once. wantQuery is "" for a URL that is
// meant to carry no query at all.
func assertSessionPath(t *testing.T, requestURI, wantID, wantTail, wantQuery string) {
	t.Helper()

	if i := strings.IndexByte(requestURI, '#'); i >= 0 {
		t.Errorf("request URI %q carries a fragment: everything after it never reached the server", requestURI)
		return
	}

	path, query := requestURI, ""
	if i := strings.IndexByte(requestURI, '?'); i >= 0 {
		path, query = requestURI[:i], requestURI[i+1:]
	}
	if query != wantQuery {
		t.Errorf("request URI %q has query %q, want %q: the id leaked into the query string",
			requestURI, query, wantQuery)
	}

	parts := strings.Split(path, "/")
	want := []string{"", "api", "v1", "sessions", "<id>", wantTail}
	if len(parts) != len(want) {
		t.Errorf("request path %q has %d segments, want %d (/api/v1/sessions/<id>/%s): the id was not one segment",
			path, len(parts), len(want), wantTail)
		return
	}
	for i, w := range want {
		if w == "<id>" {
			continue
		}
		if parts[i] != w {
			t.Errorf("request path %q: segment %d is %q, want %q", path, i, parts[i], w)
		}
	}

	got, err := url.PathUnescape(parts[4])
	if err != nil {
		t.Errorf("id segment %q of %q does not decode: %v", parts[4], path, err)
		return
	}
	if got != wantID {
		t.Errorf("id segment %q of %q decodes to %q, want %q", parts[4], path, got, wantID)
	}
}

func TestGetTurnStateSendsTheIDAsOnePathSegment(t *testing.T) {
	for _, p := range probeIDs {
		t.Run(p.name, func(t *testing.T) {
			rec := newRecorder(t, `{"in_flight":false}`)
			if _, err := New(rec.URL).GetTurnState(p.id); err != nil {
				t.Fatalf("GetTurnState: %v", err)
			}
			if len(rec.uris) != 1 {
				t.Fatalf("server saw %d requests, want 1", len(rec.uris))
			}
			assertSessionPath(t, rec.uris[0], p.id, "turn-state", "")
		})
	}
}

func TestListEventsSendsTheIDAsOnePathSegment(t *testing.T) {
	for _, p := range probeIDs {
		t.Run(p.name, func(t *testing.T) {
			rec := newRecorder(t, `[]`)
			if _, err := New(rec.URL).ListEvents(p.id, 7, nil); err != nil {
				t.Fatalf("ListEvents: %v", err)
			}
			if len(rec.uris) != 1 {
				t.Fatalf("server saw %d requests, want 1", len(rec.uris))
			}
			// after=7 is this client's own parameter. An id that reaches the
			// query is loud here precisely because something else is already
			// meant to be there.
			assertSessionPath(t, rec.uris[0], p.id, "events", "after=7")
		})
	}
}

// TestListEventsKeepsItsOwnQueryWhenFiltering pins the type filter alongside the
// id, so a repair that escaped the whole tail of the URL rather than the segment
// would be caught rather than silently changing what is filtered.
func TestListEventsKeepsItsOwnQueryWhenFiltering(t *testing.T) {
	rec := newRecorder(t, `[]`)
	if _, err := New(rec.URL).ListEvents("sess-a", 0, []string{"user_message", "result"}); err != nil {
		t.Fatalf("ListEvents: %v", err)
	}
	assertSessionPath(t, rec.uris[0], "sess-a", "events", "after=0&types=user_message,result")
}

// TestPushEventAddressesTheCollectionNotASession is the negative control.
// PushEvent interpolates nothing — its path is a constant suffix — so it must be
// untouched by any repair made here. A fix that reached the base URL breaks this
// and nothing else.
func TestPushEventAddressesTheCollectionNotASession(t *testing.T) {
	rec := newRecorder(t, `{"id":1}`)
	if _, err := New(rec.URL).PushEvent(msg.Event{Type: msg.EventResult}); err != nil {
		t.Fatalf("PushEvent: %v", err)
	}
	if got := rec.uris[0]; got != "/api/v1/events" {
		t.Errorf("PushEvent addressed %q, want %q", got, "/api/v1/events")
	}
}

// TestSessionsHoldingHarnessSessionIDAlreadyEscapesItsQueryValue records the
// precedent this repair follows. This client already escapes one uncontrolled
// value — in QUERY position, with QueryEscape, which is the right call there.
// The two path sites are the same author's oversight, not a disagreement about
// whether escaping is wanted.
func TestSessionsHoldingHarnessSessionIDAlreadyEscapesItsQueryValue(t *testing.T) {
	rec := newRecorder(t, `{"sessions":[]}`)
	if _, err := New(rec.URL).SessionsHoldingHarnessSessionID("br_1&types=x"); err != nil {
		t.Fatalf("SessionsHoldingHarnessSessionID: %v", err)
	}
	got := rec.uris[0]
	if !strings.HasPrefix(got, "/api/v1/sessions/by-harness-id?harness_session_id=") {
		t.Fatalf("addressed %q, want the by-harness-id endpoint", got)
	}
	q := strings.TrimPrefix(got, "/api/v1/sessions/by-harness-id?harness_session_id=")
	if strings.ContainsAny(q, "&=") {
		t.Errorf("query value %q still carries a separator: the id can add parameters", q)
	}
}
