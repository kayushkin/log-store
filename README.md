# log-store

Durable event log for the [llm-bridge](https://github.com/kayushkin/llm-bridge) ecosystem.

Persists every `msg.Event` from agent sessions into SQLite, the only record of what
happened. Each event is placed in its turn as it is stored, each turn's reading page
is built once and stored, and pages are served from those rows. Forwards result
statistics to [logstack](https://github.com/kayushkin/logstack).

```
  llm-bridge-server (or any HTTP client)
            │ HTTP
  ┌─────────▼──────────────────────────────────────────────────────┐
  │ log-store                                                      │
  │   POST /events ─────────── store event + place it in its turn  │
  │   GET  /messages?limit= ── reading page, from stored turns     │
  │   GET  /messages/raw ───── every event of those turns          │
  │   GET  /entries/{id} ───── one entry with full tool payloads   │
  │   GET  /events?after=N ─── raw events, oldest first            │
  │            │                                                   │
  │   SQLite (WAL): events (the record) + derived, rebuildable:    │
  │   turn_index_sessions, event_turns, turns, turn_entries,       │
  │   dedup_candidates, sessions (per-session totals)              │
  └────────────┬───────────────────────────────────────────────────┘
               │ result events
               ▼
            logstack
```

## Quick start

### Build and run

```bash
go build -o log-store ./cmd/log-store
./log-store
```

The server listens on `:8175` by default.

### Deploy as a systemd service

```bash
./deploy.sh
```

Builds the binary, installs to `~/bin/log-store`, and restarts the `log-store.service` unit.

### Ingest an event

```bash
curl -X POST http://localhost:8175/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"session_id": "abc123", "type": "result", ...}'
```

### Read a session

```bash
# The newest 30 turns as a reading page; tool payloads shortened to 2 KB
curl 'http://localhost:8175/api/v1/sessions/abc123/messages?limit=30&payload=preview'

# One entry with its tool input and output in full
curl http://localhost:8175/api/v1/sessions/abc123/entries/4242

# Raw events, oldest first (after=0 for all of them)
curl 'http://localhost:8175/api/v1/sessions/abc123/events?after=42'
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/events` | Ingest a `msg.Event` (needs `bridge_session_id`). Returns `{"id": <rowID>}` (201) |
| `GET` | `/api/v1/sessions/{id}/messages?limit=&before=&payload=` | The reading page: `{model}` with the newest `limit` prompt turns, or those older than the turn holding event `before`. Duplicates dropped, no `raw`. `payload=preview` shortens tool strings to 2 KB. **`limit` or `before` is required** (400 otherwise) |
| `GET` | `/api/v1/sessions/{id}/messages/raw?limit=&before=` | Same turns, unprojected: every event as an entry with its `raw` source, duplicates annotated. At most 5,000 events: older turns are left out first |
| `GET` | `/api/v1/sessions/{id}/entries/{eventId}` | One entry of the reading page with full tool payloads; 404 if the event is not one |
| `GET` | `/api/v1/sessions/bundle?ids=&turns=&payload=` | Reading pages for several sessions |
| `GET` | `/api/v1/sessions/validators?ids=` | `{maxEventId, eventCount, updatedAt}` per session, from the turn index |
| `GET` | `/api/v1/sessions/{id}/events?after=N&types=` | Raw events with row id > N, `event_id` spliced in |
| `GET` | `/api/v1/sessions/{id}/turn-state` | Whether a turn is in flight |
| `GET` | `/api/v1/sessions/search?q=` | Sessions whose events contain `q` |
| `GET` | `/api/v1/sessions/aggregates` | Per-session token, turn and duration totals. No cost: a session's cost is llm-bridge-server's `spend_usd` / the `session_cost` event |
| `GET` | `/api/v1/sessions/by-harness-id?harness_session_id=` | Sessions holding a harness session id |
| `GET` | `/health` | `{"status": "ok"}` plus forwarder state |
| `GET` | `/settings` | Every environment variable the service reads, with the value in force and its source; read-only |

### Stored turns

Pages are not rebuilt from events on each read. `internal/store/turn_index.go` places
every event in its turn when it is stored; `internal/server/stored_turns.go` builds a
turn once with the builder in `turnmodel.go`, stores the projected result, and rebuilds
it only when the turn gains an event, a dual-emitted copy changes what it pairs with
(`dedup.go`), or `materializerVersion` changes. Everything but `events` is derived and
can be dropped and rebuilt; sessions written before the index are indexed in the
background at startup, newest first.

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_STORE_LISTEN_ADDR` | `:8175` | HTTP listen address |
| `LOG_STORE_DB_PATH` | `~/.config/log-store/events.db` | SQLite database path |
| `LOG_STORE_LOGSTACK_URL` | `http://localhost:8081` | Logstack URL for forwarding result statistics |

All three are declared once in `internal/config` with llm-bridge `servicesettings`, and
`GET /settings` describes them with the value in force. A new variable is declared there or
`TestEveryEnvironmentVariableTheServiceReadsIsDeclared` fails. A set variable that starts with
`LOG_STORE_LISTEN`, `LOG_STORE_DB` or `LOG_STORE_LOGSTACK` and is not one of the three is a
misspelling, and stops the service at startup; `deploy.sh` checks the running service's
environment for one before it stops it. Declare nothing `Editable` and no secret: every route
here is open.

> ⚠️ **The default matches logstack's *code* default, not necessarily your deployment.**
> logstack reads `LOGSTACK_PORT` and defaults it to `8081`; if your logstack unit
> overrides that port, `LOG_STORE_LOGSTACK_URL` must be overridden to match. On this
> host logstack runs on `8088` and `8081` belongs to bookstack, so the default pointed
> log-store at the wrong service entirely — 8,088 result events were POSTed to the book
> library and 404'd away between 2026-09-02 and 2026-09-04. `log-store.service` now
> sets `8088` explicitly. Startup runs a preflight probe that says so loudly if the
> configured URL is not a logstack.

## Storage

`events` is the record: one row per event, stored verbatim, indexed by `session_id`
and `(session_id, type)`. The other tables are derived from it — see
[Stored turns](#stored-turns).

## Client library

The `client` package provides an HTTP client for pushing events:

```go
import "github.com/kayushkin/log-store/client"

c := client.New("http://localhost:8175")
id, err := c.PushEvent(event)
```

## Part of the llm-bridge ecosystem

log-store is an optional store in the [llm-bridge](https://github.com/kayushkin/llm-bridge) ecosystem. llm-bridge-server proxies the messages, raw and entry endpoints to log-store when `LLMBRIDGE_LOG_STORE_URL` is configured. See the [llm-bridge README](https://github.com/kayushkin/llm-bridge) for the full picture.
