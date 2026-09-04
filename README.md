# log-store

Durable event log for the [llm-bridge](https://github.com/kayushkin/llm-bridge) ecosystem.

Persists all `msg.Event` from agent sessions into SQLite and makes them queryable via HTTP. Reconstructs materialized conversation history on read. Forwards result statistics to [logstack](https://github.com/kayushkin/logstack) for analytics.

```
  ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
    llm-bridge-server  (or any HTTP client)
  └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┬ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
                            │ HTTP
  ╔═════════════════════════╪═════════════════════════════╗
  ║                    log-store                          ║
  ║                         │                             ║
  ║   POST /events ──── ingest + store                    ║
  ║   GET  /messages ── materialized conversation         ║
  ║   GET  /history ─── raw event timeline                ║
  ║   GET  /events ──── paginated poll (after=N)          ║
  ║                         │                             ║
  ║   ┌─────────────────────▼───────────────────────┐     ║
  ║   │              SQLite (WAL)                   │     ║
  ║   │   events table indexed by session_id        │     ║
  ║   └─────────────────────┬───────────────────────┘     ║
  ║                         │ result events               ║
  ║                         ▼                             ║
  ║                    logstack                            ║
  ║              (async stats forwarding)                  ║
  ╚═══════════════════════════════════════════════════════╝
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

### Query messages

```bash
# Materialized conversation (grouped messages with metadata)
curl http://localhost:8175/api/v1/sessions/abc123/messages

# Raw event timeline
curl http://localhost:8175/api/v1/sessions/abc123/history

# Paginated poll (events after row ID 42)
curl http://localhost:8175/api/v1/sessions/abc123/events?after=42
```

## API

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/api/v1/events` | Ingest a `msg.Event`. Returns `{"id": <rowID>}` (201 Created) |
| `GET` | `/api/v1/sessions/{id}/messages` | Materialized conversation — streaming text accumulated, tool calls matched with results, token/cost metadata |
| `GET` | `/api/v1/sessions/{id}/history` | Raw stored events in chronological order |
| `GET` | `/api/v1/sessions/{id}/events?after=N` | Events with row ID > N — for polling/reconnection without re-fetching |
| `GET` | `/health` | `{"status": "ok"}` |

### Event ingestion

`POST /api/v1/events` accepts any `msg.Event` with a non-empty `session_id`. The raw JSON body is stored verbatim — no re-serialization. If the event type is `result`, usage statistics (tokens, cost, duration, tool invocations) are forwarded to logstack asynchronously.

### Message materialization

`GET /api/v1/sessions/{id}/messages` reconstructs the conversation from raw events:

- Streaming text deltas are accumulated into complete messages
- Tool calls are matched with their results by `ToolID` (fallback to tool name)
- Each message includes its contributing events, token counts, and cost

This is the endpoint llm-bridge-server proxies for session message history.

## Configuration

All configuration is via environment variables.

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_STORE_LISTEN_ADDR` | `:8175` | HTTP listen address |
| `LOG_STORE_DB_PATH` | `~/.config/log-store/events.db` | SQLite database path |
| `LOG_STORE_LOGSTACK_URL` | `http://localhost:8081` | Logstack URL for forwarding result statistics |

> ⚠️ **The default matches logstack's *code* default, not necessarily your deployment.**
> logstack reads `LOGSTACK_PORT` and defaults it to `8081`; if your logstack unit
> overrides that port, `LOG_STORE_LOGSTACK_URL` must be overridden to match. On this
> host logstack runs on `8088` and `8081` belongs to bookstack, so the default pointed
> log-store at the wrong service entirely — 8,088 result events were POSTed to the book
> library and 404'd away between 2026-09-02 and 2026-09-04. `log-store.service` now
> sets `8088` explicitly. Startup runs a preflight probe that says so loudly if the
> configured URL is not a logstack.

## Storage

Events are stored in a single SQLite table with WAL mode and a 5-second busy timeout:

```sql
CREATE TABLE events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL,
    type       TEXT NOT NULL,
    data       TEXT NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```

Indexed by `session_id` and `(session_id, type)`.

## Client library

The `client` package provides an HTTP client for pushing events:

```go
import "github.com/kayushkin/log-store/client"

c := client.New("http://localhost:8175")
id, err := c.PushEvent(event)
```

## Part of the llm-bridge ecosystem

log-store is an optional store in the [llm-bridge](https://github.com/kayushkin/llm-bridge) ecosystem. llm-bridge-server proxies message and history endpoints to log-store when `LLMBRIDGE_LOG_STORE_URL` is configured. See the [llm-bridge README](https://github.com/kayushkin/llm-bridge) for the full picture.
