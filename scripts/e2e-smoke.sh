#!/usr/bin/env bash
# End-to-end boot-and-answer smoke test for log-store.
#
# Builds log-store from THIS checkout, boots it against a throwaway SQLite DB
# on a throwaway port, drives a full session's worth of events through the real
# HTTP API, and asserts the service reads back what was written — materialized
# messages, raw history, turn state, search, and per-session aggregates. Then
# restarts the binary against the same DB and re-asserts, so persistence and the
# migrate()/backfill path are covered too.
#
# Why this exists beyond `go build`
# ---------------------------------
# A tree can compile green and still ship a DEAD binary. Go 1.22+ http.ServeMux
# panics on a conflicting/duplicate route pattern at REGISTRATION time — inside
# server.New(), before ListenAndServe — and no compiler or vet run sees it. That
# has happened in this codebase. This script is the guard: it proves the binary
# BOOTS and ANSWERS, not merely that it links.
#
# Hermetic by construction: temp dir, temp DB, temp port, no credentials, no
# external network, no dependency on any other running service. The live
# log-store on :8175 and its DB at ~/.config/log-store/events.db are never
# touched. LOG_STORE_LOGSTACK_URL is pointed at an unreachable address on
# purpose — forwarding is fire-and-forget (logstack.Forwarder.Forward spawns a
# goroutine and only log.Printf's failures), so a dead logstack must be
# logged-and-ignored, never a failed ingest. Asserting the result event still
# returns 201 is what pins that down.
#
# Exits 0 on success, non-zero on the FIRST failing assertion, dumping the
# server log to stderr.
#
# Tunables:
#   E2E_PORT        — listen port                     (default 19103)
#   E2E_KEEP        — set to "1" to keep $TMP_DIR after the run
#   E2E_SIBLING_SRC — where to find ../llm-bridge and ../logstack when this
#                     checkout has no siblings        (default $HOME/repos)

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PORT="${E2E_PORT:-19103}"
BASE="http://127.0.0.1:$PORT"

# Unreachable on purpose: port 1 refuses instantly. Proves fire-and-forget
# forwarding degrades to a log line rather than a 5xx.
LOGSTACK_URL="http://127.0.0.1:1"

# Default flags, deliberately. An ambient GOFLAGS=-mod=mod would let the build
# silently rewrite go.mod instead of failing; a stray GOWORK would resolve deps
# from somewhere other than this checkout. Both re-create the blind spot this
# guard exists to close.
export GOFLAGS=
export GOWORK=off

for bin in go curl jq; do
  if ! command -v "$bin" >/dev/null 2>&1; then
    echo "ERROR: required tool '$bin' not found on PATH" >&2
    exit 2
  fi
done

TMP_DIR="$(mktemp -d -t log-store-e2e.XXXXXX)"
BIN_DIR="$TMP_DIR/bin"
DATA_DIR="$TMP_DIR/data"
DB_PATH="$DATA_DIR/events.db"
SERVER_LOG="$TMP_DIR/server.log"
mkdir -p "$BIN_DIR" "$DATA_DIR"

SERVER_PID=""
cleanup() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [ "${E2E_KEEP:-}" = "1" ]; then
    echo "[e2e] keeping $TMP_DIR"
  else
    rm -rf "$TMP_DIR"
  fi
}
trap cleanup EXIT INT TERM

step() { printf '\n==> %s\n' "$*"; }
dump_log() {
  echo "----- server.log -----" >&2
  [ -f "$SERVER_LOG" ] && cat "$SERVER_LOG" >&2
  echo "----------------------" >&2
}
fail() { echo "FAIL: $*" >&2; dump_log; exit 1; }
# eq <what> <got> <want> — assert on parsed content, not on a bare 200.
eq() {
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1: got '$2', want '$3'" >&2
    dump_log
    exit 1
  fi
  echo "    $1 = $2"
}

step "resolve module deps"
# go.mod pins llm-bridge and logstack with relative `replace ../<sibling>`
# directives. The nightly build guard clones those siblings alongside the repo,
# so the common path is a plain build. A bare `git clone` into /tmp has no
# siblings — provision them from $E2E_SIBLING_SRC into the temp dir and point a
# throwaway go.work at them, so this script also runs from an isolated clone.
BUILD_ENV_GOWORK="off"
MISSING=()
while read -r old new; do
  [ -n "$new" ] || continue
  case "$new" in ../*) ;; *) continue ;; esac
  [ -d "$REPO_DIR/$new" ] || MISSING+=("$old $new")
done < <(cd "$REPO_DIR" && go mod edit -json | jq -r '(.Replace // [])[] | "\(.Old.Path) \(.New.Path)"')

if [ ${#MISSING[@]} -gt 0 ]; then
  SIBLING_SRC="${E2E_SIBLING_SRC:-$HOME/repos}"
  DEPS_DIR="$TMP_DIR/deps"
  mkdir -p "$DEPS_DIR"
  GO_DIRECTIVE="$(cd "$REPO_DIR" && go mod edit -json | jq -r '.Go')"
  {
    echo "go $GO_DIRECTIVE"
    echo "use $REPO_DIR"
  } >"$TMP_DIR/go.work"
  for entry in "${MISSING[@]}"; do
    set -- $entry
    modpath="$1"; rel="$2"
    name="$(basename "$rel")"
    src="$SIBLING_SRC/$name"
    [ -d "$src" ] || fail "sibling '$name' is missing at $REPO_DIR/$rel and at $src — set E2E_SIBLING_SRC to a directory containing it"
    if git -C "$src" rev-parse --git-dir >/dev/null 2>&1; then
      git clone --quiet --local "$src" "$DEPS_DIR/$name" || fail "could not clone sibling $src"
    else
      cp -a "$src" "$DEPS_DIR/$name" || fail "could not copy sibling $src"
    fi
    echo "replace $modpath => $DEPS_DIR/$name" >>"$TMP_DIR/go.work"
    echo "    provisioned $modpath from $src"
  done
  BUILD_ENV_GOWORK="$TMP_DIR/go.work"
else
  echo "    all relative replace targets present alongside $REPO_DIR"
fi

step "build log-store from $REPO_DIR"
(cd "$REPO_DIR" && GOWORK="$BUILD_ENV_GOWORK" go build -o "$BIN_DIR/log-store" ./cmd/log-store) \
  || fail "go build failed"
echo "    binary: $(ls -lh "$BIN_DIR/log-store" | awk '{print $5}')"

# start_server — boot the binary against the temp DB/port and poll until it
# answers. Never `sleep N && hope`: a route-conflict panic kills the process at
# registration time, so give up the instant the pid is gone.
start_server() {
  LOG_STORE_LISTEN_ADDR=":$PORT" \
  LOG_STORE_DB_PATH="$DB_PATH" \
  LOG_STORE_LOGSTACK_URL="$LOGSTACK_URL" \
    "$BIN_DIR/log-store" >>"$SERVER_LOG" 2>&1 &
  SERVER_PID=$!
  echo "    pid: $SERVER_PID"
  for _ in $(seq 1 50); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
      fail "log-store exited during startup (route-pattern panic? port $PORT in use?)"
    fi
    if curl -fsS --max-time 2 "$BASE/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.2
  done
  fail "log-store did not answer $BASE/health within 10s"
}

stop_server() {
  if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  SERVER_PID=""
}

# --- HTTP helpers -----------------------------------------------------------
# -f so an HTTP error is a failure, --max-time so a hang is a failure too.
get()  { curl -fsS --max-time 10 "$BASE$1"; }
post_event() {  # post_event <json> → response body; non-2xx aborts
  curl -fsS --max-time 10 -X POST "$BASE/api/v1/events" \
    -H 'Content-Type: application/json' -d "$1"
}
status_of() {   # status_of <json> → HTTP status code (no -f: we want the code)
  curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -X POST "$BASE/api/v1/events" \
    -H 'Content-Type: application/json' -d "$1"
}

step "boot log-store on :$PORT (db: $DB_PATH, logstack: $LOGSTACK_URL unreachable)"
start_server
eq "GET /health .status" "$(get /health | jq -r '.status')" "ok"

# A session id + a nonce unique to this run, so the search assertion can't be
# satisfied by anything but the event we just wrote.
SID="e2e-smoke-$$-$(date +%s)"
NONCE="nonce-$$-$RANDOM"
NOW="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
USER_TEXT="e2e-smoke: what is 2+2? ($NONCE)"
ASSISTANT_TEXT="The answer is 4."
MSG_USER="msg_e2e_user"
MSG_ASSISTANT="msg_e2e_assistant"
MODEL="e2e-smoke-model"
echo "    session: $SID"

step "POST /api/v1/events — user_message"
RESP=$(post_event "$(jq -nc \
  --arg sid "$SID" --arg mid "$MSG_USER" --arg ts "$NOW" --arg text "$USER_TEXT" '{
    type:"user_message", harness:"mock", bridge_session_id:$sid,
    message_id:$mid, timestamp:$ts, result:{text:$text}
  }')")
USER_EVENT_ID=$(jq -r '.id' <<<"$RESP")
[ "$USER_EVENT_ID" -gt 0 ] 2>/dev/null || fail "ingest did not return a row id: $RESP"
echo "    event_id: $USER_EVENT_ID"

step "GET /api/v1/sessions/{id}/turn-state — mid-turn"
TS=$(get "/api/v1/sessions/$SID/turn-state")
eq "in_flight (user_message, no terminator)" "$(jq -r '.in_flight' <<<"$TS")" "true"
eq "last_user_message_event_id"              "$(jq -r '.last_user_message_event_id' <<<"$TS")" "$USER_EVENT_ID"
eq "last_terminator_event_id"                "$(jq -r '.last_terminator_event_id' <<<"$TS")" "0"

step "POST /api/v1/events — tool_call + tool_result + result"
post_event "$(jq -nc --arg sid "$SID" --arg mid "$MSG_ASSISTANT" --arg ts "$NOW" '{
    type:"tool_call", harness:"mock", bridge_session_id:$sid,
    message_id:$mid, timestamp:$ts,
    tool_call:{tool_id:"toolu_e2e_1", name:"Bash", input:{command:"echo 4"}, message_id:$mid}
  }')" >/dev/null
post_event "$(jq -nc --arg sid "$SID" --arg mid "$MSG_ASSISTANT" --arg ts "$NOW" '{
    type:"tool_result", harness:"mock", bridge_session_id:$sid,
    message_id:$mid, timestamp:$ts,
    tool_result:{tool_id:"toolu_e2e_1", name:"Bash", output:"4", message_id:$mid}
  }')" >/dev/null

# The result event is the one that fans out to logstack. Our logstack URL
# refuses connections, so a 201 here is the assertion that forwarding is
# fire-and-forget rather than load-bearing.
RESP=$(post_event "$(jq -nc \
  --arg sid "$SID" --arg mid "$MSG_ASSISTANT" --arg ts "$NOW" \
  --arg text "$ASSISTANT_TEXT" --arg model "$MODEL" '{
    type:"result", harness:"mock", bridge_session_id:$sid,
    message_id:$mid, timestamp:$ts,
    result:{
      text:$text, model:$model, duration_ms:777, num_turns:1,
      usage:{input_tokens:1234, output_tokens:56, total_tokens:1290},
      cost:{total_usd:0.25}
    }
  }')")
RESULT_EVENT_ID=$(jq -r '.id' <<<"$RESP")
[ "$RESULT_EVENT_ID" -gt 0 ] 2>/dev/null || fail "result ingest did not return a row id: $RESP"
echo "    result event_id: $RESULT_EVENT_ID (accepted despite unreachable logstack)"

step "GET /api/v1/sessions/{id}/history — raw events, in order, event_id-stamped"
HIST=$(get "/api/v1/sessions/$SID/history")
eq "history length"      "$(jq -r 'length' <<<"$HIST")" "4"
eq "history types"       "$(jq -r '[.[].type] | join(",")' <<<"$HIST")" "user_message,tool_call,tool_result,result"
eq "event_id injected"   "$(jq -r '[.[] | select(.event_id != null)] | length' <<<"$HIST")" "4"
eq "first event_id"      "$(jq -r '.[0].event_id' <<<"$HIST")" "$USER_EVENT_ID"
eq "user text round-trip" "$(jq -r '.[0].result.text' <<<"$HIST")" "$USER_TEXT"

step "GET /api/v1/sessions/{id}/history?types=result — server-side filter"
FILTERED=$(get "/api/v1/sessions/$SID/history?types=result")
eq "filtered length" "$(jq -r 'length' <<<"$FILTERED")" "1"
eq "filtered type"   "$(jq -r '.[0].type' <<<"$FILTERED")" "result"
eq "filtered model"  "$(jq -r '.[0].result.model' <<<"$FILTERED")" "$MODEL"

step "GET /api/v1/sessions/{id}/events?after=N — incremental tail"
TAIL=$(get "/api/v1/sessions/$SID/events?after=$USER_EVENT_ID")
eq "tail length" "$(jq -r 'length' <<<"$TAIL")" "3"
eq "tail types"  "$(jq -r '[.[].type] | join(",")' <<<"$TAIL")" "tool_call,tool_result,result"
eq "tail after latest is empty" "$(jq -r 'length' <<<"$(get "/api/v1/sessions/$SID/events?after=$RESULT_EVENT_ID")")" "0"

step "GET /api/v1/sessions/{id}/messages — materialized chat"
MSGS=$(get "/api/v1/sessions/$SID/messages")
eq "message count"      "$(jq -r 'length' <<<"$MSGS")" "2"
eq "msg[0].role"        "$(jq -r '.[0].role' <<<"$MSGS")" "user"
eq "msg[0].content"     "$(jq -r '.[0].content' <<<"$MSGS")" "$USER_TEXT"
eq "msg[0].id"          "$(jq -r '.[0].id' <<<"$MSGS")" "$MSG_USER"
eq "msg[1].role"        "$(jq -r '.[1].role' <<<"$MSGS")" "assistant"
eq "msg[1].content"     "$(jq -r '.[1].content' <<<"$MSGS")" "$ASSISTANT_TEXT"
eq "msg[1].done"        "$(jq -r '.[1].done' <<<"$MSGS")" "true"
eq "msg[1].tools[0].tool"   "$(jq -r '.[1].tools[0].tool' <<<"$MSGS")" "Bash"
eq "msg[1].tools[0].output" "$(jq -r '.[1].tools[0].output' <<<"$MSGS")" "4"
eq "msg[1].meta.model"      "$(jq -r '.[1].meta.model' <<<"$MSGS")" "$MODEL"
eq "msg[1].meta.usage.input_tokens" "$(jq -r '.[1].meta.usage.input_tokens' <<<"$MSGS")" "1234"

step "GET /api/v1/sessions/{id}/turn-state — turn closed"
TS=$(get "/api/v1/sessions/$SID/turn-state")
eq "in_flight (after result)" "$(jq -r '.in_flight' <<<"$TS")" "false"
eq "last_terminator_event_id" "$(jq -r '.last_terminator_event_id' <<<"$TS")" "$RESULT_EVENT_ID"

step "GET /api/v1/sessions/search?q=<nonce>"
HITS=$(get "/api/v1/sessions/search?q=$NONCE")
eq "search hits"        "$(jq -r 'length' <<<"$HITS")" "1"
eq "hit session_id"     "$(jq -r '.[0].session_id' <<<"$HITS")" "$SID"
eq "hit match_count"    "$(jq -r '.[0].match_count' <<<"$HITS")" "1"
eq "search miss is empty" "$(jq -r 'length' <<<"$(get "/api/v1/sessions/search?q=$NONCE-absent")")" "0"

step "GET /api/v1/sessions/aggregates — projection rollup"
AGG=$(get /api/v1/sessions/aggregates)
ROW=$(jq -c --arg sid "$SID" '.[] | select(.session_id==$sid)' <<<"$AGG")
[ -n "$ROW" ] || fail "aggregates did not include session $SID: $AGG"
eq "aggregate turns"         "$(jq -r '.turns' <<<"$ROW")" "1"
eq "aggregate input_tokens"  "$(jq -r '.input_tokens' <<<"$ROW")" "1234"
eq "aggregate output_tokens" "$(jq -r '.output_tokens' <<<"$ROW")" "56"
eq "aggregate cost_usd"      "$(jq -r '.cost_usd' <<<"$ROW")" "0.25"
eq "aggregate duration_ms"   "$(jq -r '.duration_ms' <<<"$ROW")" "777"
eq "aggregate model"         "$(jq -r '.model' <<<"$ROW")" "$MODEL"

step "POST /api/v1/events — malformed input is rejected, not stored"
eq "missing bridge_session_id → 400" "$(status_of '{"type":"result","result":{"text":"orphan"}}')" "400"
eq "invalid JSON → 400"              "$(status_of 'not json at all')" "400"
eq "history unchanged after rejects" "$(jq -r 'length' <<<"$(get "/api/v1/sessions/$SID/history")")" "4"

step "restart against the same DB — persistence + migrate() backfill"
# Second boot re-runs migrate(), including the sessions-projection backfill.
# The backfill is guarded by NOT IN (SELECT session_id FROM sessions); if that
# guard ever regresses, turn_count double-counts and this assertion catches it.
stop_server
start_server
eq "GET /health .status (2nd boot)" "$(get /health | jq -r '.status')" "ok"
eq "messages survive restart" "$(jq -r 'length' <<<"$(get "/api/v1/sessions/$SID/messages")")" "2"
eq "assistant text survives"  "$(jq -r '.[1].content' <<<"$(get "/api/v1/sessions/$SID/messages")")" "$ASSISTANT_TEXT"
ROW=$(jq -c --arg sid "$SID" '.[] | select(.session_id==$sid)' <<<"$(get /api/v1/sessions/aggregates)")
eq "turns not double-counted by backfill" "$(jq -r '.turns' <<<"$ROW")" "1"
eq "input_tokens not double-counted"      "$(jq -r '.input_tokens' <<<"$ROW")" "1234"

step "SUCCESS — log-store boots, ingests, materializes, and persists"
echo "    routes exercised: POST /api/v1/events; GET /health,"
echo "      /api/v1/sessions/{id}/{messages,history,events,turn-state},"
echo "      /api/v1/sessions/search, /api/v1/sessions/aggregates"
