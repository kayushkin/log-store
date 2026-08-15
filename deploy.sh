#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"
BIN_DIR="$HOME/bin"
SERVICE="log-store.service"
BINARY="log-store"

cd "$REPO_DIR"

# Ensure local dependency is available
if [ ! -d "$REPO_DIR/../llm-bridge" ]; then
  echo "ERROR: llm-bridge repo not found at $REPO_DIR/../llm-bridge"
  exit 1
fi
if [ ! -d "$REPO_DIR/../logstack" ]; then
  echo "ERROR: logstack repo not found at $REPO_DIR/../logstack"
  exit 1
fi

# Add go to PATH if managed by mise
export PATH="$HOME/.local/share/mise/shims:$PATH"

# systemctl --user needs these when run from a shell without its own DBus
# session (e.g. automation, Claude shells). No-op when already set.
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"
export DBUS_SESSION_BUS_ADDRESS="${DBUS_SESSION_BUS_ADDRESS:-unix:path=${XDG_RUNTIME_DIR}/bus}"

echo "==> Building $BINARY..."
go build -o "$BINARY" ./cmd/log-store
echo "    built: $(ls -lh "$BINARY" | awk '{print $5}')"

# Checked BEFORE the install, for the same reason the other pre-install checks are:
# an unidentifiable binary compiles perfectly and reads clean in the log, so
# installing first would put it in front of live callers and only then tell us it
# cannot be traced back to a commit.
echo "==> Checking provenance..."
buildinfo="$(go version -m "$BINARY")"
vcs_revision="$(printf '%s\n' "$buildinfo" | awk -F= '$1 ~ /[[:space:]]vcs\.revision$/ {print $2}')"
vcs_modified="$(printf '%s\n' "$buildinfo" | awk -F= '$1 ~ /[[:space:]]vcs\.modified$/ {print $2}')"
if [ -z "$vcs_revision" ]; then
    echo "    REFUSING TO INSTALL: this binary carries no vcs.revision, so nothing can tie" >&2
    echo "    it back to a commit. 'go build' writes no VCS stamp when it cannot find a .git" >&2
    echo "    DIRECTORY, and it does not fail when that happens -- not even with -buildvcs=true." >&2
    echo "    The usual cause is building from a git worktree, whose .git is a pointer file." >&2
    echo "    Build from a real clone or checkout instead." >&2
    exit 1
fi
echo "    vcs.revision=$vcs_revision"
if [ "$vcs_modified" = "true" ]; then
    echo "    WARNING: built from a DIRTY tree (vcs.modified=true). $vcs_revision names the" >&2
    echo "    commit this binary was built NEAR, not the source it was built FROM, and that" >&2
    echo "    source is not recoverable from any commit. Commit first for a reproducible build." >&2
fi

echo "==> Stopping $SERVICE..."
systemctl --user stop "$SERVICE" 2>/dev/null || true
sleep 1

echo "==> Installing binary to $BIN_DIR..."
cp "$BINARY" "$BIN_DIR/$BINARY"

echo "==> Installing service file..."
sed "s|__HOME__|$HOME|g" "$REPO_DIR/$SERVICE" > "$HOME/.config/systemd/user/$SERVICE"
systemctl --user daemon-reload

echo "==> Starting $SERVICE..."
systemctl --user start "$SERVICE"

echo "==> Verifying..."
sleep 2
if systemctl --user is-active --quiet "$SERVICE"; then
  echo "    $SERVICE is running"
  journalctl --user -u "$SERVICE" -n 5 --no-pager 2>&1 | grep -v '^--'
else
  echo "ERROR: $SERVICE failed to start"
  journalctl --user -u "$SERVICE" -n 15 --no-pager 2>&1
  exit 1
fi

echo "==> Done."
