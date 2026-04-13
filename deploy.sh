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

echo "==> Building $BINARY..."
go build -o "$BINARY" ./cmd/log-store
echo "    built: $(ls -lh "$BINARY" | awk '{print $5}')"

echo "==> Stopping $SERVICE..."
systemctl --user stop "$SERVICE" 2>/dev/null || true
sleep 1

echo "==> Installing binary to $BIN_DIR..."
cp "$BINARY" "$BIN_DIR/$BINARY"

echo "==> Installing service file..."
cp "$REPO_DIR/$SERVICE" "$HOME/.config/systemd/user/$SERVICE"
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
