#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  maelkloud — Deploy Script
#  Called automatically by GitHub Actions on every push to main.
#  Can also be run manually: bash deploy.sh
# ═══════════════════════════════════════════════════════════

set -e

DEPLOY_DIR="$(cd "$(dirname "$0")" && pwd)"
SERVICE_NAME="com.maelkloud.portfolio"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}▸${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }
error()   { echo -e "${RED}✗${NC} $1"; exit 1; }

echo ""
echo "  ┌────────────────────────────────────────┐"
echo "  │   🚀  maelkloud — Auto Deploy           │"
echo "  └────────────────────────────────────────┘"
echo ""

# ── 1. Show what we're deploying ─────────────────────────
COMMIT=$(git -C "$DEPLOY_DIR" rev-parse --short HEAD 2>/dev/null || echo "unknown")
BRANCH=$(git -C "$DEPLOY_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo "unknown")
info "Branch: ${BRANCH}  |  Commit: ${COMMIT}"
info "Deploy dir: ${DEPLOY_DIR}"

# ── 2. Install / update Python dependencies ──────────────
info "Checking Python dependencies…"
if python3 -c "import yfinance" 2>/dev/null; then
  success "yfinance already installed"
else
  warn "yfinance missing — installing…"
  python3 -m pip install yfinance --break-system-packages -q 2>/dev/null || \
  python3 -m pip install yfinance -q
  success "yfinance installed"
fi

# ── 3. Restart the server ─────────────────────────────────
info "Restarting server…"

# Try launchd first (preferred — managed, auto-restarts on crash)
if launchctl list 2>/dev/null | grep -q "$SERVICE_NAME"; then
  launchctl kickstart -k "gui/$(id -u)/${SERVICE_NAME}" 2>/dev/null && \
    success "Server restarted via launchd" || \
    warn "launchctl kickstart failed — falling back to manual restart"
else
  # Fallback: kill any running instance and start fresh
  warn "launchd service not found — using direct restart (run setup_mac.sh for managed service)"
  pkill -f "python3.*server\.py" 2>/dev/null || true
  sleep 1
  nohup python3 "$DEPLOY_DIR/server.py" --prod \
    >> "$DEPLOY_DIR/server.log" \
    2>> "$DEPLOY_DIR/server-error.log" &
  success "Server started (PID $!)"
fi

# ── 4. Health check ───────────────────────────────────────
info "Waiting for server to be ready…"
MAX_RETRIES=10
for i in $(seq 1 $MAX_RETRIES); do
  sleep 1
  STATUS=$(curl -sf http://localhost:8080/api/health 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('status','?'))" 2>/dev/null || echo "")
  if [ "$STATUS" = "ok" ]; then
    VERSION=$(curl -sf http://localhost:8080/api/health \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('version','?'))" 2>/dev/null || echo "?")
    success "Server is healthy  (v${VERSION})"
    break
  fi
  if [ "$i" = "$MAX_RETRIES" ]; then
    error "Health check failed after ${MAX_RETRIES}s — check server-error.log"
  fi
  echo "  …waiting ($i/${MAX_RETRIES})"
done

echo ""
echo "  ✅  Deploy complete — commit ${COMMIT} is live at https://stocks.maelkloud.com"
echo ""
