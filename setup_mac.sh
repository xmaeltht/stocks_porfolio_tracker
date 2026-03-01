#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  maelkloud — Mac Mini + Cloudflare Tunnel Setup
#  Run this ONCE on your Mac Mini to go live at maelkloud.com
# ═══════════════════════════════════════════════════════════

set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DOMAIN="maelkloud.com"
APP_SUBDOMAIN="stocks.maelkloud.com"
PORT=8080
SERVICE_NAME="com.maelkloud.portfolio"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}▸${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }
error()   { echo -e "${RED}✗${NC} $1"; exit 1; }
step()    { echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "  ${GREEN}$1${NC}"; echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"; }

echo ""
echo "  ██████████████████████████████████████"
echo "  ██                                  ██"
echo "  ██    maelkloud.com  Setup Script   ██"
echo "  ██                                  ██"
echo "  ██████████████████████████████████████"
echo ""

# ── STEP 1: Python ────────────────────────────────────────
step "Step 1 / 5 — Python"
if command -v python3 &>/dev/null; then
  PYTHON=python3
  success "Python found: $(python3 --version)"
else
  error "Python 3 not found. Install from https://www.python.org/downloads/"
fi

info "Installing Python dependencies…"
$PYTHON -m pip install yfinance --break-system-packages -q 2>/dev/null || \
$PYTHON -m pip install yfinance -q
success "yfinance ready"

# ── STEP 2: Homebrew ──────────────────────────────────────
step "Step 2 / 5 — Homebrew"
if ! command -v brew &>/dev/null; then
  warn "Homebrew not found. Installing now (this may take a few minutes)…"
  /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
  # Add brew to PATH for Apple Silicon Macs
  if [ -f "/opt/homebrew/bin/brew" ]; then
    eval "$(/opt/homebrew/bin/brew shellenv)"
  fi
fi
success "Homebrew ready"

# ── STEP 3: cloudflared ───────────────────────────────────
step "Step 3 / 5 — Cloudflare Tunnel (cloudflared)"
if ! command -v cloudflared &>/dev/null; then
  info "Installing cloudflared…"
  brew install cloudflare/cloudflare/cloudflared
fi
success "cloudflared $(cloudflared --version | head -1)"

info "Logging in to Cloudflare (your browser will open)…"
echo ""
warn "ACTION REQUIRED: A browser window will open."
warn "Log in to Cloudflare and select your domain: ${DOMAIN}"
echo ""
read -p "  Press Enter when ready to open the browser…"
cloudflared tunnel login

# Create the tunnel
info "Creating tunnel named 'maelkloud'…"
cloudflared tunnel create maelkloud 2>/dev/null || warn "Tunnel 'maelkloud' may already exist — continuing."

# Get tunnel ID
TUNNEL_ID=$(cloudflared tunnel list --output json 2>/dev/null | python3 -c "
import json, sys
tunnels = json.load(sys.stdin)
for t in tunnels:
    if t.get('name') == 'maelkloud':
        print(t['id']); break
" 2>/dev/null)

if [ -z "$TUNNEL_ID" ]; then
  warn "Could not auto-detect tunnel ID."
  echo ""
  read -p "  Paste your tunnel ID here: " TUNNEL_ID
fi
success "Tunnel ID: ${TUNNEL_ID}"

# Write cloudflared config
CLOUDFLARED_DIR="$HOME/.cloudflared"
mkdir -p "$CLOUDFLARED_DIR"
CONFIG_FILE="$CLOUDFLARED_DIR/config.yml"

cat > "$CONFIG_FILE" <<EOF
tunnel: ${TUNNEL_ID}
credentials-file: ${CLOUDFLARED_DIR}/${TUNNEL_ID}.json

ingress:
  - hostname: ${APP_SUBDOMAIN}
    service: http://localhost:${PORT}
  - service: http_status:404
EOF

success "Cloudflare config written to ${CONFIG_FILE}"

# Route DNS
info "Routing ${APP_SUBDOMAIN} → tunnel…"
cloudflared tunnel route dns maelkloud "$APP_SUBDOMAIN" 2>/dev/null || warn "DNS route may already exist."
success "DNS routes configured"

# ── STEP 4: launchd services (auto-start on boot) ─────────
step "Step 4 / 5 — Auto-start on Boot"

PLIST_SERVER="$HOME/Library/LaunchAgents/${SERVICE_NAME}.plist"
PLIST_TUNNEL="$HOME/Library/LaunchAgents/com.maelkloud.tunnel.plist"

# Server plist
cat > "$PLIST_SERVER" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>             <string>${SERVICE_NAME}</string>
    <key>ProgramArguments</key>
    <array>
        <string>$(which $PYTHON)</string>
        <string>${SCRIPT_DIR}/server.py</string>
        <string>--prod</string>
    </array>
    <key>WorkingDirectory</key>  <string>${SCRIPT_DIR}</string>
    <key>RunAtLoad</key>         <true/>
    <key>KeepAlive</key>         <true/>
    <key>StandardOutPath</key>   <string>${SCRIPT_DIR}/server.log</string>
    <key>StandardErrorPath</key> <string>${SCRIPT_DIR}/server-error.log</string>
    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin:/opt/homebrew/bin</string>
    </dict>
</dict>
</plist>
EOF

# Tunnel plist
CLOUDFLARED_BIN=$(which cloudflared)
cat > "$PLIST_TUNNEL" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>             <string>com.maelkloud.tunnel</string>
    <key>ProgramArguments</key>
    <array>
        <string>${CLOUDFLARED_BIN}</string>
        <string>tunnel</string>
        <string>--config</string>
        <string>${CONFIG_FILE}</string>
        <string>run</string>
    </array>
    <key>RunAtLoad</key>         <true/>
    <key>KeepAlive</key>         <true/>
    <key>StandardOutPath</key>   <string>${SCRIPT_DIR}/tunnel.log</string>
    <key>StandardErrorPath</key> <string>${SCRIPT_DIR}/tunnel-error.log</string>
</dict>
</plist>
EOF

success "launchd plists written"

# Load / reload services
for PLIST in "$PLIST_SERVER" "$PLIST_TUNNEL"; do
  launchctl unload "$PLIST" 2>/dev/null || true
  launchctl load   "$PLIST"
done
success "Services started and set to auto-start on boot"

# ── STEP 5: Done ──────────────────────────────────────────
step "Step 5 / 5 — All Done! 🎉"

echo "  Your app is now live. DNS can take up to 5 minutes to propagate."
echo ""
echo "  ┌─────────────────────────────────────────────┐"
echo "  │   🌐  https://${APP_SUBDOMAIN}          │"
echo "  │                                             │"
echo "  │   Anyone can now visit your site and        │"
echo "  │   create their own account.                 │"
echo "  │                                             │"
echo "  │   (maelkloud.com is free for your future    │"
echo "  │    marketing / landing page)                │"
echo "  └─────────────────────────────────────────────┘"
echo ""
echo "  Useful commands:"
echo "    View server logs:   tail -f ${SCRIPT_DIR}/server.log"
echo "    View tunnel logs:   tail -f ${SCRIPT_DIR}/tunnel.log"
echo "    Stop everything:    launchctl unload ~/Library/LaunchAgents/com.maelkloud.*.plist"
echo "    Start everything:   launchctl load   ~/Library/LaunchAgents/com.maelkloud.*.plist"
echo ""
