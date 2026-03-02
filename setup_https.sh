#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# setup_https.sh  —  Cloudflare Tunnel + HTTPS for maelkloud
#
# Run ONCE on your Mac Mini:
#   bash ~/HomeLab/Stock-porfolio/setup_https.sh
#
# What it does:
#   1. Installs cloudflared via Homebrew
#   2. Authenticates with your Cloudflare account (opens browser)
#   3. Creates a named tunnel called "maelkloud"
#   4. Writes ~/.cloudflared/config.yml pointing to localhost:8080
#   5. Routes stocks.maelkloud.com DNS → tunnel in Cloudflare
#   6. Installs cloudflared as a macOS LaunchDaemon (starts on boot, auto-restarts)
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

TUNNEL_NAME="maelkloud"
HOSTNAME="stocks.maelkloud.com"
PORT=8080
CF_DIR="$HOME/.cloudflared"

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║        maelkloud HTTPS Setup — Cloudflare Tunnel     ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""

# ── 1. Install cloudflared ────────────────────────────────────────────────────
if command -v cloudflared &>/dev/null; then
  echo "✅ cloudflared already installed: $(cloudflared version)"
else
  echo "📦 Installing cloudflared via Homebrew…"
  brew install cloudflare/cloudflare/cloudflared
  echo "✅ cloudflared installed"
fi

# ── 2. Authenticate (opens browser — log in with your Cloudflare account) ────
echo ""
echo "🌐 Opening Cloudflare login (browser will open)…"
echo "   → Select the zone that owns: $HOSTNAME"
cloudflared tunnel login

# ── 3. Create tunnel (skip if already exists) ─────────────────────────────────
echo ""
if cloudflared tunnel list 2>/dev/null | grep -q "$TUNNEL_NAME"; then
  echo "✅ Tunnel '$TUNNEL_NAME' already exists"
else
  echo "🔧 Creating tunnel: $TUNNEL_NAME"
  cloudflared tunnel create "$TUNNEL_NAME"
fi

# Grab tunnel UUID
TUNNEL_ID=$(cloudflared tunnel list --output json 2>/dev/null \
  | python3 -c "import sys,json; t=[x for x in json.load(sys.stdin) if x['name']=='$TUNNEL_NAME']; print(t[0]['id'])" \
  2>/dev/null || true)

if [ -z "$TUNNEL_ID" ]; then
  echo "❌ Could not find tunnel ID. Check 'cloudflared tunnel list' and try again."
  exit 1
fi
echo "   Tunnel ID: $TUNNEL_ID"

# ── 4. Write config.yml ───────────────────────────────────────────────────────
mkdir -p "$CF_DIR"
CREDS_FILE="$CF_DIR/$TUNNEL_ID.json"

cat > "$CF_DIR/config.yml" <<EOF
tunnel: $TUNNEL_ID
credentials-file: $CREDS_FILE

# Retry on origin errors (server restart during deploy)
originRequest:
  connectTimeout: 10s
  noTLSVerify: false

ingress:
  - hostname: $HOSTNAME
    service: http://localhost:$PORT
    originRequest:
      httpHostHeader: $HOSTNAME
  - service: http_status:404
EOF

echo "✅ Config written to $CF_DIR/config.yml"
cat "$CF_DIR/config.yml"

# ── 5. Route DNS ──────────────────────────────────────────────────────────────
echo ""
echo "🌍 Routing $HOSTNAME → tunnel in Cloudflare DNS…"
cloudflared tunnel route dns "$TUNNEL_NAME" "$HOSTNAME" || \
  echo "⚠️  DNS route may already exist — continuing"

# ── 6. Install as a launchd service (starts on boot, auto-restarts) ──────────
echo ""
echo "⚙️  Installing cloudflared as a macOS background service…"

# cloudflared service install writes a plist to /Library/LaunchDaemons/
# It needs sudo only for the system-level daemon
sudo cloudflared service install

echo ""
echo "▶️  Starting tunnel service…"
sudo launchctl start com.cloudflare.cloudflared

# ── 7. Verify ─────────────────────────────────────────────────────────────────
echo ""
echo "⏳ Waiting 5 seconds for tunnel to come up…"
sleep 5

STATUS=$(sudo launchctl list | grep cloudflared | awk '{print $1}' || true)
if [ "$STATUS" == "0" ] || [ -n "$STATUS" ]; then
  echo "✅ Tunnel service running"
else
  echo "⚠️  Check service: sudo launchctl list | grep cloudflared"
fi

echo ""
echo "╔══════════════════════════════════════════════════════╗"
echo "║  ✅  HTTPS setup complete!                           ║"
echo "║                                                      ║"
echo "║  Your app is now live at:                            ║"
echo "║  👉  https://$HOSTNAME          ║"
echo "║                                                      ║"
echo "║  The tunnel starts automatically on boot.            ║"
echo "║  To restart manually:                                ║"
echo "║    sudo launchctl stop com.cloudflare.cloudflared    ║"
echo "║    sudo launchctl start com.cloudflare.cloudflared   ║"
echo "╚══════════════════════════════════════════════════════╝"
echo ""
