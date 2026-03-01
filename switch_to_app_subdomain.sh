#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  maelkloud — Switch tunnel from maelkloud.com → app.maelkloud.com
#  Run this ONCE on your Mac Mini
# ═══════════════════════════════════════════════════════════

GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${BLUE}▸${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }

CONFIG="$HOME/.cloudflared/config.yml"

echo ""
echo "  Switching app to → app.maelkloud.com"
echo ""

# ── 1. Read tunnel ID from existing config ─────────────────
TUNNEL_ID=$(grep '^tunnel:' "$CONFIG" | awk '{print $2}')
if [ -z "$TUNNEL_ID" ]; then
  echo "✗ Could not find tunnel ID in $CONFIG"
  exit 1
fi
success "Tunnel ID: $TUNNEL_ID"

CREDS="$HOME/.cloudflared/${TUNNEL_ID}.json"

# ── 2. Rewrite config.yml ──────────────────────────────────
info "Updating ~/.cloudflared/config.yml …"
cat > "$CONFIG" <<EOF
tunnel: ${TUNNEL_ID}
credentials-file: ${CREDS}

ingress:
  - hostname: app.maelkloud.com
    service: http://localhost:8080
  - service: http_status:404
EOF
success "config.yml updated"

# ── 3. Add DNS route for app.maelkloud.com ─────────────────
info "Adding DNS route for app.maelkloud.com …"
cloudflared tunnel route dns maelkloud app.maelkloud.com 2>/dev/null \
  && success "DNS route added for app.maelkloud.com" \
  || warn "DNS route may already exist (that's fine)"

# ── 4. Restart the tunnel service ─────────────────────────
info "Restarting Cloudflare tunnel service …"
launchctl unload ~/Library/LaunchAgents/com.maelkloud.tunnel.plist 2>/dev/null || true
launchctl load   ~/Library/LaunchAgents/com.maelkloud.tunnel.plist
success "Tunnel restarted"

echo ""
echo "  ┌────────────────────────────────────────────┐"
echo "  │   ✅  https://app.maelkloud.com            │"
echo "  │                                            │"
echo "  │   DNS can take up to 2 minutes to update.  │"
echo "  │   maelkloud.com is free for your landing   │"
echo "  │   page whenever you're ready.              │"
echo "  └────────────────────────────────────────────┘"
echo ""
