#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  maelkloud — Switch tunnel to stocks.maelkloud.com
#  Run this ONCE on your Mac Mini
# ═══════════════════════════════════════════════════════════

GREEN='\033[0;32m'; BLUE='\033[0;34m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'
info()    { echo -e "${BLUE}▸${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }
error()   { echo -e "${RED}✗${NC} $1"; exit 1; }

CONFIG="$HOME/.cloudflared/config.yml"
NEW_HOST="stocks.maelkloud.com"

echo ""
echo "  Switching tunnel → ${NEW_HOST}"
echo ""

# ── 1. Read tunnel ID from existing config ─────────────────
TUNNEL_ID=$(grep '^tunnel:' "$CONFIG" 2>/dev/null | awk '{print $2}')
[ -z "$TUNNEL_ID" ] && error "Could not read tunnel ID from $CONFIG"
success "Tunnel ID: $TUNNEL_ID"

CREDS="$HOME/.cloudflared/${TUNNEL_ID}.json"

# ── 2. Rewrite config.yml ──────────────────────────────────
info "Updating ~/.cloudflared/config.yml …"
cat > "$CONFIG" <<EOF
tunnel: ${TUNNEL_ID}
credentials-file: ${CREDS}

ingress:
  - hostname: ${NEW_HOST}
    service: http://localhost:8080
  - service: http_status:404
EOF
success "config.yml updated"

# ── 3. Add DNS route for stocks.maelkloud.com ──────────────
info "Adding DNS route for ${NEW_HOST} …"
cloudflared tunnel route dns maelkloud "$NEW_HOST" 2>/dev/null \
  && success "DNS route added for ${NEW_HOST}" \
  || warn "DNS route may already exist (that's fine)"

# ── 4. Restart the tunnel service ─────────────────────────
info "Restarting Cloudflare tunnel …"
launchctl unload ~/Library/LaunchAgents/com.maelkloud.tunnel.plist 2>/dev/null || true
launchctl load   ~/Library/LaunchAgents/com.maelkloud.tunnel.plist
success "Tunnel restarted"

echo ""
echo "  ┌────────────────────────────────────────────────┐"
echo "  │   ✅  https://stocks.maelkloud.com            │"
echo "  │                                                │"
echo "  │   DNS can take up to 2 minutes to update.     │"
echo "  │   maelkloud.com stays free for your landing   │"
echo "  │   page whenever you're ready.                 │"
echo "  └────────────────────────────────────────────────┘"
echo ""
