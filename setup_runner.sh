#!/bin/bash
# ═══════════════════════════════════════════════════════════
#  maelkloud — GitHub Actions Self-Hosted Runner Setup
#  Run this ONCE on your Mac Mini to enable auto-deployment.
#
#  What it does:
#    1. Clones your GitHub repo to ~/maelkloud
#    2. Downloads & configures the GitHub Actions runner
#    3. Installs the runner as a launchd service (auto-starts on boot)
#
#  Usage:
#    bash setup_runner.sh
# ═══════════════════════════════════════════════════════════

set -e

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; BLUE='\033[0;34m'; NC='\033[0m'
info()    { echo -e "${BLUE}▸${NC} $1"; }
success() { echo -e "${GREEN}✓${NC} $1"; }
warn()    { echo -e "${YELLOW}!${NC} $1"; }
error()   { echo -e "${RED}✗${NC} $1"; exit 1; }
step()    { echo -e "\n${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"; echo -e "  ${GREEN}$1${NC}"; echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"; }

DEPLOY_DIR="$HOME/maelkloud"
RUNNER_DIR="$HOME/actions-runner"

echo ""
echo "  ██████████████████████████████████████████"
echo "  ██                                      ██"
echo "  ██   maelkloud — CI/CD Runner Setup     ██"
echo "  ██                                      ██"
echo "  ██████████████████████████████████████████"
echo ""

# ── Gather info ───────────────────────────────────────────
step "Step 1 / 4 — Repository Info"
echo ""
read -p "  Your GitHub username (e.g. ismael): " GH_USER
read -p "  Your GitHub repo name (e.g. maelkloud): " GH_REPO

REPO_URL="https://github.com/${GH_USER}/${GH_REPO}.git"
REPO_API="https://github.com/${GH_USER}/${GH_REPO}"

echo ""
info "Repo URL: ${REPO_URL}"
echo ""

# ── Clone / update repo ───────────────────────────────────
step "Step 2 / 4 — Clone Repository"

if [ -d "$DEPLOY_DIR/.git" ]; then
  warn "Directory $DEPLOY_DIR already exists and is a git repo — pulling latest…"
  git -C "$DEPLOY_DIR" pull origin main
  success "Repo updated"
else
  info "Cloning ${REPO_URL} → ${DEPLOY_DIR}…"
  git clone "$REPO_URL" "$DEPLOY_DIR"
  success "Repo cloned to ${DEPLOY_DIR}"
fi

# ── Download GitHub Actions runner ───────────────────────
step "Step 3 / 4 — Install GitHub Actions Runner"

# Detect architecture
ARCH=$(uname -m)
if [ "$ARCH" = "arm64" ]; then
  RUNNER_ARCH="arm64"
else
  RUNNER_ARCH="x64"
fi

# Get latest runner version from GitHub API
info "Fetching latest runner version…"
RUNNER_VERSION=$(curl -sf https://api.github.com/repos/actions/runner/releases/latest \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['tag_name'].lstrip('v'))" 2>/dev/null \
  || echo "2.323.0")  # fallback version
info "Runner version: ${RUNNER_VERSION}"

RUNNER_PKG="actions-runner-osx-${RUNNER_ARCH}-${RUNNER_VERSION}.tar.gz"
RUNNER_URL="https://github.com/actions/runner/releases/download/v${RUNNER_VERSION}/${RUNNER_PKG}"

mkdir -p "$RUNNER_DIR"
cd "$RUNNER_DIR"

if [ ! -f "./config.sh" ]; then
  info "Downloading runner package…"
  curl -fsSL -o "$RUNNER_PKG" "$RUNNER_URL"
  tar xzf "$RUNNER_PKG"
  rm "$RUNNER_PKG"
  success "Runner extracted to ${RUNNER_DIR}"
else
  success "Runner already downloaded — skipping"
fi

# ── Configure runner ──────────────────────────────────────
info "Now you need a registration token from GitHub."
echo ""
echo "  Please open this URL in your browser:"
echo ""
echo "  ┌──────────────────────────────────────────────────────────┐"
echo "  │  ${REPO_API}/settings/actions/runners/new?arch=${RUNNER_ARCH}  │"
echo "  └──────────────────────────────────────────────────────────┘"
echo ""
echo "  1. Click 'Configure' on that page"
echo "  2. Copy the token from the command that looks like:"
echo "     ./config.sh --url ... --token XXXXXXXXXXXXX"
echo "  3. Paste just the token value below"
echo ""
read -p "  Paste your runner registration token: " RUNNER_TOKEN

if [ -z "$RUNNER_TOKEN" ]; then
  error "No token provided. Please re-run this script."
fi

# Configure the runner (non-interactive)
./config.sh \
  --url "$REPO_API" \
  --token "$RUNNER_TOKEN" \
  --name "mac-mini" \
  --labels "self-hosted,macOS,mac-mini" \
  --work "$RUNNER_DIR/_work" \
  --unattended \
  --replace

success "Runner configured"

# ── Install as launchd service ────────────────────────────
step "Step 4 / 4 — Install as Service (auto-start on boot)"

cd "$RUNNER_DIR"

# Install the service (uses macOS launchd under the hood)
sudo ./svc.sh install
sudo ./svc.sh start

success "Runner service installed and started"

# Verify it's running
sleep 2
if ./svc.sh status 2>/dev/null | grep -q "running\|active"; then
  success "Runner is online and listening for jobs"
else
  warn "Runner may take a few seconds to show as online — check GitHub in a moment"
fi

# ── Done ─────────────────────────────────────────────────
echo ""
echo "  ┌──────────────────────────────────────────────────────┐"
echo "  │   🎉  CI/CD pipeline is ready!                       │"
echo "  │                                                      │"
echo "  │   Next steps:                                        │"
echo "  │   1. Go to: ${REPO_API}                              │"
echo "  │      Settings → Actions → Runners                    │"
echo "  │      You should see 'mac-mini' listed as Idle ✓      │"
echo "  │                                                      │"
echo "  │   2. Push any change to the main branch              │"
echo "  │      → your Mac Mini will auto-pull & restart        │"
echo "  │                                                      │"
echo "  │   View deploy logs:                                  │"
echo "  │   ${REPO_API}/actions                               │"
echo "  └──────────────────────────────────────────────────────┘"
echo ""
echo "  Useful commands:"
echo "    Runner status:  cd $RUNNER_DIR && ./svc.sh status"
echo "    Runner logs:    tail -f $RUNNER_DIR/_diag/Runner_*.log"
echo "    Stop runner:    cd $RUNNER_DIR && sudo ./svc.sh stop"
echo "    Start runner:   cd $RUNNER_DIR && sudo ./svc.sh start"
echo "    Manual deploy:  bash $DEPLOY_DIR/deploy.sh"
echo ""
