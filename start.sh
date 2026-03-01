#!/bin/bash
# ─────────────────────────────────────────────
#  Portfolio Monitor — Start Script (Mac/Linux)
# ─────────────────────────────────────────────
# Double-click this file, or run: bash start.sh

cd "$(dirname "$0")"

echo ""
echo "═══════════════════════════════════════"
echo "  📊  Portfolio Monitor — Starting…"
echo "═══════════════════════════════════════"

# Check for Python
if command -v python3 &>/dev/null; then
  PYTHON=python3
elif command -v python &>/dev/null; then
  PYTHON=python
else
  echo ""
  echo "❌  Python not found. Please install Python 3 from:"
  echo "    https://www.python.org/downloads/"
  echo ""
  read -p "Press Enter to exit…"
  exit 1
fi

echo "  Python: $($PYTHON --version)"
echo ""

# Install yfinance if missing
$PYTHON -c "import yfinance" 2>/dev/null || {
  echo "📦 Installing yfinance (one-time setup)…"
  $PYTHON -m pip install yfinance --break-system-packages -q
}

echo "  Starting server at http://localhost:8080"
echo "  Press Ctrl+C to stop"
echo ""

$PYTHON server.py
