#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────
#  Portfolio Monitor — Run Script
#  Runs hourly during US market hours via cron:
#    0 9-16 * * 1-5  /path/to/Stock-porfolio/run_monitor.sh
# ─────────────────────────────────────────────────────────────

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG="$DIR/monitor.log"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')

echo "" >> "$LOG"
echo "═══════════════════════════════════" >> "$LOG"
echo "  Run: $TIMESTAMP" >> "$LOG"
echo "═══════════════════════════════════" >> "$LOG"

# Ensure dependencies
python3 -m pip install yfinance openpyxl --break-system-packages -q >> "$LOG" 2>&1

# Run the monitor
python3 "$DIR/portfolio_monitor.py" 2>&1 | tee -a "$LOG"

echo "Done: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG"
