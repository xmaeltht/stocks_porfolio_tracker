@echo off
:: ─────────────────────────────────────────────
::  Portfolio Monitor — Start Script (Windows)
:: ─────────────────────────────────────────────
:: Double-click this file to launch the dashboard

cd /d "%~dp0"

echo.
echo ═══════════════════════════════════════
echo   Portfolio Monitor — Starting...
echo ═══════════════════════════════════════
echo.

:: Check for Python
python --version >nul 2>&1
IF ERRORLEVEL 1 (
    echo ERROR: Python not found. Please install Python 3 from:
    echo        https://www.python.org/downloads/
    echo.
    echo Make sure to check "Add Python to PATH" during installation.
    pause
    exit /b 1
)

echo Installing/checking yfinance...
python -m pip install yfinance -q

echo.
echo Starting server at http://localhost:8080
echo Press Ctrl+C to stop
echo.

python server.py
pause
