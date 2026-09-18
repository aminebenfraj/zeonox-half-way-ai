@echo off
cd /d "%~dp0"
echo Starting approval dashboard (no bots)...
echo   On this PC:        http://127.0.0.1:8799/
echo   From your phone:   see TAILSCALE_DASHBOARD_URL in .env
echo.
echo Run setup_tailscale.bat once before using the phone URL.
python approval_server.py
pause
