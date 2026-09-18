@echo off
cd /d "%~dp0"
set HOST=0.0.0.0
echo Starting approval dashboard (no bots)...
echo   On this PC:        http://127.0.0.1:8799/
echo   From phone/other device on the same network:
for /f "tokens=2 delims=:" %%a in ('ipconfig ^| findstr /c:"IPv4 Address"') do echo     http://%%a:8799/  (trim the leading space)
python approval_server.py
pause
