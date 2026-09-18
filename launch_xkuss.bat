@echo off
cd /d "%~dp0"
echo Starting approval dashboard + Xkuss bot together...
echo.
echo   Approval queue:  http://127.0.0.1:8799/
echo   Chameleon-AI:   https://chamaleon-ai-0c02461b.base44.app/AgentWorkspace
echo.
python launch_all.py xkuss
pause
