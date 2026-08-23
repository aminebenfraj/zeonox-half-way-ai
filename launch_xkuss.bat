@echo off
cd /d "%~dp0"
echo Starting approval dashboard + Xkuss bot together...
echo.
echo   Approval queue:  http://127.0.0.1:8799/
echo   Chameleon page:  http://127.0.0.1:8799/chameleon
echo.
echo On the Chameleon page, the "Automatic Mode" toggle switches Xkuss between:
echo   OFF (Normal) -- the bot uses the real Chameleon-AI site, as usual.
echo   ON  (Auto)   -- the bot pastes chat HTML into this page and gets its reply
echo                   from Groq here instead.
echo.
echo The toggle is read once when Xkuss starts. After changing it, click "Restart"
echo next to Xkuss in Bot Controls on the main dashboard for it to take effect --
echo no need to close this window.
echo.
python launch_all.py xkuss
pause
