@echo off
chcp 65001 >nul
title TickTrader API Lab
cd /d "%~dp0"

py -3 --version >nul 2>nul
if not errorlevel 1 (
  set "PY_CMD=py -3"
  goto run
)

python --version >nul 2>nul
if not errorlevel 1 (
  set "PY_CMD=python"
  goto run
)

echo Python 3 was not found.
echo Install Python 3.11 or newer from https://www.python.org/downloads/
echo During install, enable "Add python.exe to PATH".
echo.
pause
exit /b 1

:run
echo Starting TickTrader API Lab...
echo Keep this window open while you use the tool.
echo Local address: http://127.0.0.1:8765
echo.
start "" powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Sleep -Seconds 2; Start-Process 'http://127.0.0.1:8765'"
%PY_CMD% ticktrader_gui.py --host 127.0.0.1 --port 8765
echo.
echo TickTrader API Lab stopped.
pause
