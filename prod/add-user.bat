@echo off
setlocal

:: Load .env
if not exist "%~dp0.env" (
    echo ERROR: %~dp0.env not found.
    echo Create it with:
    echo.
    echo   SERVER=user@your.server.ip
    echo   CADDY_CONTAINER=jobs-caddy-1
    echo   COMPOSE_DIR=/opt/automation/jobs
    echo.
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in ("%~dp0.env") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
)

if "%~1"=="" (
    echo Usage: add-user.bat ^<username^>
    echo Example: add-user.bat john.doe
    exit /b 1
)

set USERNAME=%~1

:: Generate random 12-char password
for /f "usebackq" %%P in (`powershell -Command "-join ((65..90)+(97..122)+(48..57) | Get-Random -Count 12 | %%{[char]$_})"`) do set PASSWORD=%%P

:: SSH to server, generate bcrypt hash
echo.
echo Generating hash via %SERVER%...
for /f "usebackq delims=" %%H in (`ssh %SERVER% "docker exec %CADDY_CONTAINER% caddy hash-password --plaintext '%PASSWORD%'"`) do set HASH=%%H

echo.
echo ========================================
echo   User:     %USERNAME%
echo   Password: %PASSWORD%
echo ========================================
echo.
echo Copy this line to users.caddyfile:
echo.
echo     %USERNAME% %HASH%
echo.
echo Then run: prod\reload-caddy.bat
echo.
endlocal
