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

echo Reloading Caddy on %SERVER%...
ssh %SERVER% "docker exec %CADDY_CONTAINER% caddy reload --config /etc/caddy/Caddyfile"

if %errorlevel% neq 0 (
    echo.
    echo FAILED. Check that the server is reachable and Caddyfile is valid.
    exit /b 1
)

echo.
echo Done. Caddy reloaded.
endlocal
