@echo off
setlocal

:: Load .env
if not exist "%~dp0.env" (
    echo ERROR: %~dp0.env not found.
    echo Create it from .env.example with:
    echo.
    echo   SERVER=user@your.server.ip
    echo   CADDY_CONTAINER=jobs-caddy-1
    echo.
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in ("%~dp0.env") do (
    if not "%%A"=="" set "%%A=%%B"
)

echo Validating and reloading Caddy on %SERVER%...
ssh -t %SERVER% "sudo docker exec %CADDY_CONTAINER% caddy validate --config /etc/caddy/Caddyfile && sudo docker exec %CADDY_CONTAINER% caddy reload --config /etc/caddy/Caddyfile"

if %errorlevel% neq 0 (
    echo.
    echo FAILED. Check that the server is reachable and Caddyfile is valid.
    exit /b 1
)

echo.
echo Done. Caddy reloaded.
pause
endlocal
