@echo off
setlocal

:: Load .env
if not exist "%~dp0.env" (
    echo ERROR: %~dp0.env not found.
    echo Create it from .env.example with:
    echo.
    echo   SERVER=user@your.server.ip
    echo   CADDY_CONTAINER=jobs-caddy-1
    echo   REMOTE_PROD_DIR=/path/to/prod
    echo.
    exit /b 1
)

for /f "usebackq tokens=1,* delims==" %%A in ("%~dp0.env") do (
    if not "%%A"=="" set "%%A=%%B"
)

if "%SERVER%"=="" (
    echo ERROR: SERVER is not set in %~dp0.env
    exit /b 1
)

if "%CADDY_CONTAINER%"=="" (
    echo ERROR: CADDY_CONTAINER is not set in %~dp0.env
    exit /b 1
)

if "%REMOTE_PROD_DIR%"=="" (
    echo ERROR: REMOTE_PROD_DIR is not set in %~dp0.env
    echo Example:
    echo   REMOTE_PROD_DIR=/opt/tt-reporting/prod
    exit /b 1
)

if "%~1"=="" (
    echo Usage: deploy-product.bat ^<compose-service-name^>
    echo Example: deploy-product.bat user-data-app
    exit /b 1
)

set SERVICE=%~1

:: Validate compose service name (letters, numbers, dots, underscores, hyphens only)
echo %SERVICE% | findstr /R "^[a-zA-Z0-9._-]*$" > nul
if %errorlevel% neq 0 (
    echo ERROR: Service name must contain only letters, numbers, dots, underscores, or hyphens.
    exit /b 1
)

echo Deploying %SERVICE% on %SERVER%...
echo Remote prod directory: %REMOTE_PROD_DIR%
echo.

ssh -t %SERVER% "cd '%REMOTE_PROD_DIR%' && sudo docker compose pull %SERVICE% && sudo docker compose up -d %SERVICE% && sudo docker exec %CADDY_CONTAINER% caddy validate --config /etc/caddy/Caddyfile && sudo docker exec %CADDY_CONTAINER% caddy reload --config /etc/caddy/Caddyfile"

if %errorlevel% neq 0 (
    echo.
    echo FAILED. Check SSH access, REMOTE_PROD_DIR, compose service name, image availability, and Caddyfile validity.
    exit /b 1
)

echo.
echo Done. %SERVICE% deployed and Caddy reloaded.
pause
endlocal
