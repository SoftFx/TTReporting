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

if "%~1"=="" (
    echo Usage: add-user.bat ^<username^>
    echo Example: add-user.bat john.doe
    exit /b 1
)

set USERNAME=%~1

:: Validate username (letters, numbers, dots, underscores, hyphens only)
echo %USERNAME% | findstr /R "^[a-zA-Z0-9._-]*$" > nul
if %errorlevel% neq 0 (
    echo ERROR: Username must contain only letters, numbers, dots, underscores, or hyphens.
    exit /b 1
)

:: Generate random 12-char password
for /f "usebackq" %%P in (`powershell -Command "-join ((65..90)+(97..122)+(48..57) | Get-Random -Count 12 | %%{[char]$_})"`) do set PASSWORD=%%P

:: SSH to server, pipe password via stdin to generate bcrypt hash
echo.
echo Generating hash via %SERVER%...
for /f "usebackq delims=" %%H in (`echo %PASSWORD% | ssh %SERVER% "docker exec -i %CADDY_CONTAINER% caddy hash-password 2>/dev/null" ^| findstr /R "^\$2"`) do set HASH=%%H

if "%HASH%"=="" (
    echo.
    echo ERROR: Failed to generate hash. Check SSH connection and container name.
    exit /b 1
)

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
