@echo off
setlocal

set SERVER=user@YOUR_SERVER_IP

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
for /f "usebackq delims=" %%H in (`ssh %SERVER% "docker exec jobs-caddy-1 caddy hash-password --plaintext '%PASSWORD%'"`) do set HASH=%%H

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
echo Then reload: docker exec jobs-caddy-1 caddy reload --config /etc/caddy/Caddyfile
echo.
endlocal
