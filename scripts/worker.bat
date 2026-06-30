@echo off
REM Batch file to start 4 client instances
REM Usage: start_clients.bat

REM Navigate to project root (2 levels up from client/scripts)
cd /d "%~dp0..\"

REM Store the project root path
set PROJECT_ROOT=%CD%

REM Set your coordinator and fileserver addresses here
set COORDINATORS=localhost:50054 localhost:50055
set FILESERVERS=localhost:50051 localhost:50052 localhost:50053

REM To add more workers, copy the "Worker" command and rename to a different ID
echo Starting 4 client instances...
echo.

REM Start Worker 1
echo Starting Worker 1...
start "Worker 1" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_1 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

REM Wait 2 secs before starting next client
timeout /t 2 /nobreak >nul

REM Start Worker 2
echo Starting Worker 2...
start "Worker 2" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_2 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

REM Wait 2 secs before starting next client
timeout /t 2 /nobreak >nul

REM Start Worker 3
echo Starting Worker 3...
start "Worker 3" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_3 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

REM Wait 2 secs before starting next client
timeout /t 2 /nobreak >nul

REM Start Worker 4
echo Starting Worker 4...
start "Worker 4" cmd /k "cd /d %PROJECT_ROOT% && python -u client\client.py client_4 --coordinators %COORDINATORS% --fileservers %FILESERVERS%"

echo.
echo All 4 workers started!
echo Each worker is running in its own window.
