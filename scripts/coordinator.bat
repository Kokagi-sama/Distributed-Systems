@echo off
REM This script starts two coordinator servers for a primary-replica setup.

REM Define common variables
SET PYTHON_EXE=python
SET SCRIPT_PATH=..\coordinator\coordinator_server.py
SET FILE_LIST=..\coordinator\file_list.csv
SET FILESERVERS=localhost:50051 localhost:50052 localhost:50053
SET SNAPSHOT_INTERVAL=60
SET TIMEOUT=10

REM Coordinator 1 settings (Primary)
SET COORD1_ID=1
SET COORD1_PORT=50054
SET COORD1_REPLICA=localhost:50055

REM Coordinator 2 settings (Secondary)
SET COORD2_ID=2
SET COORD2_PORT=50055
SET COORD2_REPLICA=localhost:50054

echo Starting Coordinator 1 (Primary)...

REM Start Coordinator 1 in a new window that stays open
START "Coordinator 1" cmd /k %PYTHON_EXE% %SCRIPT_PATH% ^
    --server-id %COORD1_ID% ^
    --port %COORD1_PORT% ^
    --replica-address %COORD1_REPLICA% ^
    --file-list %FILE_LIST% ^
    --fileserver-addresses %FILESERVERS% ^
    --snapshot-interval %SNAPSHOT_INTERVAL% ^
    --timeout %TIMEOUT%

echo Waiting for 10 seconds before starting Coordinator 2...
REM A pause to allow the primary server to start up before the secondary connects.
timeout /t 10 /nobreak > NUL

echo Starting Coordinator 2 (Secondary)...
REM Start Coordinator 2 in a new window that stays open
START "Coordinator 2" cmd /k %PYTHON_EXE% %SCRIPT_PATH% ^
    --server-id %COORD2_ID% ^
    --port %COORD2_PORT% ^
    --replica-address %COORD2_REPLICA% ^
    --file-list %FILE_LIST% ^
    --fileserver-addresses %FILESERVERS% ^
    --snapshot-interval %SNAPSHOT_INTERVAL% ^
    --timeout %TIMEOUT%

echo Both coordinator servers have been launched in separate windows.