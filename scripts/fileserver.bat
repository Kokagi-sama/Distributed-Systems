@echo off
:: --- Set Working Directory ---
:: This command changes the directory to the parent folder of this script
:: This ensures all paths below are correct, no matter where you run the script from.
cd /D %~dp0..

echo Starting Raft Cluster from %cd%...

:: Define common storage paths
set INPUT_DIR=./fileserver_storage
set OUTPUT_DIR=./fileserver_output
set SNAPSHOT_DIR=./fileserver_snapshots

:: Start Server 1 in a new window
:: Path to python script is now correct (relative to root)
start "Server 1 (Port 50051)" cmd /k python fileserver/FileServer.py --id S1 --port 50051 --peers "localhost:50052,localhost:50053" --input_storage "%INPUT_DIR%" --output_storage "%OUTPUT_DIR%" --snapshot_storage "%SNAPSHOT_DIR%"

:: Wait 5 seconds to let the first one initialize
timeout /t 5 /nobreak >nul

:: Start Server 2 in a new window
:: Path to python script is now correct (relative to root)
start "Server 2 (Port 50052)" cmd /k python fileserver/FileServer.py --id S2 --port 50052 --peers "localhost:50051,localhost:50053" --input_storage "%INPUT_DIR%" --output_storage "%OUTPUT_DIR%" --snapshot_storage "%SNAPSHOT_DIR%"

:: Wait 5 seconds
timeout /t 5 /nobreak >nul

:: Start Server 3 in a new window
:: Path to python script is now correct (relative to root)
start "Server 3 (Port 50053)" cmd /k python fileserver/FileServer.py --id S3 --port 50053 --peers "localhost:50051,localhost:50052" --input_storage "%INPUT_DIR%" --output_storage "%OUTPUT_DIR%" --snapshot_storage "%SNAPSHOT_DIR%"

echo.
echo Cluster started!
echo You should see three new windows, one for each server.
echo To stop the cluster, just close those three windows.
pause