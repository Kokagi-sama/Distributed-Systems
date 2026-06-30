# Distributed Systems

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Running the System](#running-the-system)
- [Project Structure](#project-structure)
  
## Overview
A distributed AFS system with coordinator-based replication and task management.

## Architecture
- **File Server**: Handles file storage and retrieval
- **Coordinator**: Manages worker registration and tasks coordination
- **Worker**: Interacts with file server and coordinator for finding unique prime numbers

## Prerequisites
- Python 3.11
- gRPC

## Installation
```bash
# Install required packages
pip install grpcio grpcio-tools
```

If the `generated/` folder is empty or you need to regenerate gRPC code from proto files:
```bash
python -m grpc_tools.protoc -I. --python_out=generated --grpc_python_out=generated *.proto
```

## Configuration

### Default IP Addresses
Runs on localhost by default. To modify address, edit the respective localhost to the IP Address needed

### Default Ports
- **File Servers**: 50051, 50052, 50053
- **Coordinators**: 50054, 50055

### Customising Ports
To modify ports, edit the respective batch files in the `scripts/` folder:
- [scripts/fileserver.bat](scripts/fileserver.bat) - Update `--port` arguments
- [scripts/coordinator.bat](scripts/coordinator.bat) - Update `COORD1_PORT`, `COORD2_PORT`, and `FILESERVERS` variables
- [scripts/worker.bat](scripts/worker.bat) - Update `COORDINATORS` and `FILESERVERS` variables

Ensure all components reference the correct address and ports when making changes.

## Running the System
1. Prepare the File List
   - Create a `file_list.csv` file in the `coordinator/` folder
   - Format: `file_name,no_of_nums` (one file per line)
   - Example:
     ```
     file_name,no_of_nums
     data_input_001.txt,1000
     data_input_002.txt,2000
     ```

2. Start the File Servers
   ```
   scripts\fileserver.bat
   ```

3. Start the Coordinators
   ```
   scripts\coordinator.bat
   ```

4. Run Workers
   ```
   scripts\worker.bat
   ```

## Project Structure
```text
├── client/           # Worker implementation
├── coordinator/      # Coordinator implementation
├── fileserver/       # File server implementation
├── generated/        # Auto-generated gRPC code
└── scripts/          # Startup scripts
