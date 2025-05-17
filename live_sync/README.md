# Live-Sync ETL Pipeline

This module implements a near real-time synchronization system that keeps the ChatStats database updated with changes from the macOS Messages app (`chat.db`).

## Overview

The live-sync pipeline monitors the SQLite WAL file of `chat.db` for changes and incrementally processes only new messages and attachments since the last sync point.

## Key Features

1. **WAL-driven**: Uses file system events to detect changes to the `chat.db-wal` file
2. **Incremental**: Only processes new data since the last watermark
3. **Efficient**: Reuses transformation logic from existing ETL components
4. **Low-latency**: Debounces events to handle bursts while maintaining responsiveness

## Components

- **state.py**: Manages the synchronization watermark (high-water mark)
- **extractors.py**: Fast queries to extract new data from `chat.db`
- **sync_messages.py**: Processes and imports new messages
- **sync_attachments.py**: Processes and imports new attachments
- **wal.py**: Watchdog-based file monitor and orchestrator

## Dependencies

- `watchdog`: For the file system monitoring (macOS FSEvents)

## Installation

```
pip install -r requirements.txt
```

## Architecture

```
AX/Overlay          Electron main           backend.live_sync
┌─────────┐  IPC  ┌─────────┐   stdio   ┌───────────────────────┐
│ overlay │◀────▶│ backend │◀──────────│ WAL watcher           │
└─────────┘       └─────────┘           │  ↳ on burst → pull N  │
                                         │  ↳ call mini-ETLs    │
                                         └───────────────────────┘
```

The watcher starts automatically when the FastAPI application launches. 