# snapshotdb

A minimal log-based table state engine with atomic commit protocol, written in C++17.

## Architecture

```
table_root/
├── metadata/
│   └── versions/   ← sequential JSON log files (source of truth)
│       ├── 0.json
│       ├── 1.json
│       └── ...
└── data/
    ├── staging/    ← uncommitted writes (cleared after commit)
    └── ...           ← committed object files (logical names from the log)
```

### Log = database

The version chain under `metadata/versions/` is the database. Table state at any version is reconstructed by replaying log entries, not by scanning `data/` alone.

### Atomic commit protocol

Commits use write → fsync → rename for log entries. Only the rename makes a new version visible. A crash before rename means no commit; after rename, the commit is durable.

## Building

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Debug
cmake --build build
```

## Running tests & demo

```bash
cd build && ctest --output-on-failure
./demo
```

Optional HTTP server (after build): `./snapshotdb_http [--listen HOST:PORT] <table_directory>`

## Project structure

**Core engine** (`snapshotdb` library):

- `include/snapshotdb/` — public shims; implementations under `catalog/`, `common/`, `engine/`, `storage/`
- `src/engine/`, `src/storage/` — engine and storage backends
- `src/crash_commit.cpp` — crash-injection helper for tests

**Non-core**

- `extras/` — `snapshotdb_invariant` (structural checks)
- `tools/demo.cpp`, `tools/http_server.cpp`

Tests live under `tests/`.
