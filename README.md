# snapshotdb

A minimal log-based table state engine with atomic commit protocol, written in C++17.

## Architecture

```
table_root/
├── _log/           ← sequential JSON log files (the source of truth)
│   ├── 0.json
│   ├── 1.json
│   └── ...
└── data/           ← actual data files
    └── tmp/        ← staging area for uncommitted files
```

### Week 1 — Log = Database

The log directory is the database. Table state at any version is reconstructed
by replaying log entries — never by scanning `data/`.

### Week 2 — Atomic Commit Protocol

Commits follow a strict write-fsync-rename protocol. Only the `rename` step
makes a commit visible. A crash before rename = no commit. A crash after = durable commit.

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

## Project structure

**Core engine** (`snapshotdb` library — `include/` + `src/`):

```
include/snapshotdb/   — public API headers
src/                  — implementation + crash_commit helper binary
```

**Non-core** (optional link targets / tooling):

```
extras/include/snapshotdb/invariant_checker.h
extras/src/invariant_checker.cpp     → static lib: snapshotdb_invariant

tools/demo.cpp                       → executable: demo (links snapshotdb_invariant)
```

Tests link `snapshotdb_invariant` for invariant checks; production embeds may link only `snapshotdb`.

```
tests/
  test_*.cpp
```
