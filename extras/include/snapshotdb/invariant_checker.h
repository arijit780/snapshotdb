#pragma once

// ---------------------------------------------------------------------------
// invariant_checker.h — The system's immune system (extras — not core)
//
// Lives under extras/ so the main library stays lean. Link target
// `snapshotdb_invariant` if you want these checks in your binary or tests.
//
// This module enforces the five structural invariants that must ALWAYS hold
// for a snapshotdb table.  If any invariant is violated, it means there is
// a bug in the engine — not just bad user input, but a real correctness
// failure.
//
// THE FIVE INVARIANTS:
//
//   1. CONTIGUOUS VERSIONS
//      The log must be a gapless chain: 0, 1, 2, ..., N.
//      No missing versions in the middle.
//
//   2. NO DUPLICATE VERSIONS
//      Each version number appears exactly once in the log directory.
//      No two files can claim the same version.
//
//   3. DETERMINISTIC SNAPSHOT
//      build_snapshot(V) called twice must return the identical set.
//      If the snapshot depends on read order or timing, we have a bug.
//
//   4. NO DUPLICATE FILES IN SNAPSHOT
//      The active file set at any version must contain each filename at
//      most once.  (Guaranteed by std::set, but we verify the log doesn't
//      add a file that's already active.)
//
//   5. NO GHOST REMOVES
//      A log entry must never remove a file that isn't currently active.
//      Removing a non-existent file means the log is logically inconsistent.
//
// HOW TO USE:
//
//   // Check everything — throws InvariantViolation on failure
//   assert_all_invariants(table_path);
//
//   // Or check individually
//   assert_contiguous_versions(table_path);
//   assert_no_duplicate_versions(table_path);
//   assert_deterministic_snapshot(table_path, version);
//   assert_no_duplicate_files(table_path, version);
//   assert_no_ghost_removes(table_path, version);
//
// WHEN TO CALL:
//   After every commit, after recovery, after vacuum — anywhere you want
//   to prove the system is still correct.  In production, you might call
//   this in a background health check.  In tests, call it after every
//   operation.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <filesystem>

#include "snapshotdb/errors.h"

namespace snapshotdb {

// ---------------------------------------------------------------------------
// InvariantViolation — thrown when any structural invariant is broken
//
// This is a FATAL error.  It means the engine has a bug, not that the user
// did something wrong.  The message describes exactly which invariant was
// violated and where.
// ---------------------------------------------------------------------------
class InvariantViolation : public LogError {
public:
    std::string invariant_name;
    InvariantViolation(const std::string& name, const std::string& detail)
        : LogError("INVARIANT VIOLATION [" + name + "]: " + detail),
          invariant_name(name) {}
};

// ---------------------------------------------------------------------------
// Individual invariant checks — each throws InvariantViolation on failure
// ---------------------------------------------------------------------------

// Invariant 1: versions 0..latest form a gapless chain, every file parses
void assert_contiguous_versions(const std::filesystem::path& table_path);

// Invariant 2: no two log files claim the same version number
void assert_no_duplicate_versions(const std::filesystem::path& table_path);

// Invariant 3: build_snapshot(V) is deterministic (two calls == same result)
void assert_deterministic_snapshot(
    const std::filesystem::path& table_path,
    uint64_t version
);

// Invariant 4: no filename appears more than once in the active set at V
// (also checks that no single log entry adds the same file twice)
void assert_no_duplicate_files(
    const std::filesystem::path& table_path,
    uint64_t version
);

// Invariant 5: every remove in the log targets a file that is currently active
// (no removing a file that was never added or was already removed)
void assert_no_ghost_removes(
    const std::filesystem::path& table_path,
    uint64_t version
);

// ---------------------------------------------------------------------------
// assert_all_invariants — run every check on the full log
//
// This is the "prove the system is correct" function.  Call it after every
// operation if you want maximum confidence.
// ---------------------------------------------------------------------------
void assert_all_invariants(const std::filesystem::path& table_path);

} // namespace snapshotdb
