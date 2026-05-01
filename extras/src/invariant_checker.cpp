// ---------------------------------------------------------------------------
// invariant_checker.cpp — The five structural invariants (extras — not core)
//
// Implemented under extras/ alongside the header; the core `snapshotdb`
// static library does not include this file.
//
// Each function in this file checks one invariant.  They all follow the same
// pattern:
//   1. Read the log (or parts of it).
//   2. Check a specific property.
//   3. If the property is violated, throw InvariantViolation with a
//      human-readable message explaining exactly what went wrong.
//
// These checks are intentionally simple and brute-force.  They re-read the
// log from scratch rather than trusting cached state, because the whole point
// is to independently verify correctness.
// ---------------------------------------------------------------------------

#include "snapshotdb/invariant_checker.h"
#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"

#include <algorithm>
#include <set>
#include <map>
#include <filesystem>

namespace fs = std::filesystem;

namespace snapshotdb {

// ---------------------------------------------------------------------------
// Invariant 1: CONTIGUOUS VERSIONS
//
// Walk version 0, 1, 2, ..., latest.  For each version, the log file must
// exist and parse correctly.  If latest_version() says N, then files
// 0.json through N.json must all be present and valid.
// ---------------------------------------------------------------------------

void assert_contiguous_versions(const fs::path& table_path) {
    int64_t latest = latest_version(table_path);
    if (latest < 0) return;  // empty table — trivially contiguous

    // read_logs will throw MissingVersionError or CorruptedJsonError if the
    // chain is broken.  We catch those and re-throw as InvariantViolation.
    try {
        auto entries = read_logs(table_path, static_cast<uint64_t>(latest));

        // Double-check: entry count must equal latest + 1
        if (entries.size() != static_cast<size_t>(latest + 1)) {
            throw InvariantViolation(
                "contiguous_versions",
                "expected " + std::to_string(latest + 1) + " entries, got " +
                std::to_string(entries.size())
            );
        }

        // Verify each entry's version matches its position
        for (size_t i = 0; i < entries.size(); ++i) {
            if (entries[i].version != static_cast<uint64_t>(i)) {
                throw InvariantViolation(
                    "contiguous_versions",
                    "entry at position " + std::to_string(i) +
                    " has version " + std::to_string(entries[i].version)
                );
            }
        }
    } catch (const InvariantViolation&) {
        throw;  // already an invariant violation — pass through
    } catch (const LogError& e) {
        throw InvariantViolation("contiguous_versions", e.what());
    }
}

// ---------------------------------------------------------------------------
// Invariant 2: NO DUPLICATE VERSIONS
//
// Scan metadata/versions/ and collect all version numbers from filenames.
// Every version number must appear exactly once.
// ---------------------------------------------------------------------------

void assert_no_duplicate_versions(const fs::path& table_path) {
    fs::path log_dir = table_path / "metadata" / "versions";
    if (!fs::exists(log_dir)) return;

    std::map<uint64_t, int> version_count;

    for (const auto& entry : fs::directory_iterator(log_dir)) {
        std::string filename = entry.path().filename().string();

        // Skip .tmp files
        if (filename.size() > 9 &&
            filename.substr(filename.size() - 9) == ".json.tmp") {
            continue;
        }

        const std::string suffix = ".json";
        if (filename.size() <= suffix.size()) continue;
        if (filename.substr(filename.size() - suffix.size()) != suffix) continue;

        std::string stem = filename.substr(0, filename.size() - suffix.size());
        try {
            uint64_t v = std::stoull(stem);
            version_count[v]++;
        } catch (...) {
            // Not a version file — skip
        }
    }

    for (const auto& [version, count] : version_count) {
        if (count > 1) {
            throw InvariantViolation(
                "no_duplicate_versions",
                "version " + std::to_string(version) +
                " appears " + std::to_string(count) + " times"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Invariant 3: DETERMINISTIC SNAPSHOT
//
// Call build_snapshot(V) twice and compare.  If the results differ, the
// snapshot computation depends on some non-deterministic factor (read order,
// timing, etc.) and is broken.
// ---------------------------------------------------------------------------

void assert_deterministic_snapshot(const fs::path& table_path, uint64_t version) {
    auto snap1 = build_snapshot(table_path, version);
    auto snap2 = build_snapshot(table_path, version);

    if (snap1 != snap2) {
        throw InvariantViolation(
            "deterministic_snapshot",
            "build_snapshot(" + std::to_string(version) +
            ") returned different results on two consecutive calls"
        );
    }
}

// ---------------------------------------------------------------------------
// Invariant 4: NO DUPLICATE FILES IN SNAPSHOT
//
// Two sub-checks:
//   a) No single log entry adds the same filename twice in its adds list.
//   b) No log entry adds a file that is already active (re-add without
//      an intervening remove is suspicious, though technically the set
//      would deduplicate it — we still flag it as a log inconsistency).
// ---------------------------------------------------------------------------

void assert_no_duplicate_files(const fs::path& table_path, uint64_t version) {
    auto entries = read_logs(table_path, version);

    // Check (a): no duplicates within a single entry's adds list
    for (const auto& entry : entries) {
        std::set<std::string> seen;
        for (const auto& f : entry.adds) {
            if (!seen.insert(f).second) {
                throw InvariantViolation(
                    "no_duplicate_files",
                    "version " + std::to_string(entry.version) +
                    " adds '" + f + "' more than once in the same entry"
                );
            }
        }
    }

    // Check (b): replay the log and verify no add targets an already-active file
    std::set<std::string> active;
    for (const auto& entry : entries) {
        for (const auto& f : entry.adds) {
            if (active.count(f)) {
                throw InvariantViolation(
                    "no_duplicate_files",
                    "version " + std::to_string(entry.version) +
                    " adds '" + f + "' which is already active (added without prior remove)"
                );
            }
            active.insert(f);
        }
        for (const auto& f : entry.removes) {
            active.erase(f);
        }
    }
}

// ---------------------------------------------------------------------------
// Invariant 5: NO GHOST REMOVES
//
// Replay the log and verify that every remove targets a file that is
// currently in the active set.  Removing a file that doesn't exist means
// the log is logically inconsistent — the writer removed something that
// was never added or was already removed.
// ---------------------------------------------------------------------------

void assert_no_ghost_removes(const fs::path& table_path, uint64_t version) {
    auto entries = read_logs(table_path, version);

    std::set<std::string> active;

    for (const auto& entry : entries) {
        // Process adds first (within a single version, adds come before removes)
        for (const auto& f : entry.adds) {
            active.insert(f);
        }
        // Then check removes
        for (const auto& f : entry.removes) {
            if (!active.count(f)) {
                throw InvariantViolation(
                    "no_ghost_removes",
                    "version " + std::to_string(entry.version) +
                    " removes '" + f + "' which is not currently active"
                );
            }
            active.erase(f);
        }
    }
}

// ---------------------------------------------------------------------------
// assert_all_invariants — the comprehensive health check
//
// Runs every invariant check on the full log.  If the table is empty,
// most checks are trivially satisfied.
// ---------------------------------------------------------------------------

void assert_all_invariants(const fs::path& table_path) {
    // Invariant 1: contiguous versions
    assert_contiguous_versions(table_path);

    // Invariant 2: no duplicate versions
    assert_no_duplicate_versions(table_path);

    // For invariants 3-5, we need the latest version
    int64_t latest = latest_version(table_path);
    if (latest < 0) return;  // empty table — nothing more to check

    uint64_t latest_u = static_cast<uint64_t>(latest);

    // Invariant 3: deterministic snapshot (check at every version)
    for (uint64_t v = 0; v <= latest_u; ++v) {
        assert_deterministic_snapshot(table_path, v);
    }

    // Invariant 4: no duplicate files (check at latest — it covers all entries)
    assert_no_duplicate_files(table_path, latest_u);

    // Invariant 5: no ghost removes (check at latest — it covers all entries)
    assert_no_ghost_removes(table_path, latest_u);
}

} // namespace snapshotdb
