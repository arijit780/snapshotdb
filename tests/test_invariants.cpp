// ---------------------------------------------------------------------------
// test_invariants.cpp — Tests for the invariant checker itself
//
// These tests verify that the checker CATCHES violations.  A checker that
// never fires is useless — we need to prove it actually detects broken state.
//
// For each invariant, we:
//   1. Build a valid table → assert_all_invariants passes.
//   2. Manually corrupt the table in a way that violates exactly one
//      invariant → the specific assertion fires.
//
// We also verify that assert_all_invariants passes on a clean table after
// commits, recoveries, and vacuums — the checker is safe to call anywhere.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "snapshotdb/invariant_checker.h"
#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/transaction.h"
#include "snapshotdb/recovery.h"
#include "snapshotdb/gc.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class InvariantTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_inv_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }
};

// ---------------------------------------------------------------------------
// CLEAN TABLE — all invariants hold after normal operations
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, CleanTablePassesAllInvariants) {
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.add("b.parquet");
        txn.commit();
    }
    {
        Transaction txn(table);
        txn.add("c.parquet");
        txn.remove("b.parquet");
        txn.commit();
    }
    {
        Transaction txn(table);
        txn.add("d.parquet");
        txn.commit();
    }

    EXPECT_NO_THROW(assert_all_invariants(table));
}

// ---------------------------------------------------------------------------
// EMPTY TABLE — trivially passes
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, EmptyTablePasses) {
    EXPECT_NO_THROW(assert_all_invariants(table));
}

// ---------------------------------------------------------------------------
// INVARIANT 1: Contiguous versions — delete a mid-chain file to create a gap
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, DetectsNonContiguousVersions) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Create a gap by deleting version 1
    fs::remove(table / "metadata" / "versions" / "1.json");

    EXPECT_THROW(assert_contiguous_versions(table), InvariantViolation);
}

// ---------------------------------------------------------------------------
// INVARIANT 2: No duplicate versions — would require filesystem-level
// trickery (two files with the same version).  We test the checker with a
// well-formed table to ensure it doesn't false-positive.
// (In practice, duplicates are prevented by write_log at write time.)
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, NoDuplicateVersionsOnCleanTable) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});

    EXPECT_NO_THROW(assert_no_duplicate_versions(table));
}

// ---------------------------------------------------------------------------
// INVARIANT 3: Deterministic snapshot — this should always pass for correct
// code, but we verify it runs without error.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, DeterministicSnapshotPasses) {
    write_log(table, 0, {"a.parquet", "b.parquet"}, {});
    write_log(table, 1, {"c.parquet"}, {"b.parquet"});

    EXPECT_NO_THROW(assert_deterministic_snapshot(table, 0));
    EXPECT_NO_THROW(assert_deterministic_snapshot(table, 1));
}

// ---------------------------------------------------------------------------
// INVARIANT 4: No duplicate files — manually write a log entry that adds
// the same file twice in one entry.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, DetectsDuplicateAddsInSingleEntry) {
    // Write a log entry that adds "x.parquet" twice
    ensure_dirs(table);
    nlohmann::json j;
    j["version"] = 0;
    j["timestamp"] = 0;
    j["adds"] = {"x.parquet", "x.parquet"};  // DUPLICATE!
    j["removes"] = nlohmann::json::array();
    std::ofstream(table / "metadata" / "versions" / "0.json") << j.dump();

    EXPECT_THROW(assert_no_duplicate_files(table, 0), InvariantViolation);
}

// ---------------------------------------------------------------------------
// INVARIANT 4b: No re-add without intervening remove — manually write a
// log that adds "x.parquet" in v0 and again in v1 without removing it.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, DetectsReAddWithoutRemove) {
    write_log(table, 0, {"x.parquet"}, {});

    // Manually write v1 that re-adds x.parquet without removing it first
    ensure_dirs(table);
    nlohmann::json j;
    j["version"] = 1;
    j["timestamp"] = 0;
    j["adds"] = {"x.parquet"};  // x is already active — violation!
    j["removes"] = nlohmann::json::array();
    std::ofstream(table / "metadata" / "versions" / "1.json") << j.dump();

    EXPECT_THROW(assert_no_duplicate_files(table, 1), InvariantViolation);
}

// ---------------------------------------------------------------------------
// INVARIANT 5: No ghost removes — manually write a log entry that removes
// a file that was never added.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, DetectsGhostRemove) {
    write_log(table, 0, {"a.parquet"}, {});

    // v1 removes "phantom.parquet" which was never added
    ensure_dirs(table);
    nlohmann::json j;
    j["version"] = 1;
    j["timestamp"] = 0;
    j["adds"] = nlohmann::json::array();
    j["removes"] = {"phantom.parquet"};  // GHOST REMOVE!
    std::ofstream(table / "metadata" / "versions" / "1.json") << j.dump();

    EXPECT_THROW(assert_no_ghost_removes(table, 1), InvariantViolation);
}

// ---------------------------------------------------------------------------
// INVARIANT 5b: Removing an already-removed file is also a ghost remove.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, DetectsDoubleRemove) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {}, {"a.parquet"});

    // v2 removes a.parquet AGAIN — it's already gone
    ensure_dirs(table);
    nlohmann::json j;
    j["version"] = 2;
    j["timestamp"] = 0;
    j["adds"] = nlohmann::json::array();
    j["removes"] = {"a.parquet"};  // already removed in v1!
    std::ofstream(table / "metadata" / "versions" / "2.json") << j.dump();

    EXPECT_THROW(assert_no_ghost_removes(table, 2), InvariantViolation);
}

// ---------------------------------------------------------------------------
// ALL INVARIANTS AFTER RECOVERY:
//   Corrupt the log, recover, then verify all invariants hold.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, AllInvariantsAfterRecovery) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Corrupt the tail
    std::ofstream(table / "metadata" / "versions" / "2.json") << "CORRUPT";

    // Recover (truncates v2)
    recover_log(table);

    // All invariants must hold on the recovered log
    EXPECT_NO_THROW(assert_all_invariants(table));
}

// ---------------------------------------------------------------------------
// ALL INVARIANTS AFTER VACUUM:
//   Commit several versions, vacuum, then verify invariants.
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, AllInvariantsAfterVacuum) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {"a.parquet"});
    write_log(table, 2, {"c.parquet"}, {});

    // Create data files
    fs::create_directories(table / "data");
    std::ofstream(table / "data" / "a.parquet").close();
    std::ofstream(table / "data" / "b.parquet").close();
    std::ofstream(table / "data" / "c.parquet").close();

    vacuum(table, 2);

    EXPECT_NO_THROW(assert_all_invariants(table));
}

// ---------------------------------------------------------------------------
// INVARIANT CHECK AFTER EVERY COMMIT — integration stress test
// ---------------------------------------------------------------------------

TEST_F(InvariantTest, InvariantsHoldAfterEveryCommit) {
    for (int i = 0; i < 10; ++i) {
        Transaction txn(table);
        txn.add("file_" + std::to_string(i) + ".parquet");
        if (i > 0) {
            txn.remove("file_" + std::to_string(i - 1) + ".parquet");
        }
        txn.commit();

        EXPECT_NO_THROW(assert_all_invariants(table));
    }
}
