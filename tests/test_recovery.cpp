// ---------------------------------------------------------------------------
// test_recovery.cpp — Week 6 crash recovery and validation tests
//
// WHAT WE'RE TESTING:
//
//   1. Clean log → recovery is a no-op (was_clean == true).
//   2. Leftover .tmp files → cleaned up, log intact.
//   3. Corrupted tail (last log file has bad JSON) → truncated.
//   4. Gap in version chain → everything above the gap is truncated.
//   5. Version mismatch (file content disagrees with filename) → truncated.
//   6. validate_log() is read-only — doesn't modify anything.
//   7. After recovery, build_snapshot works correctly on the repaired log.
//   8. Crash at various points → recover → snapshot is always valid.
//   9. Repeated restart cycles → invariants hold every time.
//
// THE GUARANTEE:
//   After recover_log(), the log is a valid contiguous chain.
//   build_snapshot() on the recovered log NEVER returns inconsistent data.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>

#include "snapshotdb/recovery.h"
#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/transaction.h"
#include "snapshotdb/atomic_writer.h"
#include "snapshotdb/errors.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class RecoveryTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_recovery_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }
};

static std::set<std::string> make_set(std::initializer_list<std::string> vals) {
    return std::set<std::string>(vals);
}

// ---------------------------------------------------------------------------
// CLEAN LOG — recovery should detect no issues and do nothing.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, CleanLogIsNoop) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});

    auto result = recover_log(table);

    EXPECT_TRUE(result.was_clean);
    EXPECT_EQ(result.valid_version, 1);
    EXPECT_TRUE(result.cleaned_tmp_files.empty());
    EXPECT_TRUE(result.truncated_versions.empty());
}

// ---------------------------------------------------------------------------
// LEFTOVER .TMP FILES — cleaned up, log intact.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, CleansUpTmpFiles) {
    write_log(table, 0, {"a.parquet"}, {});

    // Plant a leftover .tmp file (simulating a crashed atomic write)
    std::ofstream(table / "metadata" / "versions" / "1.json.tmp") << R"({"version":1})";

    auto result = recover_log(table);

    EXPECT_FALSE(result.was_clean);
    EXPECT_EQ(result.valid_version, 0);
    EXPECT_EQ(result.cleaned_tmp_files.size(), 1u);
    EXPECT_TRUE(result.truncated_versions.empty());

    // The .tmp file is gone
    EXPECT_FALSE(fs::exists(table / "metadata" / "versions" / "1.json.tmp"));

    // The valid log is intact
    auto snap = build_snapshot(table, 0);
    EXPECT_EQ(snap, make_set({"a.parquet"}));
}

// ---------------------------------------------------------------------------
// CORRUPTED TAIL — last log file has invalid JSON → truncated.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, CorruptedTailTruncated) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Corrupt version 2
    std::ofstream(table / "metadata" / "versions" / "2.json") << "NOT VALID JSON {{{";

    auto result = recover_log(table);

    EXPECT_FALSE(result.was_clean);
    EXPECT_EQ(result.valid_version, 1);
    EXPECT_EQ(result.truncated_versions.size(), 1u);
    EXPECT_EQ(result.truncated_versions[0], 2u);

    // Version 2 is gone
    EXPECT_FALSE(fs::exists(table / "metadata" / "versions" / "2.json"));

    // Snapshot at v1 works correctly
    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "b.parquet"}));
}

// ---------------------------------------------------------------------------
// GAP IN CHAIN — version 1 is missing → version 2 and above are truncated.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, GapInChainTruncatesAbove) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Delete version 1, creating a gap: 0, _, 2
    fs::remove(table / "metadata" / "versions" / "1.json");

    auto result = recover_log(table);

    EXPECT_FALSE(result.was_clean);
    EXPECT_EQ(result.valid_version, 0);
    // Version 2 should be truncated (it's above the gap)
    EXPECT_TRUE(std::find(result.truncated_versions.begin(),
                          result.truncated_versions.end(), 2u)
                != result.truncated_versions.end());

    // Only version 0 survives
    EXPECT_EQ(latest_version(table), 0);
    auto snap = build_snapshot(table, 0);
    EXPECT_EQ(snap, make_set({"a.parquet"}));
}

// ---------------------------------------------------------------------------
// VERSION MISMATCH — file 1.json contains {"version": 99, ...}
// The content disagrees with the filename → truncated.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, VersionMismatchTruncated) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});

    // Overwrite version 1 with a mismatched version number
    nlohmann::json bad;
    bad["version"] = 99;
    bad["timestamp"] = 0;
    bad["adds"] = nlohmann::json::array({"x.parquet"});
    bad["removes"] = nlohmann::json::array();
    std::ofstream(table / "metadata" / "versions" / "1.json") << bad.dump();

    auto result = recover_log(table);

    EXPECT_FALSE(result.was_clean);
    EXPECT_EQ(result.valid_version, 0);
    EXPECT_EQ(result.truncated_versions.size(), 1u);
}

// ---------------------------------------------------------------------------
// VALIDATE_LOG — read-only: reports issues but doesn't fix them.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, ValidateLogReadOnly) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Corrupt version 2
    std::ofstream(table / "metadata" / "versions" / "2.json") << "GARBAGE";

    // validate_log reports the issue
    int64_t valid = validate_log(table);
    EXPECT_EQ(valid, 1);

    // But the corrupt file is still there (read-only check)
    EXPECT_TRUE(fs::exists(table / "metadata" / "versions" / "2.json"));
}

// ---------------------------------------------------------------------------
// EMPTY LOG — recovery returns valid_version -1, no errors.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, EmptyLogRecovery) {
    auto result = recover_log(table);

    EXPECT_TRUE(result.was_clean);
    EXPECT_EQ(result.valid_version, -1);
}

// ---------------------------------------------------------------------------
// SNAPSHOT VALID AFTER RECOVERY:
//   Corrupt the tail, recover, then verify snapshot correctness.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, SnapshotValidAfterRecovery) {
    write_log(table, 0, {"a.parquet", "b.parquet"}, {});
    write_log(table, 1, {"c.parquet"}, {"b.parquet"});
    write_log(table, 2, {"d.parquet"}, {});

    // Corrupt version 2
    std::ofstream(table / "metadata" / "versions" / "2.json") << "";

    // Recover — version 2 is truncated
    auto result = recover_log(table);
    EXPECT_EQ(result.valid_version, 1);

    // Snapshot at v1 must be valid and correct
    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "c.parquet"}));
}

// ---------------------------------------------------------------------------
// CRASH BEFORE LOG WRITE + RECOVERY:
//   Use the crash_commit binary to crash after data write but before log.
//   Then recover.  The commit must not exist.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, CrashBeforeLogWriteRecovery) {
    write_log(table, 0, {"a.parquet"}, {});

    // Simulate: someone wrote to data/staging/ but never got to the log write
    // (this is what happens if the crash is between phase 1 and phase 3)
    fs::create_directories(table / "data" / "staging");
    std::ofstream(table / "data" / "staging" / "b.parquet").close();

    auto result = recover_log(table);

    // Log is fine — only version 0 exists
    EXPECT_EQ(result.valid_version, 0);

    // No trace of the incomplete commit in the snapshot
    auto snap = build_snapshot(table, 0);
    EXPECT_EQ(snap, make_set({"a.parquet"}));
}

// ---------------------------------------------------------------------------
// REPEATED RESTART CYCLES:
//   Corrupt the log, recover, add more commits, corrupt again, recover.
//   Invariant: after every recovery, the log is valid and the snapshot
//   is consistent.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, RepeatedRestartCycles) {
    // Cycle 1: create some data
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});

    // Corrupt and recover
    std::ofstream(table / "metadata" / "versions" / "1.json") << "CORRUPT";
    auto r1 = recover_log(table);
    EXPECT_EQ(r1.valid_version, 0);

    // Cycle 2: add more commits on top of the recovered state
    write_log(table, 1, {"c.parquet"}, {});
    write_log(table, 2, {"d.parquet"}, {});

    // Corrupt and recover again
    std::ofstream(table / "metadata" / "versions" / "2.json") << "";
    auto r2 = recover_log(table);
    EXPECT_EQ(r2.valid_version, 1);

    // Snapshot must always be valid
    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "c.parquet"}));

    // Cycle 3: clean recovery (no corruption)
    write_log(table, 2, {"e.parquet"}, {});
    auto r3 = recover_log(table);
    EXPECT_TRUE(r3.was_clean);
    EXPECT_EQ(r3.valid_version, 2);
}

// ---------------------------------------------------------------------------
// MULTIPLE CORRUPTIONS AT ONCE:
//   Versions 3, 4, 5 are all corrupted.  Recovery should truncate all of
//   them and report the last valid version as 2.
// ---------------------------------------------------------------------------

TEST_F(RecoveryTest, MultipleTailCorruptions) {
    for (uint64_t v = 0; v <= 5; ++v) {
        write_log(table, v, {"file_" + std::to_string(v) + ".parquet"}, {});
    }

    // Corrupt versions 3, 4, 5
    std::ofstream(table / "metadata" / "versions" / "3.json") << "BAD";
    std::ofstream(table / "metadata" / "versions" / "4.json") << "BAD";
    std::ofstream(table / "metadata" / "versions" / "5.json") << "BAD";

    auto result = recover_log(table);

    EXPECT_EQ(result.valid_version, 2);
    EXPECT_EQ(result.truncated_versions.size(), 3u);

    // Snapshot at v2 works
    auto snap = build_snapshot(table, 2);
    EXPECT_TRUE(snap.count("file_0.parquet"));
    EXPECT_TRUE(snap.count("file_1.parquet"));
    EXPECT_TRUE(snap.count("file_2.parquet"));
    EXPECT_FALSE(snap.count("file_3.parquet"));
}

// ---------------------------------------------------------------------------
// CRASH AT DIFFERENT POINTS + RECOVERY (integration with crash_commit):
//
// We use the crash_commit binary to inject crashes, then recover.
// ---------------------------------------------------------------------------

class CrashRecoveryTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_crashrec_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }

    int run_crash_commit(const std::string& crash_point,
                         const std::vector<std::string>& add_files) {
        std::ostringstream cmd;
        cmd << "./crash_commit"
            << " \"" << table.string() << "\""
            << " " << crash_point;
        for (const auto& f : add_files) {
            cmd << " " << f;
        }
        cmd << " 2>/dev/null";
        return std::system(cmd.str().c_str());
    }
};

// Crash after fsync (before rename) → recover → commit never happened
TEST_F(CrashRecoveryTest, CrashBeforeRenameRecoverClean) {
    write_log(table, 0, {"a.parquet"}, {});

    run_crash_commit("after_fsync", {"b.parquet"});

    // There might be a .tmp file — recovery cleans it up
    auto result = recover_log(table);

    EXPECT_EQ(result.valid_version, 0);

    auto snap = build_snapshot(table, 0);
    EXPECT_EQ(snap, make_set({"a.parquet"}));
    EXPECT_FALSE(snap.count("b.parquet"));
}

// Crash after rename → recover → commit IS durable
TEST_F(CrashRecoveryTest, CrashAfterRenameRecoverDurable) {
    write_log(table, 0, {"a.parquet"}, {});

    run_crash_commit("after_rename", {"b.parquet"});

    auto result = recover_log(table);

    // The commit survived the crash
    EXPECT_EQ(result.valid_version, 1);

    auto snap = build_snapshot(table, 1);
    EXPECT_TRUE(snap.count("a.parquet"));
    EXPECT_TRUE(snap.count("b.parquet"));
}

// Crash + recover + commit again → the chain continues correctly
TEST_F(CrashRecoveryTest, RecoverThenCommitAgain) {
    write_log(table, 0, {"a.parquet"}, {});

    // Crash before rename — commit doesn't exist
    run_crash_commit("after_fsync", {"b.parquet"});
    recover_log(table);

    // Now commit normally — should get version 1
    {
        Transaction txn(table);
        txn.add("c.parquet");
        uint64_t v = txn.commit();
        EXPECT_EQ(v, 1u);
    }

    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "c.parquet"}));
}
