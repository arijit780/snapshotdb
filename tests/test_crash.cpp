// ---------------------------------------------------------------------------
// test_crash.cpp — Week 2 crash-injection tests
//
// These tests verify the fundamental guarantee of the atomic commit protocol:
//
//   • Crash BEFORE rename  → commit does NOT exist (invisible)
//   • Crash AFTER  rename  → commit DOES exist (durable)
//
// To test this safely, we spawn the `crash_commit` helper binary as a child
// process.  That binary calls _exit(1) at the requested crash point, which
// terminates only the child — the test runner stays alive.
//
// We then inspect the table directory from the parent to verify correctness.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <cstdlib>
#include <string>
#include <array>
#include <memory>
#include <sstream>

#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/atomic_writer.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class CrashTest : public ::testing::Test {
protected:
    fs::path table;
    std::string crash_commit_bin;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_crash_" + std::to_string(std::rand()));
        fs::create_directories(table);

        // The crash_commit binary is built alongside the tests by CMake.
        // It lives in the same directory as the test binary.
        // We find it relative to the current test executable.
        crash_commit_bin = find_crash_commit_binary();
    }

    void TearDown() override {
        fs::remove_all(table);
    }

    // Run the crash_commit helper and return its exit code.
    int run_crash_commit(const std::string& crash_point,
                         const std::vector<std::string>& add_files) {
        std::ostringstream cmd;
        cmd << "\"" << crash_commit_bin << "\""
            << " \"" << table.string() << "\""
            << " " << crash_point;
        for (const auto& f : add_files) {
            cmd << " " << f;
        }
        cmd << " 2>/dev/null";  // suppress stderr from the crashing process
        return std::system(cmd.str().c_str());
    }

private:
    // Locate the crash_commit binary.  CMake places it next to the test binary.
    static std::string find_crash_commit_binary() {
        // Try common CMake build output locations
        std::vector<std::string> candidates = {
            "./crash_commit",
            "../crash_commit",
        };

        for (const auto& c : candidates) {
            if (fs::exists(c)) return fs::canonical(c).string();
        }

        // Fallback: assume it's on PATH
        return "crash_commit";
    }
};

// ---------------------------------------------------------------------------
// Crash BEFORE rename → version must NOT exist
//
// Scenario:
//   1. Write version 0 normally (the "existing" state)
//   2. Attempt a commit that crashes after fsync but before rename
//   3. Verify: latest_version is still 0, snapshot is unchanged
// ---------------------------------------------------------------------------

TEST_F(CrashTest, CrashBeforeRenameCommitDoesNotExist) {
    write_log(table, 0, {"existing.parquet"}, {});

    int exit_code = run_crash_commit("after_fsync", {"new.parquet"});
    // Non-zero exit expected (the child called _exit(1))
    EXPECT_NE(exit_code, 0);

    // The attempted commit must be invisible
    EXPECT_EQ(latest_version(table), 0);

    auto snap = build_snapshot(table, 0);
    EXPECT_TRUE(snap.count("existing.parquet"));
    EXPECT_FALSE(snap.count("new.parquet"));
}

// ---------------------------------------------------------------------------
// Crash AFTER rename → version MUST exist
//
// Scenario:
//   1. Write version 0 normally
//   2. Commit crashes after rename (the log file was already created)
//   3. Verify: latest_version is 1, snapshot includes the new file
// ---------------------------------------------------------------------------

TEST_F(CrashTest, CrashAfterRenameCommitExists) {
    write_log(table, 0, {"existing.parquet"}, {});

    int exit_code = run_crash_commit("after_rename", {"new.parquet"});
    EXPECT_NE(exit_code, 0);

    // The commit IS durable — rename already happened
    EXPECT_EQ(latest_version(table), 1);

    auto snap = build_snapshot(table, 1);
    EXPECT_TRUE(snap.count("existing.parquet"));
    EXPECT_TRUE(snap.count("new.parquet"));
}

// ---------------------------------------------------------------------------
// After a pre-rename crash, read_logs sees no trace of the aborted commit
// ---------------------------------------------------------------------------

TEST_F(CrashTest, NoPartialCommitsVisibleAfterCrash) {
    write_log(table, 0, {"a.parquet"}, {});

    run_crash_commit("after_fsync", {"b.parquet", "c.parquet"});

    // Only version 0 should be visible
    auto entries = read_logs(table, 0);
    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].version, 0u);
}

// ---------------------------------------------------------------------------
// cleanup_tmp_files removes stale .tmp leftovers from a crashed commit
// ---------------------------------------------------------------------------

TEST_F(CrashTest, TmpCleanupAfterCrash) {
    write_log(table, 0, {"a.parquet"}, {});

    run_crash_commit("after_fsync", {"b.parquet"});

    fs::path tmp_file = table / "metadata" / "versions" / "1.json.tmp";
    bool tmp_existed = fs::exists(tmp_file);

    // Clean up as a real system would on startup
    cleanup_tmp_files(table);

    if (tmp_existed) {
        EXPECT_FALSE(fs::exists(tmp_file));
    }

    // System remains consistent
    EXPECT_EQ(latest_version(table), 0);
}

// ---------------------------------------------------------------------------
// Normal commit via the binary (no crash) — sanity check
// ---------------------------------------------------------------------------

TEST_F(CrashTest, NormalCommitViaBinary) {
    write_log(table, 0, {"a.parquet"}, {});

    int exit_code = run_crash_commit("none", {"b.parquet"});
    EXPECT_EQ(exit_code, 0);

    EXPECT_EQ(latest_version(table), 1);
    auto snap = build_snapshot(table, 1);
    EXPECT_TRUE(snap.count("a.parquet"));
    EXPECT_TRUE(snap.count("b.parquet"));
}
