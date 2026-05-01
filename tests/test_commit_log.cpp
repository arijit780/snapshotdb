// ---------------------------------------------------------------------------
// test_commit_log.cpp — Week 1 tests for the log engine
//
// Each test creates an isolated temporary directory so tests never interfere
// with each other.  We verify:
//   • Round-trip write → read
//   • Contiguity enforcement (no gaps)
//   • Duplicate rejection
//   • Missing-file detection on read
//   • Corrupted-JSON detection
//   • latest_version scanning (including .tmp file ignorance)
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>

#include "snapshotdb/commit_log.h"
#include "snapshotdb/errors.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

// Helper: create a unique temp directory that is cleaned up after the test.
class CommitLogTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_test_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }
};

// --- Basic round-trip: write version 0, read it back, fields must match ---

TEST_F(CommitLogTest, WriteAndReadSingleLog) {
    write_log(table, 0, {"a.parquet"}, {});
    auto entries = read_logs(table, 0);

    ASSERT_EQ(entries.size(), 1u);
    EXPECT_EQ(entries[0].version, 0u);
    EXPECT_EQ(entries[0].adds, std::vector<std::string>{"a.parquet"});
    EXPECT_TRUE(entries[0].removes.empty());
}

// --- Writing v2 without v1 must fail (contiguity) ---

TEST_F(CommitLogTest, ContiguousVersionsEnforced) {
    write_log(table, 0, {"a.parquet"}, {});

    // Skip version 1 → should throw MissingVersionError for version 0's successor
    EXPECT_THROW(
        write_log(table, 2, {"c.parquet"}, {}),
        MissingVersionError
    );

    // Writing version 1 normally succeeds
    EXPECT_NO_THROW(write_log(table, 1, {"b.parquet"}, {}));
}

// --- Writing the same version twice must fail ---

TEST_F(CommitLogTest, DuplicateVersionRejected) {
    write_log(table, 0, {"a.parquet"}, {});

    EXPECT_THROW(
        write_log(table, 0, {"b.parquet"}, {}),
        DuplicateVersionError
    );
}

// --- Deleting a mid-chain file causes read_logs to fail ---

TEST_F(CommitLogTest, ReadMissingVersionMidSequence) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Remove version 1 from disk
    fs::remove(table / "metadata" / "versions" / "1.json");

    EXPECT_THROW(read_logs(table, 2), MissingVersionError);
}

// --- Overwriting a log file with garbage triggers CorruptedJsonError ---

TEST_F(CommitLogTest, CorruptedJsonErrors) {
    write_log(table, 0, {"a.parquet"}, {});

    // Corrupt the file with invalid JSON
    std::ofstream(table / "metadata" / "versions" / "0.json") << "NOT VALID JSON {{{";

    EXPECT_THROW(read_logs(table, 0), CorruptedJsonError);
}

// --- Empty table returns -1 for latest_version ---

TEST_F(CommitLogTest, LatestVersionEmptyTable) {
    EXPECT_EQ(latest_version(table), -1);
}

// --- latest_version returns the highest committed version ---

TEST_F(CommitLogTest, LatestVersionWithLogs) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    EXPECT_EQ(latest_version(table), 2);
}

// --- .tmp files must be invisible to latest_version ---

TEST_F(CommitLogTest, LatestVersionIgnoresTmpFiles) {
    write_log(table, 0, {"a.parquet"}, {});

    // Plant a stale .tmp file — simulates an incomplete atomic write
    std::ofstream(table / "metadata" / "versions" / "1.json.tmp") << "{}";

    EXPECT_EQ(latest_version(table), 0);
}
