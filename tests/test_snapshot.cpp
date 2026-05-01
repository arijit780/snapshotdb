// ---------------------------------------------------------------------------
// test_snapshot.cpp — Week 1 tests for snapshot reconstruction
//
// build_snapshot replays the log and returns the set of active files.
// We verify:
//   • Adds accumulate correctly
//   • Removes take effect
//   • Remove-then-readd restores the file
//   • Empty log → empty snapshot
//   • Intermediate version doesn't include later additions
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <set>
#include <string>

#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class SnapshotTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_snap_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }
};

// Helper to build an expected set from an initializer list
static std::set<std::string> make_set(std::initializer_list<std::string> vals) {
    return std::set<std::string>(vals);
}

// --- Three versions of pure adds → snapshot is the union ---

TEST_F(SnapshotTest, AddsOnly) {
    write_log(table, 0, {"a.parquet", "b.parquet"}, {});
    write_log(table, 1, {"c.parquet"}, {});
    write_log(table, 2, {"d.parquet"}, {});

    auto snap = build_snapshot(table, 2);
    EXPECT_EQ(snap, make_set({"a.parquet", "b.parquet", "c.parquet", "d.parquet"}));
}

// --- Remove a file in v1 → it disappears from the snapshot ---

TEST_F(SnapshotTest, AddsAndRemoves) {
    write_log(table, 0, {"a.parquet", "b.parquet", "c.parquet"}, {});
    write_log(table, 1, {}, {"b.parquet"});

    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "c.parquet"}));
}

// --- Remove in v1, re-add in v2 → file is active again ---

TEST_F(SnapshotTest, RemoveThenReadd) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {}, {"a.parquet"});
    write_log(table, 2, {"a.parquet"}, {});

    // After remove: gone
    auto snap1 = build_snapshot(table, 1);
    EXPECT_TRUE(snap1.empty());

    // After re-add: back
    auto snap2 = build_snapshot(table, 2);
    EXPECT_EQ(snap2, make_set({"a.parquet"}));
}

// --- Version 0 with no adds/removes → empty snapshot ---

TEST_F(SnapshotTest, EmptySnapshot) {
    write_log(table, 0, {}, {});

    auto snap = build_snapshot(table, 0);
    EXPECT_TRUE(snap.empty());
}

// --- Snapshot at v1 must not include v2's additions ---

TEST_F(SnapshotTest, IntermediateVersion) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "b.parquet"}));
}
