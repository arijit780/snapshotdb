// ---------------------------------------------------------------------------
// test_gc.cpp — Week 5 garbage collection and mutation tests
//
// WHAT WE'RE TESTING:
//
//   1. Overwrite pattern:
//      - Identify files in a partition, remove them, add replacements.
//      - The snapshot must reflect the overwrite, not a union of old+new.
//
//   2. Safe vacuum:
//      - vacuum(retain_last_n) deletes orphaned data files.
//      - Files still visible to ANY retained snapshot are NEVER deleted.
//
//   3. Time-travel safety:
//      - A file added in v0 and removed in v2 must still be accessible
//        to a reader pinned at v0 or v1, even after vacuum retains [v0..latest].
//
//   4. No data loss:
//      - After vacuum, all retained snapshots still reconstruct correctly.
//
// THE CRITICAL BUG THIS PREVENTS:
//   Deleting a file because it's not in the LATEST snapshot, while a reader
//   at an older version still needs it.  vacuum() computes the UNION of all
//   retained snapshots specifically to prevent this.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>

#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/transaction.h"
#include "snapshotdb/gc.h"
#include "snapshotdb/reader.h"
#include "snapshotdb/errors.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class GCTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_gc_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }
};

static std::set<std::string> make_set(std::initializer_list<std::string> vals) {
    return std::set<std::string>(vals);
}

// Helper: create a physical data file (simulating what Transaction does)
static void touch_data_file(const fs::path& table, const std::string& name) {
    fs::path p = table / "data" / name;
    fs::create_directories(p.parent_path());
    std::ofstream(p).close();
}

// ---------------------------------------------------------------------------
// OVERWRITE PATTERN:
//   v0: {old_a, old_b}
//   v1: remove old_a, old_b; add new_a, new_b  (overwrite)
//
// Snapshot at v1 must be {new_a, new_b} — no trace of old files.
// ---------------------------------------------------------------------------

TEST_F(GCTest, OverwriteCorrectness) {
    // v0: initial data
    {
        Transaction txn(table);
        txn.add("old_a.parquet");
        txn.add("old_b.parquet");
        txn.commit();
    }

    // v1: overwrite — remove old, add new
    {
        Transaction txn(table);
        txn.remove("old_a.parquet");
        txn.remove("old_b.parquet");
        txn.add("new_a.parquet");
        txn.add("new_b.parquet");
        txn.commit();
    }

    // Snapshot at v1 must contain ONLY the new files
    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"new_a.parquet", "new_b.parquet"}));

    // Snapshot at v0 must still show the old files (time-travel)
    auto snap_v0 = build_snapshot(table, 0);
    EXPECT_EQ(snap_v0, make_set({"old_a.parquet", "old_b.parquet"}));
}

// ---------------------------------------------------------------------------
// SAFE VACUUM — retain last 1 version:
//   v0: {A, B, C}
//   v1: removes B       → snap = {A, C}
//   v2: removes C, adds D → snap = {A, D}
//
// vacuum(retain_last_n=1) keeps only v2: referenced = {A, D}
// Physical files B and C should be deleted.
// ---------------------------------------------------------------------------

TEST_F(GCTest, VacuumDeletesOrphanedFiles) {
    write_log(table, 0, {"A.parquet", "B.parquet", "C.parquet"}, {});
    write_log(table, 1, {}, {"B.parquet"});
    write_log(table, 2, {"D.parquet"}, {"C.parquet"});

    // Create physical data files for all of them
    touch_data_file(table, "A.parquet");
    touch_data_file(table, "B.parquet");
    touch_data_file(table, "C.parquet");
    touch_data_file(table, "D.parquet");

    auto result = vacuum(table, 1);

    // Only v2 is retained: referenced = {A, D}
    EXPECT_EQ(result.retained_from_version, 2u);
    EXPECT_EQ(result.retained_to_version, 2u);
    EXPECT_EQ(result.referenced_files, make_set({"A.parquet", "D.parquet"}));

    // B and C should be deleted
    EXPECT_FALSE(fs::exists(table / "data" / "B.parquet"));
    EXPECT_FALSE(fs::exists(table / "data" / "C.parquet"));

    // A and D must still exist
    EXPECT_TRUE(fs::exists(table / "data" / "A.parquet"));
    EXPECT_TRUE(fs::exists(table / "data" / "D.parquet"));
}

// ---------------------------------------------------------------------------
// TIME-TRAVEL SAFETY — file removed in v2, reader at v1 needs it:
//
//   v0: {A, B}    v1: {A, B, C}    v2: {A, C} (B removed)
//
// vacuum(retain_last_n=3) keeps all three versions.
// B must NOT be deleted because it's still in v0 and v1's snapshots.
// ---------------------------------------------------------------------------

TEST_F(GCTest, VacuumRetainsFilesNeededByOlderSnapshots) {
    write_log(table, 0, {"A.parquet", "B.parquet"}, {});
    write_log(table, 1, {"C.parquet"}, {});
    write_log(table, 2, {}, {"B.parquet"});

    touch_data_file(table, "A.parquet");
    touch_data_file(table, "B.parquet");
    touch_data_file(table, "C.parquet");

    auto result = vacuum(table, 3);

    // All three versions retained: union = {A, B, C}
    EXPECT_EQ(result.referenced_files, make_set({"A.parquet", "B.parquet", "C.parquet"}));
    EXPECT_TRUE(result.deleted_files.empty());

    // B must still exist — reader at v1 needs it
    EXPECT_TRUE(fs::exists(table / "data" / "B.parquet"));
}

// ---------------------------------------------------------------------------
// READER AT OLD VERSION AFTER VACUUM:
//
//   v0: {X}     v1: {X, Y}    v2: {Y} (X removed)
//
// vacuum(retain_last_n=2) keeps v1 and v2.
// X is in v1's snapshot, so it must survive.
// Reader at v1 must see {X, Y}.
// ---------------------------------------------------------------------------

TEST_F(GCTest, ReaderAtOldVersionAfterVacuum) {
    write_log(table, 0, {"X.parquet"}, {});
    write_log(table, 1, {"Y.parquet"}, {});
    write_log(table, 2, {}, {"X.parquet"});

    touch_data_file(table, "X.parquet");
    touch_data_file(table, "Y.parquet");

    auto result = vacuum(table, 2);

    // v1 and v2 retained: union = {X, Y}
    EXPECT_EQ(result.referenced_files, make_set({"X.parquet", "Y.parquet"}));
    EXPECT_TRUE(result.deleted_files.empty());

    // Reader pinned at v1 must see both files
    SnapshotReader reader(table, 1);
    EXPECT_EQ(reader.read(), make_set({"X.parquet", "Y.parquet"}));
}

// ---------------------------------------------------------------------------
// VACUUM WITH RETAIN_LAST_N > TOTAL VERSIONS — retains everything
// ---------------------------------------------------------------------------

TEST_F(GCTest, VacuumRetainMoreThanAvailable) {
    write_log(table, 0, {"A.parquet"}, {});
    write_log(table, 1, {"B.parquet"}, {});

    touch_data_file(table, "A.parquet");
    touch_data_file(table, "B.parquet");
    touch_data_file(table, "orphan.parquet");  // not referenced by any version

    auto result = vacuum(table, 100);

    // Both versions retained
    EXPECT_EQ(result.retained_from_version, 0u);
    EXPECT_EQ(result.retained_to_version, 1u);

    // Orphan deleted, referenced files kept
    EXPECT_TRUE(fs::exists(table / "data" / "A.parquet"));
    EXPECT_TRUE(fs::exists(table / "data" / "B.parquet"));
    EXPECT_FALSE(fs::exists(table / "data" / "orphan.parquet"));
}

// ---------------------------------------------------------------------------
// VACUUM ON EMPTY TABLE — should throw
// ---------------------------------------------------------------------------

TEST_F(GCTest, VacuumEmptyTableThrows) {
    EXPECT_THROW(vacuum(table, 1), LogError);
}

// ---------------------------------------------------------------------------
// files_referenced_in_range — unit test for the helper function
// ---------------------------------------------------------------------------

TEST_F(GCTest, FilesReferencedInRange) {
    write_log(table, 0, {"A.parquet", "B.parquet"}, {});
    write_log(table, 1, {"C.parquet"}, {"B.parquet"});
    write_log(table, 2, {"D.parquet"}, {"A.parquet"});

    // Range [0, 0]: just v0 → {A, B}
    auto r0 = files_referenced_in_range(table, 0, 0);
    EXPECT_EQ(r0, make_set({"A.parquet", "B.parquet"}));

    // Range [1, 2]: v1={A,C}, v2={C,D} → union={A, C, D}
    auto r12 = files_referenced_in_range(table, 1, 2);
    EXPECT_EQ(r12, make_set({"A.parquet", "C.parquet", "D.parquet"}));

    // Range [0, 2]: all versions → {A, B, C, D}
    auto r02 = files_referenced_in_range(table, 0, 2);
    EXPECT_EQ(r02, make_set({"A.parquet", "B.parquet", "C.parquet", "D.parquet"}));
}

// ---------------------------------------------------------------------------
// NO DATA LOSS: after vacuum, all retained snapshots still work correctly.
// ---------------------------------------------------------------------------

TEST_F(GCTest, AllRetainedSnapshotsValidAfterVacuum) {
    write_log(table, 0, {"A.parquet"}, {});
    write_log(table, 1, {"B.parquet"}, {});
    write_log(table, 2, {"C.parquet"}, {"A.parquet"});
    write_log(table, 3, {"D.parquet"}, {});

    touch_data_file(table, "A.parquet");
    touch_data_file(table, "B.parquet");
    touch_data_file(table, "C.parquet");
    touch_data_file(table, "D.parquet");

    // Retain last 2 versions: v2 and v3
    vacuum(table, 2);

    // v2 snapshot: {B, C}
    EXPECT_EQ(build_snapshot(table, 2), make_set({"B.parquet", "C.parquet"}));

    // v3 snapshot: {B, C, D}
    EXPECT_EQ(build_snapshot(table, 3), make_set({"B.parquet", "C.parquet", "D.parquet"}));
}

// ---------------------------------------------------------------------------
// OVERWRITE + VACUUM INTEGRATION:
//   v0: {old}   v1: remove old, add new   vacuum(1) → old is deleted
// ---------------------------------------------------------------------------

TEST_F(GCTest, OverwriteThenVacuum) {
    {
        Transaction txn(table);
        txn.add("old.parquet");
        txn.commit();
    }
    {
        Transaction txn(table);
        txn.remove("old.parquet");
        txn.add("new.parquet");
        txn.commit();
    }

    // old.parquet might still be on disk (Transaction doesn't delete data files)
    touch_data_file(table, "old.parquet");

    auto result = vacuum(table, 1);

    // Only v1 retained: {new.parquet}
    EXPECT_FALSE(fs::exists(table / "data" / "old.parquet"));
    EXPECT_TRUE(fs::exists(table / "data" / "new.parquet"));
}
