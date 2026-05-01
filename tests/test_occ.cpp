// ---------------------------------------------------------------------------
// test_occ.cpp — Week 4 optimistic concurrency control (OCC) tests
//
// WHAT OCC IS:
//   "Optimistic" means we let transactions run without locks and only check
//   for conflicts at commit time.  If a conflict is detected, the transaction
//   is aborted — no partial writes, no silent overwrites.
//
// WHAT WE'RE TESTING:
//   Week 2 already has basic version-level conflict detection (if another
//   commit happened, your base_version is stale → ConflictError).
//
//   Week 4 adds FILE-LEVEL conflict detection:
//     If any file that this transaction READ was modified (added or removed)
//     by a commit after our base_version, that's a write-write or read-write
//     conflict → WriteConflictError.
//
//   This is stricter than "last write wins" and approximates serializability.
//
// THE SCENARIO THAT MUST FAIL:
//   T1 and T2 both start at version 3.
//   T1 reads file X, then commits (adds file Y) → version 4.
//   T2 reads file X, then tries to commit (modifies X) → MUST FAIL.
//   Because T2's read set overlaps with T1's write set.
//
// THE SCENARIO THAT MUST SUCCEED:
//   T1 writes file A.  T2 writes file B.  No overlap → both can commit
//   (sequentially, since only one version-slot exists at a time).
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <set>
#include <string>

#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/transaction.h"
#include "snapshotdb/errors.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class OCCTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_occ_" + std::to_string(std::rand()));
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
// Basic version-level conflict (inherited from Week 2):
//   T1 and T2 start at the same base.  T1 commits.  T2 must fail.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, BasicVersionConflict) {
    // Seed the table so both transactions start at base_version 0
    {
        Transaction txn(table);
        txn.add("seed.parquet");
        txn.commit();
    }

    // T1 and T2 both start at version 0
    Transaction t1(table);
    Transaction t2(table);

    t1.add("t1_file.parquet");
    t2.add("t2_file.parquet");

    // T1 commits successfully → version 1
    t1.commit();

    // T2's base_version (0) != latest_version (1) → ConflictError
    EXPECT_THROW(t2.commit(), ConflictError);
}

// ---------------------------------------------------------------------------
// File-level read-write conflict (Week 4 NEW):
//
//   v0: {data.parquet}
//   T1: reads data.parquet, writes new_data.parquet → commits v1
//   T2: track_read(data.parquet), tries to remove data.parquet
//   T2 must fail because data.parquet was in T1's write set (T1 touched
//   the same version range), and T2 read it from a now-stale base.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, FileReadWriteConflict) {
    // v0: add data.parquet
    {
        Transaction txn(table);
        txn.add("data.parquet");
        txn.commit();
    }

    // T1 starts at v0, will modify the table
    Transaction t1(table);
    t1.add("extra.parquet");

    // T2 starts at v0, reads data.parquet
    Transaction t2(table);
    t2.track_read("data.parquet");  // Week 4: declare what we've read
    t2.remove("data.parquet");

    // T1 commits → v1 (adds extra.parquet)
    t1.commit();

    // T2 cannot commit: its base_version is stale AND it read a file
    // from a version that is no longer the latest.
    //
    // Even though T1 didn't touch data.parquet, the version moved.
    // With OCC, ANY concurrent commit invalidates a transaction whose
    // base_version is stale.  The file-level check adds a SECOND layer:
    // even if we allowed stale-base commits (which we don't), we'd still
    // reject T2 if its read set was modified.
    EXPECT_THROW(t2.commit(), ConflictError);
}

// ---------------------------------------------------------------------------
// File-level write-write conflict:
//
//   T1 and T2 both want to remove the same file.  Only one can win.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, FileWriteWriteConflict) {
    {
        Transaction txn(table);
        txn.add("shared.parquet");
        txn.commit();
    }

    Transaction t1(table);
    Transaction t2(table);

    // Both try to remove the same file.
    // T2 also declares a read on it, so the file-level OCC check fires first.
    t1.remove("shared.parquet");
    t2.track_read("shared.parquet");
    t2.remove("shared.parquet");

    t1.commit();  // wins

    // T2 must lose.  Because T2 tracked a read on "shared.parquet" and T1
    // removed it, the file-level OCC check fires BEFORE the generic
    // version-level check, yielding WriteConflictError (the more specific error).
    EXPECT_THROW(t2.commit(), WriteConflictError);  // must lose
}

// ---------------------------------------------------------------------------
// No conflict when transactions touch disjoint files:
//
//   T1 adds file_a.  T2 (started AFTER T1 commits) adds file_b.
//   No overlap → T2 commits fine.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, DisjointWritesNoConflict) {
    {
        Transaction t1(table);
        t1.add("file_a.parquet");
        t1.commit();
    }

    // T2 starts AFTER T1 committed — base_version is now 0
    {
        Transaction t2(table);
        t2.add("file_b.parquet");
        EXPECT_NO_THROW(t2.commit());
    }

    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"file_a.parquet", "file_b.parquet"}));
}

// ---------------------------------------------------------------------------
// No silent overwrites:
//   Verify that after a conflict, the table state is exactly what the
//   winning transaction committed — no partial state from the loser.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, NoSilentOverwrite) {
    {
        Transaction txn(table);
        txn.add("original.parquet");
        txn.commit();
    }

    Transaction t1(table);
    Transaction t2(table);

    t1.add("t1_wins.parquet");
    t2.add("t2_loses.parquet");

    t1.commit();

    // T2 fails
    EXPECT_THROW(t2.commit(), ConflictError);

    // Table must reflect ONLY T1's commit, nothing from T2
    auto snap = build_snapshot(table, 1);
    EXPECT_TRUE(snap.count("original.parquet"));
    EXPECT_TRUE(snap.count("t1_wins.parquet"));
    EXPECT_FALSE(snap.count("t2_loses.parquet"));
}

// ---------------------------------------------------------------------------
// Retry mechanism: after a conflict, creating a new transaction from the
// current state and retrying must succeed.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, RetryAfterConflict) {
    {
        Transaction txn(table);
        txn.add("seed.parquet");
        txn.commit();
    }

    Transaction t1(table);
    Transaction t2(table);

    t1.add("from_t1.parquet");
    t2.add("from_t2.parquet");

    t1.commit();

    // T2 fails due to conflict
    EXPECT_THROW(t2.commit(), ConflictError);

    // Retry: create a fresh transaction from current state
    {
        Transaction retry(table);
        retry.add("from_t2.parquet");
        EXPECT_NO_THROW(retry.commit());
    }

    // Both files should now exist
    auto snap = build_snapshot(table, 2);
    EXPECT_EQ(snap, make_set({"seed.parquet", "from_t1.parquet", "from_t2.parquet"}));
}

// ---------------------------------------------------------------------------
// Track-read on a file that was modified between base and commit:
//
//   v0: {a.parquet, b.parquet}
//   T1: removes a.parquet → v1
//   T2 (started at v0): track_read("a.parquet"), adds c.parquet
//   T2 must fail — it read a.parquet which was removed in v1.
//
// This is the read-write conflict that distinguishes OCC from simple
// version-level checking.  Even if we hypothetically allowed stale bases,
// the file-level check would still catch this.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, ReadOfModifiedFileConflicts) {
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.add("b.parquet");
        txn.commit();
    }

    Transaction t1(table);
    Transaction t2(table);

    // T1 removes a file T2 will read
    t1.remove("a.parquet");

    // T2 reads a.parquet (which T1 is about to remove)
    t2.track_read("a.parquet");
    t2.add("c.parquet");

    t1.commit();  // v1: {b.parquet}

    // T2 must fail: a.parquet (in its read set) was modified after base_version
    EXPECT_THROW(t2.commit(), WriteConflictError);
}

// ---------------------------------------------------------------------------
// Track-read with no overlap in modifications → should still fail on
// version-level conflict (base_version mismatch), but if we add
// file-level granularity in the future, this could be allowed.
// For now, version-level conflict takes precedence.
// ---------------------------------------------------------------------------

TEST_F(OCCTest, ReadNoOverlapStillVersionConflicts) {
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.add("b.parquet");
        txn.commit();
    }

    Transaction t1(table);
    Transaction t2(table);

    // T1 modifies a file T2 does NOT read
    t1.add("c.parquet");

    // T2 reads b.parquet (untouched by T1) but base version still moves
    t2.track_read("b.parquet");
    t2.add("d.parquet");

    t1.commit();

    // Version-level conflict (base_version 0 != latest 1)
    EXPECT_THROW(t2.commit(), ConflictError);
}
