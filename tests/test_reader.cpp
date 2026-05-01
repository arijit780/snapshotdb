// ---------------------------------------------------------------------------
// test_reader.cpp — Week 3 snapshot isolation tests
//
// These tests prove that SnapshotReader delivers true snapshot isolation:
//   • A reader pinned to version V never sees V+1, even after it's committed.
//   • Multiple read() calls return the exact same set.
//   • An explicit version pin works correctly.
//   • An empty table yields an empty snapshot.
//
// The concurrency model here is simulated (single-threaded interleaving)
// because the guarantees are structural, not timing-dependent:
//   - The reader caches its snapshot in the constructor.
//   - read() returns a const reference to the cache.
//   - No code path exists to re-read the log.
//
// If someone refactors read() to call build_snapshot() again, these tests
// will catch the regression immediately.
// ---------------------------------------------------------------------------

#include <gtest/gtest.h>
#include <filesystem>
#include <set>
#include <string>

#include "snapshotdb/reader.h"
#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/transaction.h"
#include "snapshotdb/errors.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

class ReaderTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_reader_" + std::to_string(std::rand()));
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
// Core snapshot isolation test:
//
//   1. Commit version 0 with file "a.parquet"
//   2. Open a SnapshotReader (pins to version 0)
//   3. Commit version 1 with file "b.parquet"
//   4. Reader.read() must still return ONLY {"a.parquet"}
//
// If the reader re-fetched the log, it would see "b.parquet" — that would
// be read-committed isolation, NOT snapshot isolation.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, ReaderDoesNotSeeNewerVersion) {
    // Version 0: add a.parquet
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.commit();
    }

    // Open a reader — pins to version 0
    SnapshotReader reader(table);
    EXPECT_EQ(reader.pinned_version(), 0);

    // Another writer commits version 1 AFTER the reader was created
    {
        Transaction txn(table);
        txn.add("b.parquet");
        txn.commit();
    }

    // Reader must NOT see b.parquet
    auto files = reader.read();
    EXPECT_EQ(files, make_set({"a.parquet"}));
    EXPECT_EQ(files.count("b.parquet"), 0u);
}

// ---------------------------------------------------------------------------
// Multiple reads return the exact same result — no drift.
//
// We read(), commit a new version, then read() again.  Both results
// must be identical.  This catches any implementation that re-reads
// the log on each call.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, MultipleReadsAreStable) {
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.commit();
    }

    SnapshotReader reader(table);

    // First read
    auto first = reader.read();

    // Concurrent commit
    {
        Transaction txn(table);
        txn.add("b.parquet");
        txn.commit();
    }

    // Second read — must match the first exactly
    auto second = reader.read();
    EXPECT_EQ(first, second);
}

// ---------------------------------------------------------------------------
// Interleaved read/write scenario with three versions.
//
//   v0: {a}    v1: {a, b}    v2: {a, b, c}
//
// A reader pinned at v1 sees {a, b} even after v2 is committed.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, InterleavedReadWrite) {
    // Build up v0 and v1
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});

    // Pin reader at v1
    SnapshotReader reader(table, 1);

    // Commit v2 (concurrent writer)
    write_log(table, 2, {"c.parquet"}, {});

    // Reader must see exactly v1's state
    auto files = reader.read();
    EXPECT_EQ(files, make_set({"a.parquet", "b.parquet"}));
}

// ---------------------------------------------------------------------------
// Explicit version pinning — reader binds to v1 even when v2 exists.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, ExplicitVersionPin) {
    write_log(table, 0, {"a.parquet"}, {});
    write_log(table, 1, {"b.parquet"}, {});
    write_log(table, 2, {"c.parquet"}, {});

    // Pin to v1 — should NOT see v2
    SnapshotReader reader(table, 1);
    EXPECT_EQ(reader.pinned_version(), 1);

    auto files = reader.read();
    EXPECT_EQ(files, make_set({"a.parquet", "b.parquet"}));
}

// ---------------------------------------------------------------------------
// Empty table — reader pins to version -1, read() returns empty set.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, EmptyTable) {
    SnapshotReader reader(table);
    EXPECT_EQ(reader.pinned_version(), -1);
    EXPECT_TRUE(reader.read().empty());
}

// ---------------------------------------------------------------------------
// Requesting a version that doesn't exist must throw.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, NonExistentVersionThrows) {
    write_log(table, 0, {"a.parquet"}, {});

    EXPECT_THROW(
        SnapshotReader(table, 5),
        MissingVersionError
    );
}

// ---------------------------------------------------------------------------
// Verify no mixed-version reads:
//
//   v0: {a, b}
//   v1: removes b, adds c  →  {a, c}
//
// A reader at v0 must see {a, b} (b is NOT removed).
// A reader at v1 must see {a, c} (b IS removed).
// There must be no state where a reader sees {a, b, c} (mixed).
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, NoMixedVersionReads) {
    write_log(table, 0, {"a.parquet", "b.parquet"}, {});
    write_log(table, 1, {"c.parquet"}, {"b.parquet"});

    SnapshotReader reader_v0(table, 0);
    SnapshotReader reader_v1(table, 1);

    EXPECT_EQ(reader_v0.read(), make_set({"a.parquet", "b.parquet"}));
    EXPECT_EQ(reader_v1.read(), make_set({"a.parquet", "c.parquet"}));

    // The forbidden mixed state {a, b, c} must never appear
    auto v0_files = reader_v0.read();
    auto v1_files = reader_v1.read();
    EXPECT_NE(v0_files, make_set({"a.parquet", "b.parquet", "c.parquet"}));
    EXPECT_NE(v1_files, make_set({"a.parquet", "b.parquet", "c.parquet"}));
}

// ---------------------------------------------------------------------------
// Stress: many commits after reader creation, reader stays frozen.
// ---------------------------------------------------------------------------

TEST_F(ReaderTest, StableUnderManyConcurrentCommits) {
    {
        Transaction txn(table);
        txn.add("base.parquet");
        txn.commit();
    }

    SnapshotReader reader(table);
    auto baseline = reader.read();

    // Simulate 20 concurrent commits
    for (int i = 0; i < 20; ++i) {
        Transaction txn(table);
        txn.add("file_" + std::to_string(i) + ".parquet");
        txn.commit();
    }

    // Reader must be completely unaffected
    EXPECT_EQ(reader.read(), baseline);
    EXPECT_EQ(reader.read().size(), 1u);
    EXPECT_TRUE(reader.read().count("base.parquet"));
}
