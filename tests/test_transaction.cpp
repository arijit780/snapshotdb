// ---------------------------------------------------------------------------
// test_transaction.cpp — Week 2 tests for the Transaction class
//
// These tests verify the high-level transactional API:
//   • A basic commit creates a new version visible in the snapshot
//   • Sequential commits build on each other
//   • Concurrent transactions from the same base conflict correctly
//   • Removes via transaction are reflected in the snapshot
//   • Staged data files end up in data/, not data/staging/
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

class TransactionTest : public ::testing::Test {
protected:
    fs::path table;

    void SetUp() override {
        table = fs::temp_directory_path() / ("snapshotdb_txn_" + std::to_string(std::rand()));
        fs::create_directories(table);
    }

    void TearDown() override {
        fs::remove_all(table);
    }
};

static std::set<std::string> make_set(std::initializer_list<std::string> vals) {
    return std::set<std::string>(vals);
}

// --- Single commit on an empty table → version 0 ---

TEST_F(TransactionTest, BasicCommit) {
    Transaction txn(table);
    txn.add("a.parquet");
    txn.add("b.parquet");
    uint64_t v = txn.commit();

    EXPECT_EQ(v, 0u);
    auto snap = build_snapshot(table, 0);
    EXPECT_EQ(snap, make_set({"a.parquet", "b.parquet"}));
}

// --- Two sequential commits stack correctly ---

TEST_F(TransactionTest, SequentialCommits) {
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.commit();
    }
    {
        Transaction txn(table);
        txn.add("b.parquet");
        uint64_t v = txn.commit();
        EXPECT_EQ(v, 1u);
    }

    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "b.parquet"}));
}

// --- Two transactions opened at the same base: second must conflict ---

TEST_F(TransactionTest, ConflictDetection) {
    // Both transactions see the same base_version (-1, empty table)
    Transaction txn1(table);
    Transaction txn2(table);

    txn1.add("a.parquet");
    txn2.add("b.parquet");

    // First commit succeeds
    txn1.commit();

    // Second must throw ConflictError because base_version is stale
    EXPECT_THROW(txn2.commit(), ConflictError);
}

// --- Remove via transaction is reflected in the snapshot ---

TEST_F(TransactionTest, AddAndRemoveViaTransaction) {
    // First: add some files
    {
        Transaction txn(table);
        txn.add("a.parquet");
        txn.add("b.parquet");
        txn.add("c.parquet");
        txn.commit();
    }

    // Second: remove one
    {
        Transaction txn(table);
        txn.remove("b.parquet");
        txn.commit();
    }

    auto snap = build_snapshot(table, 1);
    EXPECT_EQ(snap, make_set({"a.parquet", "c.parquet"}));
}

// --- After commit, data files live in data/, not data/staging/ ---

TEST_F(TransactionTest, DataFilesNotInTmpAfterCommit) {
    Transaction txn(table);
    txn.add("x.parquet");
    txn.commit();

    // Staging directory should be cleaned up
    EXPECT_FALSE(fs::exists(table / "data" / "staging"));

    // Data file should be in its final location
    EXPECT_TRUE(fs::exists(table / "data" / "x.parquet"));
}

// --- New transaction on empty table has base_version -1 ---

TEST_F(TransactionTest, EmptyTableBaseVersion) {
    Transaction txn(table);
    EXPECT_EQ(txn.base_version(), -1);
}

// --- latest_version updates after commit ---

TEST_F(TransactionTest, LatestVersionReflectsCommits) {
    EXPECT_EQ(latest_version(table), -1);

    Transaction txn(table);
    txn.add("a.parquet");
    txn.commit();

    EXPECT_EQ(latest_version(table), 0);
}
