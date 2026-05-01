// ---------------------------------------------------------------------------
// tools/demo.cpp — CLI demonstration (non-core; keep separate from src/)
//
// This program demonstrates the four core guarantees of the engine:
//
//   1. SNAPSHOT ISOLATION  — a reader never sees concurrent writes
//   2. CONFLICT REJECTION  — two writers to the same base version can't both win
//   3. CRASH RECOVERY      — corrupted log is repaired, snapshot stays valid
//   4. TIME TRAVEL         — any historical version can be reconstructed
//
// No UI.  Just logs and CLI output.
//
// Build: cmake --build build  →  run: build/demo
// ---------------------------------------------------------------------------

#include <iostream>
#include <iomanip>
#include <string>
#include <set>
#include <filesystem>
#include <fstream>

#include "snapshotdb/commit_log.h"
#include "snapshotdb/snapshot.h"
#include "snapshotdb/transaction.h"
#include "snapshotdb/reader.h"
#include "snapshotdb/recovery.h"
#include "snapshotdb/gc.h"
#include "snapshotdb/invariant_checker.h"
#include "snapshotdb/errors.h"

namespace fs = std::filesystem;
using namespace snapshotdb;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static void banner(const std::string& title) {
    std::cout << "\n"
              << "================================================================\n"
              << "  " << title << "\n"
              << "================================================================\n\n";
}

static void print_snapshot(const std::string& label, const std::set<std::string>& files) {
    std::cout << "  " << label << ": {";
    bool first = true;
    for (const auto& f : files) {
        if (!first) std::cout << ", ";
        std::cout << f;
        first = false;
    }
    std::cout << "}\n";
}

static void ok(const std::string& msg) {
    std::cout << "  [OK] " << msg << "\n";
}

static void fail_expected(const std::string& msg) {
    std::cout << "  [EXPECTED FAILURE] " << msg << "\n";
}

// ---------------------------------------------------------------------------
// Setup: create a fresh table in a temp directory
// ---------------------------------------------------------------------------

static fs::path setup_table(const std::string& name) {
    fs::path table = fs::temp_directory_path() / ("snapshotdb_demo_" + name);
    fs::remove_all(table);
    fs::create_directories(table);
    return table;
}

// ---------------------------------------------------------------------------
// Demo 1: Snapshot Isolation
// ---------------------------------------------------------------------------

static void demo_snapshot_isolation() {
    banner("DEMO 1: SNAPSHOT ISOLATION");

    auto table = setup_table("isolation");

    std::cout << "  Committing v0: {alpha.parquet, beta.parquet}\n";
    {
        Transaction txn(table);
        txn.add("alpha.parquet");
        txn.add("beta.parquet");
        txn.commit();
    }

    std::cout << "  Opening a SnapshotReader (pins to v0)...\n";
    SnapshotReader reader(table);

    std::cout << "  Committing v1: adds gamma.parquet (concurrent writer)\n";
    {
        Transaction txn(table);
        txn.add("gamma.parquet");
        txn.commit();
    }

    std::cout << "  Committing v2: removes beta.parquet (concurrent writer)\n";
    {
        Transaction txn(table);
        txn.remove("beta.parquet");
        txn.commit();
    }

    auto reader_files = reader.read();
    print_snapshot("Reader sees (pinned at v0)", reader_files);

    auto latest_snap = build_snapshot(table, 2);
    print_snapshot("Latest snapshot (v2)", latest_snap);

    if (reader_files.count("gamma.parquet") == 0 &&
        reader_files.count("beta.parquet") == 1) {
        ok("Reader did NOT see v1/v2 changes — snapshot isolation holds!");
    }

    assert_all_invariants(table);
    ok("All invariants verified.");

    fs::remove_all(table);
}

// ---------------------------------------------------------------------------
// Demo 2: Conflict Rejection
// ---------------------------------------------------------------------------

static void demo_conflict_rejection() {
    banner("DEMO 2: CONFLICT REJECTION (OCC)");

    auto table = setup_table("conflict");

    std::cout << "  Committing v0: {data.parquet}\n";
    {
        Transaction txn(table);
        txn.add("data.parquet");
        txn.commit();
    }

    std::cout << "  T1 and T2 both open at base_version=0\n";
    Transaction t1(table);
    Transaction t2(table);

    t1.add("t1_output.parquet");
    t2.add("t2_output.parquet");
    t2.track_read("data.parquet");

    std::cout << "  T1 commits successfully...\n";
    t1.commit();
    ok("T1 committed -> v1");

    std::cout << "  T2 tries to commit...\n";
    try {
        t2.commit();
        std::cout << "  [BUG] T2 should have failed!\n";
    } catch (const WriteConflictError& e) {
        fail_expected(std::string("T2 rejected: ") + e.what());
    } catch (const ConflictError& e) {
        fail_expected(std::string("T2 rejected: ") + e.what());
    }

    std::cout << "\n  Retrying T2 with fresh base...\n";
    {
        Transaction retry(table);
        retry.add("t2_output.parquet");
        retry.commit();
        ok("Retry succeeded -> v2");
    }

    auto snap = build_snapshot(table, 2);
    print_snapshot("Final snapshot (v2)", snap);

    assert_all_invariants(table);
    ok("All invariants verified.");

    fs::remove_all(table);
}

// ---------------------------------------------------------------------------
// Demo 3: Crash Recovery
// ---------------------------------------------------------------------------

static void demo_crash_recovery() {
    banner("DEMO 3: CRASH RECOVERY");

    auto table = setup_table("recovery");

    std::cout << "  Committing v0: {important.parquet}\n";
    write_log(table, 0, {"important.parquet"}, {});

    std::cout << "  Committing v1: {also_important.parquet}\n";
    write_log(table, 1, {"also_important.parquet"}, {});

    std::cout << "  Committing v2: {new_data.parquet}\n";
    write_log(table, 2, {"new_data.parquet"}, {});

    std::cout << "\n  --- Simulating crash damage ---\n";
    std::cout << "  Corrupting v2 log file (bad JSON)...\n";
    std::ofstream(table / "metadata" / "versions" / "2.json") << "CRASH_CORRUPTED{{{";

    std::cout << "  Planting orphan .tmp file (incomplete atomic write)...\n";
    std::ofstream(table / "metadata" / "versions" / "3.json.tmp") << "{}";

    std::cout << "\n  --- Running recovery ---\n";
    auto result = recover_log(table);

    std::cout << "  Recovery result:\n";
    std::cout << "    Valid version: " << result.valid_version << "\n";
    std::cout << "    Cleaned .tmp files: " << result.cleaned_tmp_files.size() << "\n";
    std::cout << "    Truncated versions: " << result.truncated_versions.size() << "\n";
    std::cout << "    Was clean: " << (result.was_clean ? "yes" : "no") << "\n";

    auto snap = build_snapshot(table, static_cast<uint64_t>(result.valid_version));
    print_snapshot("Recovered snapshot", snap);

    if (snap.count("important.parquet") && snap.count("also_important.parquet") &&
        !snap.count("new_data.parquet")) {
        ok("Corrupted commit discarded, valid data preserved!");
    }

    assert_all_invariants(table);
    ok("All invariants verified after recovery.");

    fs::remove_all(table);
}

// ---------------------------------------------------------------------------
// Demo 4: Time Travel
// ---------------------------------------------------------------------------

static void demo_time_travel() {
    banner("DEMO 4: TIME TRAVEL");

    auto table = setup_table("timetravel");

    std::cout << "  Building version history:\n";
    std::cout << "    v0: {users.parquet}\n";
    {
        Transaction txn(table);
        txn.add("users.parquet");
        txn.commit();
    }

    std::cout << "    v1: +{orders.parquet}\n";
    {
        Transaction txn(table);
        txn.add("orders.parquet");
        txn.commit();
    }

    std::cout << "    v2: +{products.parquet}, -{users.parquet} (schema migration)\n";
    {
        Transaction txn(table);
        txn.add("products.parquet");
        txn.remove("users.parquet");
        txn.commit();
    }

    std::cout << "    v3: +{users_v2.parquet}\n";
    {
        Transaction txn(table);
        txn.add("users_v2.parquet");
        txn.commit();
    }

    std::cout << "\n  Querying every version (time travel):\n";
    for (uint64_t v = 0; v <= 3; ++v) {
        auto snap = build_snapshot(table, v);
        print_snapshot("  v" + std::to_string(v), snap);
    }

    std::cout << "\n  Demonstrating vacuum with retention:\n";

    // Create data files
    fs::create_directories(table / "data");
    std::ofstream(table / "data" / "users.parquet").close();
    std::ofstream(table / "data" / "orders.parquet").close();
    std::ofstream(table / "data" / "products.parquet").close();
    std::ofstream(table / "data" / "users_v2.parquet").close();

    auto gc_result = vacuum(table, 2);  // keep last 2 versions (v2, v3)
    std::cout << "    vacuum(retain_last_n=2):\n";
    std::cout << "      Retained versions: [" << gc_result.retained_from_version
              << ", " << gc_result.retained_to_version << "]\n";
    std::cout << "      Referenced files: {";
    bool first = true;
    for (const auto& f : gc_result.referenced_files) {
        if (!first) std::cout << ", ";
        std::cout << f;
        first = false;
    }
    std::cout << "}\n";

    if (!gc_result.deleted_files.empty()) {
        std::cout << "      Deleted: {";
        first = true;
        for (const auto& f : gc_result.deleted_files) {
            if (!first) std::cout << ", ";
            std::cout << f;
            first = false;
        }
        std::cout << "}\n";
    } else {
        std::cout << "      Deleted: (none)\n";
    }

    assert_all_invariants(table);
    ok("All invariants verified.");

    fs::remove_all(table);
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

int main() {
    std::cout << "snapshotdb — Engine Demonstration\n"
              << "A log-based table state engine with atomic commits.\n";

    demo_snapshot_isolation();
    demo_conflict_rejection();
    demo_crash_recovery();
    demo_time_travel();

    banner("ALL DEMOS COMPLETE");
    std::cout << "  Every operation maintained all five structural invariants.\n"
              << "  The system never returned an inconsistent snapshot.\n\n";

    return 0;
}
