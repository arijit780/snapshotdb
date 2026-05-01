// ---------------------------------------------------------------------------
// crash_commit.cpp — Helper binary for crash-injection tests
//
// This is NOT part of the library.  It exists solely so that tests can
// exercise the crash-injection code paths without killing the test runner.
//
// The test launches this binary as a child process with arguments that
// specify where to crash.  The binary performs a single transaction commit
// and calls _exit(1) at the requested point.
//
// Usage:
//   crash_commit <table_path> <crash_point> [add_file1 add_file2 ...]
//
// crash_point values:
//   "after_fsync"  — crash after fsyncing the .tmp file, before rename
//   "after_rename" — crash after rename (commit is already durable)
//   "none"         — no crash, normal commit
// ---------------------------------------------------------------------------

#include "snapshotdb/transaction.h"
#include "snapshotdb/atomic_writer.h"

#include <iostream>
#include <string>
#include <vector>
#include <optional>

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "usage: crash_commit <table_path> <crash_point> [add_files...]"
                  << std::endl;
        return 2;
    }

    std::string table_path = argv[1];
    std::string crash_str  = argv[2];

    // Parse the crash point argument
    std::optional<snapshotdb::CrashPoint> crash_point;
    if (crash_str == "after_fsync") {
        crash_point = snapshotdb::CrashPoint::AfterFsync;
    } else if (crash_str == "after_rename") {
        crash_point = snapshotdb::CrashPoint::AfterRename;
    } else if (crash_str == "none") {
        crash_point = std::nullopt;
    } else {
        std::cerr << "unknown crash point: " << crash_str << std::endl;
        return 2;
    }

    // Collect the file names to add
    std::vector<std::string> add_files;
    for (int i = 3; i < argc; ++i) {
        add_files.emplace_back(argv[i]);
    }

    // Build and execute the transaction
    try {
        snapshotdb::Transaction txn(table_path);
        for (const auto& f : add_files) {
            txn.add(f);
        }

        if (crash_point.has_value()) {
            txn.commit_with_crash(crash_point.value());
        } else {
            txn.commit();
        }
    } catch (const std::exception& e) {
        std::cerr << "commit failed: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
