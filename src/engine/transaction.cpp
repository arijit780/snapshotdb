// ---------------------------------------------------------------------------
// transaction.cpp — Atomic commit + OCC (storage-backed)
// ---------------------------------------------------------------------------

#include "snapshotdb/engine/transaction.h"
#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/common/errors.h"
#include "snapshotdb/catalog/table_layout.h"
#include "snapshotdb/storage/backend.h"

#include <chrono>

namespace snapshotdb {

Transaction::Transaction(std::shared_ptr<StorageBackend> storage)
    : storage_(std::move(storage))
{
    ensure_dirs(storage_);
    base_version_ = latest_version(storage_);
}

Transaction::Transaction(const std::filesystem::path& table_path)
    : Transaction(make_local_storage(table_path))
{}

void Transaction::add(const std::string& filename) {
    adds_.push_back(filename);
}

void Transaction::remove(const std::string& filename) {
    removes_.push_back(filename);
}

void Transaction::track_read(const std::string& filename) {
    read_set_.insert(filename);
}

uint64_t Transaction::commit() {
    return commit_inner(std::nullopt);
}

uint64_t Transaction::commit_with_crash(CrashPoint crash_point) {
    return commit_inner(crash_point);
}

uint64_t Transaction::commit_inner(std::optional<CrashPoint> crash_point) {
    uint64_t new_version = static_cast<uint64_t>(base_version_ + 1);

    auto& st = *storage_;

    std::string staging_base(std::string(catalog::TableLayout::kStagingDir) + "/");
    st.mkdir_parents_for_file(staging_base + ".keep");

    for (const auto& filename : adds_) {
        std::string staged = staging_base + filename;
        st.mkdir_parents_for_file(staged);
        if (!st.exists(staged)) {
            st.write_all_bytes(staged, {});
        }
    }

    int64_t current = latest_version(storage_);
    if (current != base_version_) {
        if (!read_set_.empty()) {
            try {
                validate_read_set(current);
            } catch (...) {
                cleanup_staged();
                throw;
            }
        }
        cleanup_staged();
        throw ConflictError(base_version_, current);
    }

    auto now = std::chrono::system_clock::now();
    uint64_t epoch_secs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(
            now.time_since_epoch()
        ).count()
    );

    LogEntry entry{new_version, epoch_secs, adds_, removes_};
    atomic_write_log(storage_, new_version, entry, crash_point);

    std::string data_base(std::string(catalog::TableLayout::kDataDir) + "/");
    for (const auto& filename : adds_) {
        std::string src = staging_base + filename;
        std::string dst = data_base + filename;
        if (st.exists(src)) {
            st.mkdir_parents_for_file(dst);
            try {
                st.rename_no_overwrite(src, dst);
            } catch (const LogError&) {
                if (!st.exists(dst) && st.exists(src)) {
                    throw;
                }
            }
        }
    }

    cleanup_staged();
    return new_version;
}

void Transaction::cleanup_staged() {
    storage_->remove_tree(catalog::TableLayout::kStagingDir);
}

void Transaction::validate_read_set(int64_t current_version) {
    if (read_set_.empty()) return;

    auto all_entries = read_logs(storage_, static_cast<uint64_t>(current_version));

    for (const auto& entry : all_entries) {
        if (entry.version <= static_cast<uint64_t>(base_version_)) {
            continue;
        }
        for (const auto& f : entry.adds) {
            if (read_set_.count(f)) {
                throw WriteConflictError(f, entry.version);
            }
        }
        for (const auto& f : entry.removes) {
            if (read_set_.count(f)) {
                throw WriteConflictError(f, entry.version);
            }
        }
    }
}

} // namespace snapshotdb
