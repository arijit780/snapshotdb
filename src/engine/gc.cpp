#include "snapshotdb/engine/gc.h"
#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/engine/snapshot.h"
#include "snapshotdb/catalog/table_layout.h"
#include "snapshotdb/common/errors.h"
#include "snapshotdb/storage/backend.h"

namespace snapshotdb {

std::set<std::string> files_referenced_in_range(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t from_version,
    uint64_t to_version
) {
    std::set<std::string> referenced;
    for (uint64_t v = from_version; v <= to_version; ++v) {
        auto snap = build_snapshot(storage, v);
        referenced.insert(snap.begin(), snap.end());
    }
    return referenced;
}

std::set<std::string> files_referenced_in_range(
    const std::filesystem::path& table_path,
    uint64_t from_version,
    uint64_t to_version
) {
    return files_referenced_in_range(make_local_storage(table_path), from_version, to_version);
}

static std::vector<std::string> scan_data_files(const std::shared_ptr<StorageBackend>& st) {
    return st->list_dir_filenames(catalog::TableLayout::kDataDir);
}

VacuumResult vacuum(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t retain_last_n
) {
    if (retain_last_n == 0) {
        retain_last_n = 1;
    }

    int64_t latest = latest_version(storage);
    if (latest < 0) {
        throw LogError("cannot vacuum an empty table (no committed versions)");
    }

    uint64_t latest_u = static_cast<uint64_t>(latest);
    uint64_t from_version = 0;
    if (latest_u >= retain_last_n) {
        from_version = latest_u - retain_last_n + 1;
    }

    std::set<std::string> referenced = files_referenced_in_range(
        storage, from_version, latest_u
    );

    std::vector<std::string> physical_files = scan_data_files(storage);
    std::vector<std::string> deleted;

    std::string data_prefix(std::string(catalog::TableLayout::kDataDir) + "/");
    for (const auto& filename : physical_files) {
        if (referenced.find(filename) == referenced.end()) {
            storage->remove_file(data_prefix + filename);
            deleted.push_back(filename);
        }
    }

    return VacuumResult{
        from_version,
        latest_u,
        std::move(referenced),
        std::move(deleted)
    };
}

VacuumResult vacuum(
    const std::filesystem::path& table_path,
    uint64_t retain_last_n
) {
    return vacuum(make_local_storage(table_path), retain_last_n);
}

} // namespace snapshotdb
