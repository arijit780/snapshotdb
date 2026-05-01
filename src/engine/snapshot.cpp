#include "snapshotdb/engine/snapshot.h"
#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/storage/backend.h"

#include <vector>

namespace snapshotdb {

std::set<std::string> build_snapshot(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t version
) {
    std::vector<LogEntry> entries = read_logs(storage, version);
    std::set<std::string> active_files;

    for (const auto& entry : entries) {
        for (const auto& f : entry.adds) {
            active_files.insert(f);
        }
        for (const auto& f : entry.removes) {
            active_files.erase(f);
        }
    }
    return active_files;
}

std::set<std::string> build_snapshot(
    const std::filesystem::path& table_path,
    uint64_t version
) {
    return build_snapshot(make_local_storage(table_path), version);
}

} // namespace snapshotdb
