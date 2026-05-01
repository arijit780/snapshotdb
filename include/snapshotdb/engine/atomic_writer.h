#pragma once

#include <memory>
#include <optional>
#include <string>
#include <filesystem>

#include "snapshotdb/common/log_entry.h"
#include "snapshotdb/storage/backend.h"

namespace snapshotdb {

enum class CrashPoint {
    AfterFsync,
    AfterRename
};

// Returns committed object key e.g. "metadata/versions/3.json"
std::string atomic_write_log(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t version,
    const LogEntry& entry,
    std::optional<CrashPoint> crash_point = std::nullopt
);

/// \param table_root Table directory (not the versions subdirectory).
std::filesystem::path atomic_write_log(
    const std::filesystem::path& table_root,
    uint64_t version,
    const LogEntry& entry,
    std::optional<CrashPoint> crash_point = std::nullopt
);

void cleanup_tmp_files(const std::shared_ptr<StorageBackend>& storage);
/// \param table_root Table directory containing metadata/versions/
void cleanup_tmp_files(const std::filesystem::path& table_root);

} // namespace snapshotdb
