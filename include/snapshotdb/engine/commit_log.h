#pragma once

// ---------------------------------------------------------------------------
// commit_log.h — Sequential version log (catalog / metadata layer)
//
// Primary API: functions taking std::shared_ptr<StorageBackend>.
// Convenience: overloads taking std::filesystem::path use LocalFilesystemStorage.
//
// Keys: metadata/versions/{N}.json (see catalog/table_layout.h).
// ---------------------------------------------------------------------------

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>

#include "snapshotdb/catalog/table_layout.h"
#include "snapshotdb/common/log_entry.h"
#include "snapshotdb/storage/backend.h"

namespace snapshotdb {

using catalog::log_key;
using catalog::log_tmp_key;

void ensure_dirs(const std::shared_ptr<StorageBackend>& storage);
void ensure_dirs(const std::filesystem::path& table_path); // local

// Returns committed log object key, e.g. "metadata/versions/0.json"
std::string write_log(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t version,
    const std::vector<std::string>& adds,
    const std::vector<std::string>& removes
);
std::filesystem::path write_log(
    const std::filesystem::path& table_path,
    uint64_t version,
    const std::vector<std::string>& adds,
    const std::vector<std::string>& removes
);

std::vector<LogEntry> read_logs(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t upto_version
);
std::vector<LogEntry> read_logs(
    const std::filesystem::path& table_path,
    uint64_t upto_version
);

int64_t latest_version(const std::shared_ptr<StorageBackend>& storage);
int64_t latest_version(const std::filesystem::path& table_path);

} // namespace snapshotdb
