#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <filesystem>

namespace snapshotdb {

class StorageBackend;

std::set<std::string> build_snapshot(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t version
);

std::set<std::string> build_snapshot(
    const std::filesystem::path& table_path,
    uint64_t version
);

} // namespace snapshotdb
