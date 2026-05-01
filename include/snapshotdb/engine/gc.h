#pragma once

#include <cstdint>
#include <set>
#include <string>
#include <vector>
#include <filesystem>
#include <memory>

namespace snapshotdb {

class StorageBackend;

struct VacuumResult {
    uint64_t              retained_from_version;
    uint64_t              retained_to_version;
    std::set<std::string> referenced_files;
    std::vector<std::string> deleted_files;
};

VacuumResult vacuum(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t retain_last_n
);
VacuumResult vacuum(
    const std::filesystem::path& table_path,
    uint64_t retain_last_n
);

std::set<std::string> files_referenced_in_range(
    const std::shared_ptr<StorageBackend>& storage,
    uint64_t from_version,
    uint64_t to_version
);
std::set<std::string> files_referenced_in_range(
    const std::filesystem::path& table_path,
    uint64_t from_version,
    uint64_t to_version
);

} // namespace snapshotdb
