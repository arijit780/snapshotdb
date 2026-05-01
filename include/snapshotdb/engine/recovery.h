#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <filesystem>

namespace snapshotdb {

class StorageBackend;

struct RecoveryResult {
    int64_t                  valid_version;
    std::vector<std::string> cleaned_tmp_files;
    std::vector<uint64_t>    truncated_versions;
    bool                     was_clean;
};

RecoveryResult recover_log(const std::shared_ptr<StorageBackend>& storage);
RecoveryResult recover_log(const std::filesystem::path& table_path);

int64_t validate_log(const std::shared_ptr<StorageBackend>& storage);
int64_t validate_log(const std::filesystem::path& table_path);

} // namespace snapshotdb
