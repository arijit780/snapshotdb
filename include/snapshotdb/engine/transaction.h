#pragma once

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <filesystem>
#include <optional>

#include "snapshotdb/engine/atomic_writer.h"

namespace snapshotdb {

class StorageBackend;

class Transaction {
public:
    explicit Transaction(std::shared_ptr<StorageBackend> storage);
    explicit Transaction(const std::filesystem::path& table_path);

    std::shared_ptr<StorageBackend> storage() const { return storage_; }

    void add(const std::string& filename);
    void remove(const std::string& filename);
    void track_read(const std::string& filename);

    int64_t base_version() const { return base_version_; }

    uint64_t commit();
    uint64_t commit_with_crash(CrashPoint crash_point);

private:
    uint64_t commit_inner(std::optional<CrashPoint> crash_point);
    void cleanup_staged();
    void validate_read_set(int64_t current_version);

    std::shared_ptr<StorageBackend> storage_;
    int64_t                         base_version_;
    std::vector<std::string>        adds_;
    std::vector<std::string>        removes_;
    std::set<std::string>           read_set_;
};

} // namespace snapshotdb
