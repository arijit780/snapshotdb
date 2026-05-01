#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <filesystem>

namespace snapshotdb {

class StorageBackend;

class SnapshotReader {
public:
    explicit SnapshotReader(
        std::shared_ptr<StorageBackend> storage,
        std::optional<uint64_t> version = std::nullopt
    );

    explicit SnapshotReader(
        const std::filesystem::path& table_path,
        std::optional<uint64_t> version = std::nullopt
    );

    const std::set<std::string>& read() const;
    int64_t pinned_version() const { return pinned_version_; }

private:
    std::shared_ptr<StorageBackend> storage_;
    int64_t                         pinned_version_;
    std::set<std::string>           cached_snapshot_;
};

} // namespace snapshotdb
