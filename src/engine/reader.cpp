#include "snapshotdb/engine/reader.h"
#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/engine/snapshot.h"
#include "snapshotdb/storage/backend.h"

namespace snapshotdb {

SnapshotReader::SnapshotReader(
    std::shared_ptr<StorageBackend> storage,
    std::optional<uint64_t> version
)
    : storage_(std::move(storage))
{
    if (version.has_value()) {
        pinned_version_ = static_cast<int64_t>(version.value());
    } else {
        pinned_version_ = latest_version(storage_);
    }

    if (pinned_version_ >= 0) {
        cached_snapshot_ = build_snapshot(storage_, static_cast<uint64_t>(pinned_version_));
    }
}

SnapshotReader::SnapshotReader(
    const std::filesystem::path& table_path,
    std::optional<uint64_t> version
)
    : SnapshotReader(make_local_storage(table_path), version)
{}

const std::set<std::string>& SnapshotReader::read() const {
    return cached_snapshot_;
}

} // namespace snapshotdb
