#include "snapshotdb/engine/atomic_writer.h"
#include "snapshotdb/catalog/table_layout.h"
#include "snapshotdb/common/errors.h"
#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/storage/backend.h"

#include <cstdlib>
#include <nlohmann/json.hpp>

namespace snapshotdb {

std::string atomic_write_log(
    const std::shared_ptr<StorageBackend>& st,
    uint64_t version,
    const LogEntry& entry,
    std::optional<CrashPoint> crash_point
) {
    std::string tmp_path   = log_tmp_key(version);
    std::string final_path = log_key(version);

    if (st->exists(final_path)) {
        throw DuplicateVersionError(version);
    }

    nlohmann::json j      = entry;
    std::string    contents = j.dump();

    st->write_all_text(tmp_path, contents);
    st->sync_for_durability(tmp_path);

    if (crash_point == CrashPoint::AfterFsync) {
        std::_Exit(1);
    }

    st->rename_no_overwrite(tmp_path, final_path);

    if (crash_point == CrashPoint::AfterRename) {
        std::_Exit(1);
    }

    return final_path;
}

std::filesystem::path atomic_write_log(
    const std::filesystem::path& table_root,
    uint64_t version,
    const LogEntry& entry,
    std::optional<CrashPoint> crash_point
) {
    auto st = make_local_storage(table_root);
    atomic_write_log(st, version, entry, crash_point);
    return catalog::table_versions_path(table_root) / (std::to_string(version) + ".json");
}

void cleanup_tmp_files(const std::shared_ptr<StorageBackend>& st) {
    auto        names = st->list_dir_filenames(catalog::TableLayout::kVersionsDir);
    std::string prefix(std::string(catalog::TableLayout::kVersionsDir) + "/");
    for (const auto& name : names) {
        if (name.size() > 9 && name.substr(name.size() - 9) == ".json.tmp") {
            st->remove_file(prefix + name);
        }
    }
}

void cleanup_tmp_files(const std::filesystem::path& table_root) {
    cleanup_tmp_files(make_local_storage(table_root));
}

} // namespace snapshotdb
