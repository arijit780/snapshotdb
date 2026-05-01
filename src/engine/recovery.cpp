#include "snapshotdb/engine/recovery.h"
#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/engine/atomic_writer.h"
#include "snapshotdb/catalog/table_layout.h"
#include "snapshotdb/common/errors.h"
#include "snapshotdb/common/log_entry.h"
#include "snapshotdb/storage/backend.h"

#include <algorithm>
#include <nlohmann/json.hpp>

namespace snapshotdb {

static std::vector<uint64_t> collect_version_files(const std::shared_ptr<StorageBackend>& st) {
    std::vector<uint64_t> versions;
    auto                  names = st->list_dir_filenames(catalog::TableLayout::kVersionsDir);
    const std::string     suffix = ".json";

    for (const std::string& filename : names) {
        if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".json.tmp") {
            continue;
        }
        if (filename.size() <= suffix.size()) continue;
        if (filename.substr(filename.size() - suffix.size()) != suffix) continue;
        std::string stem = filename.substr(0, filename.size() - suffix.size());
        try {
            versions.push_back(std::stoull(stem));
        } catch (...) {}
    }
    std::sort(versions.begin(), versions.end());
    return versions;
}

static bool try_parse_log_file(const std::shared_ptr<StorageBackend>& st, uint64_t version) {
    std::string path = log_key(version);
    if (!st->exists(path)) return false;
    std::string contents = st->read_all_text(path);
    if (contents.empty()) return false;
    try {
        nlohmann::json j = nlohmann::json::parse(contents);
        LogEntry entry   = j.get<LogEntry>();
        return entry.version == version;
    } catch (...) {
        return false;
    }
}

int64_t validate_log(const std::shared_ptr<StorageBackend>& st) {
    auto versions = collect_version_files(st);
    if (versions.empty()) return -1;

    int64_t last_valid = -1;
    for (uint64_t expected = 0;; ++expected) {
        auto it = std::find(versions.begin(), versions.end(), expected);
        if (it == versions.end()) break;
        if (!try_parse_log_file(st, expected)) break;
        last_valid = static_cast<int64_t>(expected);
    }
    return last_valid;
}

int64_t validate_log(const std::filesystem::path& table_path) {
    return validate_log(make_local_storage(table_path));
}

RecoveryResult recover_log(const std::shared_ptr<StorageBackend>& st) {
    ensure_dirs(st);

    RecoveryResult result;
    result.was_clean = true;

    std::string prefix(std::string(catalog::TableLayout::kVersionsDir) + "/");
    auto        names = st->list_dir_filenames(catalog::TableLayout::kVersionsDir);
    for (const std::string& name : names) {
        if (name.size() > 9 && name.substr(name.size() - 9) == ".json.tmp") {
            st->remove_file(prefix + name);
            result.cleaned_tmp_files.push_back(name);
            result.was_clean = false;
        }
    }

    int64_t last_valid = validate_log(st);

    auto all_versions = collect_version_files(st);
    for (uint64_t v : all_versions) {
        if (static_cast<int64_t>(v) > last_valid) {
            st->remove_file(log_key(v));
            result.truncated_versions.push_back(v);
            result.was_clean = false;
        }
    }

    std::sort(result.truncated_versions.begin(), result.truncated_versions.end());
    result.valid_version = last_valid;
    return result;
}

RecoveryResult recover_log(const std::filesystem::path& table_path) {
    return recover_log(make_local_storage(table_path));
}

} // namespace snapshotdb
