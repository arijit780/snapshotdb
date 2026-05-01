// ---------------------------------------------------------------------------
// commit_log.cpp — The log IS the database (metadata / versions layer)
// ---------------------------------------------------------------------------

#include "snapshotdb/engine/commit_log.h"
#include "snapshotdb/common/errors.h"

#include <chrono>
#include <nlohmann/json.hpp>
#include <vector>

namespace snapshotdb {

void ensure_dirs(const std::shared_ptr<StorageBackend>& st) {
    using catalog::TableLayout;
    st->mkdir_parents_for_file(std::string(TableLayout::kVersionsDir) + "/.keep");
    st->mkdir_parents_for_file(std::string(TableLayout::kDataDir) + "/.keep");
    st->mkdir_parents_for_file(std::string(TableLayout::kStagingDir) + "/.keep");
}

void ensure_dirs(const std::filesystem::path& table_path) {
    ensure_dirs(make_local_storage(table_path));
}

std::string write_log(
    const std::shared_ptr<StorageBackend>& st,
    uint64_t version,
    const std::vector<std::string>& adds,
    const std::vector<std::string>& removes
) {
    ensure_dirs(st);
    std::string path = log_key(version);

    if (st->exists(path)) {
        throw DuplicateVersionError(version);
    }

    if (version > 0) {
        std::string prev = log_key(version - 1);
        if (!st->exists(prev)) {
            throw MissingVersionError(version - 1);
        }
    }

    auto now = std::chrono::system_clock::now();
    auto epoch_secs = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count()
    );

    LogEntry entry{version, epoch_secs, adds, removes};
    nlohmann::json j = entry;
    st->write_all_text(path, j.dump());
    return path;
}

std::filesystem::path write_log(
    const std::filesystem::path& table_path,
    uint64_t version,
    const std::vector<std::string>& adds,
    const std::vector<std::string>& removes
) {
    auto st = make_local_storage(table_path);
    std::string rel = write_log(st, version, adds, removes);
    return table_path / std::filesystem::path(rel);
}

std::vector<LogEntry> read_logs(
    const std::shared_ptr<StorageBackend>& st,
    uint64_t upto_version
) {
    std::vector<LogEntry> entries;
    entries.reserve(upto_version + 1);

    for (uint64_t v = 0; v <= upto_version; ++v) {
        std::string path = log_key(v);
        if (!st->exists(path)) {
            throw MissingVersionError(v);
        }
        std::string contents = st->read_all_text(path);
        try {
            nlohmann::json j = nlohmann::json::parse(contents);
            LogEntry entry = j.get<LogEntry>();
            entries.push_back(std::move(entry));
        } catch (const nlohmann::json::exception& e) {
            throw CorruptedJsonError(v, e.what());
        }
    }
    return entries;
}

std::vector<LogEntry> read_logs(
    const std::filesystem::path& table_path,
    uint64_t upto_version
) {
    return read_logs(make_local_storage(table_path), upto_version);
}

int64_t latest_version(const std::shared_ptr<StorageBackend>& st) {
    auto names = st->list_dir_filenames(catalog::TableLayout::kVersionsDir);
    int64_t max_version = -1;
    const std::string suffix = ".json";

    for (const std::string& filename : names) {
        if (filename.size() > 9 && filename.substr(filename.size() - 9) == ".json.tmp") {
            continue;
        }
        if (filename.size() <= suffix.size()) continue;
        if (filename.substr(filename.size() - suffix.size()) != suffix) continue;
        std::string stem = filename.substr(0, filename.size() - suffix.size());
        try {
            uint64_t v = std::stoull(stem);
            if (static_cast<int64_t>(v) > max_version) {
                max_version = static_cast<int64_t>(v);
            }
        } catch (...) {}
    }
    return max_version;
}

int64_t latest_version(const std::filesystem::path& table_path) {
    return latest_version(make_local_storage(table_path));
}

} // namespace snapshotdb
