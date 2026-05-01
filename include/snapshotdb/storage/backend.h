#pragma once

// ---------------------------------------------------------------------------
// storage.h — Pluggable object/file access for snapshotdb
//
// All engine paths are POSIX-style KEYS relative to the logical table root
// (Iceberg-style layout; see catalog/table_layout.h), e.g.
// `metadata/versions/0.json`, `data/part-000.parquet`.
//
// Implementations:
//   • LocalFilesystemStorage — maps keys to files under a directory on disk
//   • AzureSasBlobStorage     — maps keys to blob names (optional; see storage/azure.h)
//
// Callers normally hold `std::shared_ptr<StorageBackend>` and pass it into
// Transaction, SnapshotReader, vacuum, recover_log, etc.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <filesystem>

namespace snapshotdb {

// Normalize: trim, drop leading '/', flip '\' -> '/', collapse duplicate '/'
std::string normalize_rel_path(std::string_view rel);

class StorageBackend {
public:
    virtual ~StorageBackend() = default;

    virtual bool exists(const std::string& rel_path) = 0;

    virtual std::string read_all_text(const std::string& rel_path) = 0;
    virtual void write_all_text(const std::string& rel_path, const std::string& utf8) = 0;

    virtual std::vector<std::uint8_t> read_all_bytes(const std::string& rel_path) = 0;
    virtual void write_all_bytes(const std::string& rel_path, const std::vector<std::uint8_t>& data) = 0;

    virtual void mkdir_parents_for_file(const std::string& rel_path) = 0;

    virtual void remove_file(const std::string& rel_path) = 0;

    // From must exist; to must not exist. Local: atomic rename when same volume.
    virtual void rename_no_overwrite(const std::string& from_rel, const std::string& to_rel) = 0;

    // Best-effort durability after a write (fsync on local; optional no-op on blob).
    virtual void sync_for_durability(const std::string& rel_path) = 0;

    // List file names in `rel_dir` (e.g. "metadata/versions"); not recursive.
    virtual std::vector<std::string> list_dir_filenames(const std::string& rel_dir) = 0;

    // Delete everything under this relative prefix (directory tree). Best-effort for blob (list+delete).
    virtual void remove_tree(const std::string& rel_dir_prefix) = 0;
};

// Local disk: root_directory + "/" + rel_key
class LocalFilesystemStorage final : public StorageBackend {
public:
    explicit LocalFilesystemStorage(std::filesystem::path root_directory);
    const std::filesystem::path& root() const { return root_; }

    bool exists(const std::string& rel_path) override;
    std::string read_all_text(const std::string& rel_path) override;
    void write_all_text(const std::string& rel_path, const std::string& utf8) override;
    std::vector<std::uint8_t> read_all_bytes(const std::string& rel_path) override;
    void write_all_bytes(const std::string& rel_path, const std::vector<std::uint8_t>& data) override;
    void mkdir_parents_for_file(const std::string& rel_path) override;
    void remove_file(const std::string& rel_path) override;
    void rename_no_overwrite(const std::string& from_rel, const std::string& to_rel) override;
    void sync_for_durability(const std::string& rel_path) override;
    std::vector<std::string> list_dir_filenames(const std::string& rel_dir) override;
    void remove_tree(const std::string& rel_dir_prefix) override;

private:
    std::filesystem::path abs(const std::string& rel) const;

    std::filesystem::path root_;
};

// Convenience: shared local store for a table directory
inline std::shared_ptr<LocalFilesystemStorage> make_local_storage(std::filesystem::path table_root) {
    return std::make_shared<LocalFilesystemStorage>(std::move(table_root));
}

} // namespace snapshotdb
