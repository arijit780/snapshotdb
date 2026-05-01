#pragma once

// ---------------------------------------------------------------------------
// table_layout.h — Iceberg-style table directory layout (catalog layer)
//
// Mirrors common lakehouse separation:
//   • metadata/   — version chain JSON, future manifest pointers, sidecars
//   • data/       — committed object files (e.g. *.parquet)
//   • data/staging — uncommitted / in-flight writes (replaces a literal "tmp")
//
// Keys are always relative to the table root and work with any StorageBackend.
// ---------------------------------------------------------------------------

#include <cstdint>
#include <filesystem>
#include <string>

namespace snapshotdb::catalog {

struct TableLayout {
    /// Root for JSON that describes the table / version lineage.
    static constexpr const char* kMetadataDir = "metadata";
    /// Sequential commit log entries: metadata/versions/{N}.json
    static constexpr const char* kVersionsDir = "metadata/versions";
    /// Committed data objects (logical paths stored in the log).
    static constexpr const char* kDataDir = "data";
    /// In-flight writes before commit (cleared after each commit).
    static constexpr const char* kStagingDir = "data/staging";
};

inline std::filesystem::path table_versions_path(const std::filesystem::path& table_root) {
    return table_root / "metadata" / "versions";
}

inline std::filesystem::path table_staging_path(const std::filesystem::path& table_root) {
    return table_root / "data" / "staging";
}

inline std::string log_key(uint64_t version) {
    return std::string(TableLayout::kVersionsDir) + "/" + std::to_string(version) + ".json";
}

inline std::string log_tmp_key(uint64_t version) {
    return std::string(TableLayout::kVersionsDir) + "/" + std::to_string(version) + ".json.tmp";
}

} // namespace snapshotdb::catalog
