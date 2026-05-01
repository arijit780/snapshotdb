#pragma once

// ---------------------------------------------------------------------------
// log_entry.h — The on-disk schema for a single log record
//
// Each version of the table corresponds to exactly one LogEntry, serialized
// as JSON into metadata/versions/{version}.json.  The struct here is the in-memory
// representation; nlohmann/json macros generate the serializer/deserializer.
//
// On-disk format example:
//   {
//     "version": 0,
//     "timestamp": 1714400000,
//     "adds": ["file_a.parquet"],
//     "removes": []
//   }
// ---------------------------------------------------------------------------

#include <cstdint>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>

namespace snapshotdb {

struct LogEntry {
    uint64_t              version;    // monotonically increasing, zero-based
    uint64_t              timestamp;  // unix epoch seconds at write time
    std::vector<std::string> adds;    // files introduced in this version
    std::vector<std::string> removes; // files retired in this version

    // nlohmann/json automatic serialization / deserialization
    NLOHMANN_DEFINE_TYPE_INTRUSIVE(LogEntry, version, timestamp, adds, removes)
};

} // namespace snapshotdb
