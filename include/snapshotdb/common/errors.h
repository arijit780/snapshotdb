#pragma once

// ---------------------------------------------------------------------------
// errors.h — Exception hierarchy for snapshotdb
//
// Every failure in the engine surfaces as a typed exception derived from
// LogError.  The caller can catch a specific subclass (e.g. DuplicateVersion)
// or the base LogError when a blanket handler is enough.
//
// Why exceptions instead of error codes?
//   The plan mandates "fail hard" — an exception unwinds immediately,
//   making it impossible for the caller to silently ignore a corrupt or
//   inconsistent state.
// ---------------------------------------------------------------------------

#include <stdexcept>
#include <string>
#include <cstdint>

namespace snapshotdb {

// Base class for all log-engine errors.
class LogError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};

// Raised when a caller tries to write a version that already has a log file.
class DuplicateVersionError : public LogError {
public:
    uint64_t version;
    explicit DuplicateVersionError(uint64_t v)
        : LogError("duplicate version: " + std::to_string(v)), version(v) {}
};

// Raised when a required version file is absent (gap in the log chain).
class MissingVersionError : public LogError {
public:
    uint64_t version;
    explicit MissingVersionError(uint64_t v)
        : LogError("missing version: " + std::to_string(v)), version(v) {}
};

// Raised when a log file exists but contains unparseable JSON.
class CorruptedJsonError : public LogError {
public:
    uint64_t version;
    CorruptedJsonError(uint64_t v, const std::string& detail)
        : LogError("corrupted JSON at version " + std::to_string(v) + ": " + detail),
          version(v) {}
};

// Raised when a transaction's base_version no longer matches the latest
// committed version (another commit snuck in between).
class ConflictError : public LogError {
public:
    int64_t expected;
    int64_t found;
    ConflictError(int64_t exp, int64_t fnd)
        : LogError("conflict: expected base version " + std::to_string(exp) +
                    ", found " + std::to_string(fnd)),
          expected(exp), found(fnd) {}
};

// ---------------------------------------------------------------------------
// Week 4: File-level write conflict (OCC)
//
// Raised when a transaction's read set overlaps with modifications made by
// commits that happened after the transaction's base_version.
//
// This is stricter than ConflictError: ConflictError catches ANY concurrent
// commit (version-level), while WriteConflictError specifically identifies
// WHICH file caused the conflict (file-level).
//
// Example:
//   T1 reads file X at version 3.
//   T2 commits version 4, which removes file X.
//   T1 tries to commit → WriteConflictError("X was modified in version 4").
// ---------------------------------------------------------------------------
class WriteConflictError : public LogError {
public:
    std::string filename;
    uint64_t    conflicting_version;
    WriteConflictError(const std::string& file, uint64_t ver)
        : LogError("write conflict: file '" + file +
                    "' was modified in version " + std::to_string(ver)),
          filename(file), conflicting_version(ver) {}
};

} // namespace snapshotdb
