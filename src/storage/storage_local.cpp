// ---------------------------------------------------------------------------
// storage_local.cp — LocalFilesystemStorage
// ---------------------------------------------------------------------------

#include "snapshotdb/storage/backend.h"
#include "snapshotdb/common/errors.h"

#include <fstream>
#include <sstream>
#include <algorithm>

#ifdef _WIN32
#include <io.h>
#include <fcntl.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace snapshotdb {

std::string normalize_rel_path(std::string_view rel) {
    std::string out;
    out.reserve(rel.size());
    bool start = true;
    for (char c : rel) {
        if (c == '\\') c = '/';
        if (c == '/' && start) continue;
        start = false;
        out.push_back(c);
    }
    while (!out.empty() && out.front() == '/') out.erase(out.begin());
    while (!out.empty() && out.back() == '/') out.pop_back();
    return out;
}

static void replace_in_place(std::string& s, char from, char to) {
    for (char& c : s) if (c == from) c = to;
}

LocalFilesystemStorage::LocalFilesystemStorage(fs::path root_directory)
    : root_(std::move(root_directory))
{
    root_ = fs::weakly_canonical(fs::absolute(root_));
}

fs::path LocalFilesystemStorage::abs(const std::string& rel) const {
    std::string n = normalize_rel_path(rel);
    replace_in_place(n, '/', fs::path::preferred_separator);
    fs::path p = root_;
    if (!n.empty()) p /= n;
    return p;
}

bool LocalFilesystemStorage::exists(const std::string& rel_path) {
    return fs::exists(abs(rel_path));
}

std::string LocalFilesystemStorage::read_all_text(const std::string& rel_path) {
    fs::path p = abs(rel_path);
    std::ifstream in(p);
    if (!in) throw LogError("read failed: " + p.string());
    std::stringstream buf;
    buf << in.rdbuf();
    return buf.str();
}

void LocalFilesystemStorage::write_all_text(const std::string& rel_path, const std::string& utf8) {
    fs::path p = abs(rel_path);
    mkdir_parents_for_file(rel_path);
    std::ofstream out(p, std::ios::trunc | std::ios::binary);
    if (!out) throw LogError("write failed: " + p.string());
    out.write(utf8.data(), static_cast<std::streamsize>(utf8.size()));
}

std::vector<std::uint8_t> LocalFilesystemStorage::read_all_bytes(const std::string& rel_path) {
    fs::path p = abs(rel_path);
    std::ifstream in(p, std::ios::binary);
    if (!in) throw LogError("read bytes failed: " + p.string());
    return std::vector<std::uint8_t>(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
}

void LocalFilesystemStorage::write_all_bytes(const std::string& rel_path, const std::vector<std::uint8_t>& data) {
    fs::path p = abs(rel_path);
    mkdir_parents_for_file(rel_path);
    std::ofstream out(p, std::ios::trunc | std::ios::binary);
    if (!out) throw LogError("write bytes failed: " + p.string());
    if (!data.empty()) {
        out.write(reinterpret_cast<const char*>(data.data()), static_cast<std::streamsize>(data.size()));
    }
}

void LocalFilesystemStorage::mkdir_parents_for_file(const std::string& rel_path) {
    fs::path p = abs(rel_path);
    if (p.has_parent_path()) {
        fs::create_directories(p.parent_path());
    }
}

void LocalFilesystemStorage::remove_file(const std::string& rel_path) {
    std::error_code ec;
    fs::remove(abs(rel_path), ec);
}

void LocalFilesystemStorage::rename_no_overwrite(const std::string& from_rel, const std::string& to_rel) {
    fs::path a = abs(from_rel);
    fs::path b = abs(to_rel);
    if (!fs::exists(a)) throw LogError("rename: missing source " + a.string());
    if (fs::exists(b)) throw LogError("rename: destination exists " + b.string());
    if (b.has_parent_path()) fs::create_directories(b.parent_path());
    std::error_code ec;
    fs::rename(a, b, ec);
    if (ec) throw LogError("rename failed: " + ec.message());
}

void LocalFilesystemStorage::sync_for_durability(const std::string& rel_path) {
    fs::path p = abs(rel_path);
#ifdef _WIN32
    int fd = _open(p.string().c_str(), _O_RDONLY);
    if (fd >= 0) { _commit(fd); _close(fd); }
#else
    int fd = open(p.c_str(), O_RDONLY);
    if (fd >= 0) { ::fsync(fd); ::close(fd); }
#endif
}

std::vector<std::string> LocalFilesystemStorage::list_dir_filenames(const std::string& rel_dir) {
    std::vector<std::string> names;
    fs::path d = abs(rel_dir);
    if (!fs::exists(d)) return names;
    for (const auto& e : fs::directory_iterator(d)) {
        if (!e.is_regular_file()) continue;
        names.push_back(e.path().filename().string());
    }
    std::sort(names.begin(), names.end());
    return names;
}

void LocalFilesystemStorage::remove_tree(const std::string& rel_dir_prefix) {
    fs::path p = abs(rel_dir_prefix);
    std::error_code ec;
    fs::remove_all(p, ec);
}

} // namespace snapshotdb
