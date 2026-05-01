// ---------------------------------------------------------------------------
// storage_azure.cpp — Azure Blob REST (SAS) StorageBackend
// ---------------------------------------------------------------------------

// CPPHTTPLIB_OPENSSL_SUPPORT may be set by CMake for snapshotdb_azure target
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
#define CPPHTTPLIB_OPENSSL_SUPPORT
#endif
#include <httplib.h>

#include "snapshotdb/storage/azure.h"
#include "snapshotdb/common/errors.h"

#include <algorithm>

namespace snapshotdb {

namespace {

std::string normalize_sas(std::string sas) {
    while (!sas.empty() && sas[0] == '?') {
        sas.erase(sas.begin());
    }
    if (!sas.empty()) {
        sas = "?" + sas;
    }
    return sas;
}

static std::string pct_encode_segment(std::string_view seg) {
    auto hex = [](unsigned char c) {
        static const char* d = "0123456789ABCDEF";
        std::string s(1, '%');
        s.push_back(d[c >> 4u]);
        s.push_back(d[c & 0xfu]);
        return s;
    };
    std::string out;
    out.reserve(seg.size());
    for (unsigned char c : seg) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
            c == '-' || c == '_' || c == '.' || c == '~') {
            out.push_back(static_cast<char>(c));
        } else {
            out += hex(c);
        }
    }
    return out;
}

static std::string blob_object_path(const std::string& container, const std::string& rel_key) {
    std::string n = normalize_rel_path(rel_key);
    std::string path = "/" + container;
    std::size_t start = 0;
    while (start <= n.size()) {
        std::size_t slash = n.find('/', start);
        std::string seg =
            slash == std::string::npos ? n.substr(start) : n.substr(start, slash - start);
        start = slash == std::string::npos ? n.size() + 1 : slash + 1;
        if (!seg.empty()) {
            path.push_back('/');
            path += pct_encode_segment(seg);
        }
    }
    return path;
}

static void require_res(const httplib::Result& res, const char* ctx) {
    if (!res) {
        throw LogError(std::string(ctx) + ": no response");
    }
    if (res->status < 200 || res->status >= 300) {
        throw LogError(std::string(ctx) + ": HTTP " + std::to_string(res->status));
    }
}

static std::vector<std::string> parse_list_names(const std::string& xml) {
    std::vector<std::string> names;
    const std::string open  = "<Name>";
    const std::string close = "</Name>";
    std::size_t pos = 0;
    while (true) {
        std::size_t a = xml.find(open, pos);
        if (a == std::string::npos) break;
        a += open.size();
        std::size_t b = xml.find(close, a);
        if (b == std::string::npos) break;
        names.emplace_back(xml.substr(a, b - a));
        pos = b + close.size();
    }
    return names;
}

static std::string azure_list_url(
    const std::string& container,
    const std::string& sas,
    const std::string& prefix_filter
) {
    std::string path = "/" + container + sas + "&restype=container&comp=list";
    if (!prefix_filter.empty()) {
        path += "&prefix=";
        path += pct_encode_segment(prefix_filter);
    }
    return path;
}

class AzureSasBlobStorage final : public StorageBackend {
public:
    AzureSasBlobStorage(std::string host, std::string container, std::string sas)
        : host_(std::move(host))
        , container_(std::move(container))
        , sas_(normalize_sas(std::move(sas)))
    {
        cli_ = std::make_unique<httplib::SSLClient>(host_, 443);
        cli_->set_default_headers({{"x-ms-version", "2020-10-02"}});
    }

    bool exists(const std::string& rel_path) override {
        std::string path = blob_object_path(container_, rel_path) + sas_;
        auto         res = cli_->Head(path.c_str());
        if (!res) return false;
        if (res->status == 404) return false;
        if (res->status >= 200 && res->status < 300) return true;
        throw LogError("azure exists: HTTP " + std::to_string(res->status));
    }

    std::string read_all_text(const std::string& rel_path) override {
        std::string path = blob_object_path(container_, rel_path) + sas_;
        auto        res  = cli_->Get(path.c_str());
        require_res(res, "azure read text");
        return res->body;
    }

    void write_all_text(const std::string& rel_path, const std::string& utf8) override {
        write_all_bytes(rel_path, std::vector<std::uint8_t>(utf8.begin(), utf8.end()));
    }

    std::vector<std::uint8_t> read_all_bytes(const std::string& rel_path) override {
        std::string path = blob_object_path(container_, rel_path) + sas_;
        auto        res  = cli_->Get(path.c_str());
        require_res(res, "azure read bytes");
        return std::vector<std::uint8_t>(res->body.begin(), res->body.end());
    }

    void write_all_bytes(const std::string& rel_path, const std::vector<std::uint8_t>& data) override {
        std::string      path = blob_object_path(container_, rel_path) + sas_;
        httplib::Headers hdrs;
        std::string      body(reinterpret_cast<const char*>(data.data()), data.size());
        auto             res = cli_->Put(path.c_str(), hdrs, body, "application/octet-stream");
        require_res(res, "azure write bytes");
    }

    void mkdir_parents_for_file(const std::string&) override {}

    void remove_file(const std::string& rel_path) override {
        std::string path = blob_object_path(container_, rel_path) + sas_;
        auto        res  = cli_->Delete(path.c_str());
        if (!res) {
            throw LogError("azure delete: no response");
        }
        if (res->status == 404) {
            return;
        }
        if (res->status < 200 || res->status >= 300) {
            throw LogError("azure delete: HTTP " + std::to_string(res->status));
        }
    }

    void rename_no_overwrite(const std::string& from_rel, const std::string& to_rel) override {
        if (exists(to_rel)) {
            throw LogError("azure rename: destination exists");
        }
        auto bytes = read_all_bytes(from_rel);
        write_all_bytes(to_rel, bytes);
        remove_file(from_rel);
    }

    void sync_for_durability(const std::string&) override {}

    std::vector<std::string> list_dir_filenames(const std::string& rel_dir) override {
        std::string prefix = normalize_rel_path(rel_dir);
        if (!prefix.empty()) {
            prefix.push_back('/');
        }

        auto res = cli_->Get(azure_list_url(container_, sas_, prefix).c_str());
        require_res(res, "azure list");

        std::string pfx = normalize_rel_path(rel_dir);
        if (!pfx.empty()) {
            pfx.push_back('/');
        }

        std::vector<std::string> out;
        for (const auto& full : parse_list_names(res->body)) {
            if (pfx.empty()) {
                std::size_t slash = full.find('/');
                if (slash == std::string::npos) {
                    out.push_back(full);
                }
            } else if (full.size() > pfx.size() && full.compare(0, pfx.size(), pfx) == 0) {
                std::string tail = full.substr(pfx.size());
                if (!tail.empty() && tail.find('/') == std::string::npos) {
                    out.push_back(tail);
                }
            }
        }
        std::sort(out.begin(), out.end());
        return out;
    }

    void remove_tree(const std::string& rel_dir_prefix) override {
        std::string pfx = normalize_rel_path(rel_dir_prefix);
        if (!pfx.empty()) {
            pfx.push_back('/');
        }

        auto res = cli_->Get(azure_list_url(container_, sas_, pfx).c_str());
        require_res(res, "azure list (remove_tree)");

        for (const auto& name : parse_list_names(res->body)) {
            remove_file(name);
        }
    }

private:
    std::string                         host_;
    std::string                         container_;
    std::string                         sas_;
    std::unique_ptr<httplib::SSLClient> cli_;
};

} // namespace

std::shared_ptr<StorageBackend> make_azure_sas_storage(
    std::string host,
    std::string container,
    std::string sas_query
) {
    return std::make_shared<AzureSasBlobStorage>(
        std::move(host),
        std::move(container),
        std::move(sas_query)
    );
}

} // namespace snapshotdb
