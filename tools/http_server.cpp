// ---------------------------------------------------------------------------
// snapshotdb_http — REST actions over one table (local StorageBackend)
//
// Conventions:
//   • Table data files live under keys like data/<name>.parquet (opaque bytes).
//   • Metadata JSON (sidecars, stats) under metadata/*.json alongside Iceberg customs.
//   • Stage commits with PUT .../blob/data/staging/... then POST /v1/commit.
//
// Usage: snapshotdb_http [--listen HOST:PORT] <table_directory>
//    or: SNAPSHOTDB_TABLE=<dir> snapshotdb_http [--listen HOST:PORT]
// ---------------------------------------------------------------------------

#include "snapshotdb/commit_log.h"
#include "snapshotdb/errors.h"
#include "snapshotdb/gc.h"
#include "snapshotdb/reader.h"
#include "snapshotdb/recovery.h"
#include "snapshotdb/storage.h"
#include "snapshotdb/transaction.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <optional>
#include <string>

namespace fs = std::filesystem;

namespace {

bool is_safe_rel_key(const std::string& key) {
    std::string n = snapshotdb::normalize_rel_path(key);
    if (n.empty()) {
        return false;
    }
    std::size_t start = 0;
    while (start <= n.size()) {
        std::size_t slash = n.find('/', start);
        std::string seg =
            slash == std::string::npos ? n.substr(start) : n.substr(start, slash - start);
        if (seg.empty() || seg == "." || seg == "..") {
            return false;
        }
        start = slash == std::string::npos ? n.size() + 1 : slash + 1;
    }
    return true;
}

void json_error(httplib::Response& res, int status, const std::string& msg) {
    res.status = status;
    nlohmann::json j;
    j["error"] = msg;
    res.set_content(j.dump(), "application/json");
}

void handle_log_error(httplib::Response& res, const snapshotdb::LogError& e) {
    if (dynamic_cast<const snapshotdb::ConflictError*>(&e) ||
        dynamic_cast<const snapshotdb::WriteConflictError*>(&e) ||
        dynamic_cast<const snapshotdb::DuplicateVersionError*>(&e)) {
        json_error(res, 409, e.what());
    } else if (dynamic_cast<const snapshotdb::MissingVersionError*>(&e) ||
               dynamic_cast<const snapshotdb::CorruptedJsonError*>(&e)) {
        json_error(res, 400, e.what());
    } else {
        json_error(res, 500, e.what());
    }
}

std::pair<std::string, int> parse_listen(const std::string& s) {
    auto colon = s.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= s.size()) {
        return {"127.0.0.1", 8080};
    }
    std::string host = s.substr(0, colon);
    int         port = std::atoi(s.substr(colon + 1).c_str());
    if (port <= 0 || port > 65535) {
        port = 8080;
    }
    return {host, port};
}

bool parse_args(int argc, char** argv, std::string& out_listen, fs::path& out_table) {
    out_listen = "127.0.0.1:8080";
    out_table.clear();

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--listen" && i + 1 < argc) {
            out_listen = argv[++i];
        } else if (a.rfind("--listen=", 0) == 0) {
            out_listen = a.substr(std::string("--listen=").size());
        } else if (!a.empty() && a[0] != '-') {
            if (!out_table.empty()) {
                std::cerr << "extra positional argument: " << a << '\n';
                return false;
            }
            out_table = fs::path(a);
        } else {
            std::cerr << "unknown option: " << a << '\n';
            return false;
        }
    }

    if (out_table.empty()) {
        const char* env = std::getenv("SNAPSHOTDB_TABLE");
        if (env != nullptr && *env != '\0') {
            out_table = fs::path(env);
        }
    }

    if (out_table.empty()) {
        std::cerr << "usage: snapshotdb_http [--listen HOST:PORT] <table_directory>\n";
        std::cerr << "   or: SNAPSHOTDB_TABLE=<dir> snapshotdb_http\n";
        return false;
    }
    return true;
}

} // namespace

int main(int argc, char** argv) {
    std::string listen;
    fs::path    table_root;
    if (!parse_args(argc, argv, listen, table_root)) {
        return 1;
    }

    table_root = fs::weakly_canonical(fs::absolute(table_root));
    auto storage = snapshotdb::make_local_storage(table_root);

    auto [host, port] = parse_listen(listen);

    httplib::Server sv;

    sv.Get("/health", [](const httplib::Request&, httplib::Response& res) {
        res.set_content(R"({"ok":true})", "application/json");
    });

    sv.Get("/v1/version", [storage](const httplib::Request&, httplib::Response& res) {
        try {
            nlohmann::json j;
            j["latest"] = snapshotdb::latest_version(storage);
            res.set_content(j.dump(), "application/json");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    });

    sv.Get("/v1/snapshot", [storage](const httplib::Request& req, httplib::Response& res) {
        try {
            std::optional<std::uint64_t> at;
            if (req.has_param("at")) {
                at = static_cast<std::uint64_t>(std::stoull(req.get_param_value("at")));
            }
            snapshotdb::SnapshotReader reader(storage, at);
            const auto&                 files = reader.read();
            nlohmann::json              j;
            j["pinned_version"] = reader.pinned_version();
            j["files"]          = nlohmann::json::array();
            for (const auto& f : files) {
                j["files"].push_back(f);
            }
            res.set_content(j.dump(), "application/json");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    });

    sv.Post("/v1/recover", [storage](const httplib::Request&, httplib::Response& res) {
        try {
            auto r = snapshotdb::recover_log(storage);
            nlohmann::json j;
            j["valid_version"]        = r.valid_version;
            j["was_clean"]            = r.was_clean;
            j["cleaned_tmp_files"]    = r.cleaned_tmp_files;
            j["truncated_versions"]   = r.truncated_versions;
            res.set_content(j.dump(), "application/json");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    });

    sv.Post("/v1/validate", [storage](const httplib::Request&, httplib::Response& res) {
        try {
            auto v = snapshotdb::validate_log(storage);
            nlohmann::json j;
            j["valid_through"] = v;
            res.set_content(j.dump(), "application/json");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    });

    sv.Post("/v1/vacuum", [storage](const httplib::Request& req, httplib::Response& res) {
        try {
            auto body = nlohmann::json::parse(req.body.empty() ? "{}" : req.body);
            std::uint64_t retain = body.value("retain_last_n", static_cast<std::uint64_t>(1));
            auto          vr    = snapshotdb::vacuum(storage, retain);
            nlohmann::json j;
            j["retained_from_version"] = vr.retained_from_version;
            j["retained_to_version"]   = vr.retained_to_version;
            j["referenced_files"]      = nlohmann::json::array();
            for (const auto& f : vr.referenced_files) {
                j["referenced_files"].push_back(f);
            }
            j["deleted_files"] = nlohmann::json::array();
            for (const auto& f : vr.deleted_files) {
                j["deleted_files"].push_back(f);
            }
            res.set_content(j.dump(), "application/json");
        } catch (const nlohmann::json::exception&) {
            json_error(res, 400, "invalid JSON body");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    });

    sv.Post("/v1/commit", [storage](const httplib::Request& req, httplib::Response& res) {
        try {
            auto body = nlohmann::json::parse(req.body.empty() ? "{}" : req.body);
            auto adds    = body.value("adds", std::vector<std::string>{});
            auto removes = body.value("removes", std::vector<std::string>{});
            auto reads   = body.value("read_set", std::vector<std::string>{});

            for (const auto& k : adds) {
                if (!is_safe_rel_key(k)) {
                    json_error(res, 400, "invalid key in adds");
                    return;
                }
            }
            for (const auto& k : removes) {
                if (!is_safe_rel_key(k)) {
                    json_error(res, 400, "invalid key in removes");
                    return;
                }
            }
            for (const auto& k : reads) {
                if (!is_safe_rel_key(k)) {
                    json_error(res, 400, "invalid key in read_set");
                    return;
                }
            }

            snapshotdb::Transaction txn(storage);
            for (const auto& f : reads) {
                txn.track_read(f);
            }
            for (const auto& f : adds) {
                txn.add(f);
            }
            for (const auto& f : removes) {
                txn.remove(f);
            }
            std::uint64_t v = txn.commit();
            nlohmann::json j;
            j["version"] = v;
            res.set_content(j.dump(), "application/json");
        } catch (const nlohmann::json::exception&) {
            json_error(res, 400, "invalid JSON body");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    });

    auto blob_get = [storage](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.matches[1];
        if (!is_safe_rel_key(key)) {
            json_error(res, 400, "invalid path");
            return;
        }
        try {
            if (!storage->exists(key)) {
                json_error(res, 404, "not found");
                return;
            }
            auto bytes = storage->read_all_bytes(key);
            const char* ct =
                (key.size() >= 5 && key.compare(key.size() - 5, 5, ".json") == 0)
                    ? "application/json; charset=utf-8"
                    : "application/octet-stream";
            res.set_content(
                std::string(reinterpret_cast<const char*>(bytes.data()), bytes.size()),
                ct
            );
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    };

    auto blob_put = [storage](const httplib::Request& req, httplib::Response& res) {
        std::string key = req.matches[1];
        if (!is_safe_rel_key(key)) {
            json_error(res, 400, "invalid path");
            return;
        }
        try {
            storage->mkdir_parents_for_file(key);
            std::vector<std::uint8_t> body(req.body.begin(), req.body.end());
            storage->write_all_bytes(key, body);
            storage->sync_for_durability(key);
            nlohmann::json j;
            j["ok"]   = true;
            j["key"]  = key;
            j["bytes"] = body.size();
            res.set_content(j.dump(), "application/json");
        } catch (const snapshotdb::LogError& e) {
            handle_log_error(res, e);
        }
    };

    sv.Get(R"(/v1/blob/(.*))", blob_get);
    sv.Put(R"(/v1/blob/(.*))", blob_put);

    std::cerr << "snapshotdb_http table=" << table_root.string() << " listen=" << host << ":" << port
              << "\n";
    if (!sv.listen(host.c_str(), port)) {
        std::cerr << "listen failed\n";
        return 1;
    }
    return 0;
}
