#pragma once

// ---------------------------------------------------------------------------
// storage_azure.h — Azure Blob Storage via HTTPS + container SAS
//
// Limitations vs local disk:
//   • rename_no_overwrite uses download + upload + delete (not server-side copy).
//   • No true POSIX durability; sync_for_durability is a no-op.
// Build: CMake option SNAPSHOTDB_AZURE + OpenSSL + CPPHTTPLIB_OPENSSL_SUPPORT
// ---------------------------------------------------------------------------

#include <memory>
#include <string>

#include "snapshotdb/storage/backend.h"

namespace snapshotdb {

// host: "myaccount.blob.core.windows.net" (no scheme)
// container: container name
// sas_query: leading "?" optional; must grant read/write/list/delete as needed
std::shared_ptr<StorageBackend> make_azure_sas_storage(
    std::string host,
    std::string container,
    std::string sas_query
);

} // namespace snapshotdb
