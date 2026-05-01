cmake_minimum_required(VERSION 3.16)
project(snapshotdb VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# ---------------------------------------------------------------------------
# Third-party: nlohmann/json (header-only JSON library)
# ---------------------------------------------------------------------------
include(FetchContent)

FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
    GIT_SHALLOW    TRUE
)

FetchContent_Declare(
    googletest
    GIT_REPOSITORY https://github.com/google/googletest.git
    GIT_TAG        v1.14.0
    GIT_SHALLOW    TRUE
)

FetchContent_MakeAvailable(json googletest)

# cpp-httplib (HTTP server + optional Azure HTTPS client)
set(HTTPLIB_REQUIRE_OPENSSL OFF CACHE BOOL "" FORCE)
set(HTTPLIB_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
    httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG        v0.18.3
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(httplib)

# ---------------------------------------------------------------------------
# Library: snapshotdb — CORE ENGINE ONLY (include/ + src/)
# ---------------------------------------------------------------------------
add_library(snapshotdb
    src/storage/storage_local.cpp
    src/engine/commit_log.cpp
    src/engine/snapshot.cpp
    src/engine/atomic_writer.cpp
    src/engine/transaction.cpp
    src/engine/reader.cpp
    src/engine/gc.cpp
    src/engine/recovery.cpp
)

target_include_directories(snapshotdb PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries(snapshotdb PUBLIC nlohmann_json::nlohmann_json)

# ---------------------------------------------------------------------------
# Extras: invariant_checker (verification / debug — optional link target)
# ---------------------------------------------------------------------------
add_library(snapshotdb_invariant STATIC
    extras/src/invariant_checker.cpp
)
target_include_directories(snapshotdb_invariant PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/extras/include"
)
target_link_libraries(snapshotdb_invariant PUBLIC snapshotdb)

# ---------------------------------------------------------------------------
# Binary: crash_commit (helper for crash-injection tests — core)
# ---------------------------------------------------------------------------
add_executable(crash_commit src/crash_commit.cpp)
target_link_libraries(crash_commit PRIVATE snapshotdb)

# ---------------------------------------------------------------------------
# Binary: demo — CLI walkthrough (non-core; lives under tools/)
# ---------------------------------------------------------------------------
add_executable(demo tools/demo.cpp)
target_link_libraries(demo PRIVATE snapshotdb_invariant)

# ---------------------------------------------------------------------------
# Binary: snapshotdb_http — REST API (local table directory)
# ---------------------------------------------------------------------------
add_executable(snapshotdb_http tools/http_server.cpp)
target_link_libraries(snapshotdb_http PRIVATE snapshotdb httplib::httplib)

# ---------------------------------------------------------------------------
# Optional: Azure Blob SAS backend (OpenSSL + SSLClient)
# ---------------------------------------------------------------------------
option(SNAPSHOTDB_AZURE "Build Azure SAS StorageBackend (snapshotdb_azure); requires OpenSSL" OFF)
if(SNAPSHOTDB_AZURE)
    find_package(OpenSSL QUIET)
    if(OpenSSL_FOUND)
        add_library(snapshotdb_azure STATIC src/storage/storage_azure.cpp)
        target_include_directories(snapshotdb_azure PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
        target_link_libraries(snapshotdb_azure
            PUBLIC
                snapshotdb
                OpenSSL::SSL
                OpenSSL::Crypto
            PRIVATE
                httplib::httplib
        )
        target_compile_definitions(snapshotdb_azure PRIVATE CPPHTTPLIB_OPENSSL_SUPPORT)
        message(STATUS "snapshotdb_azure: enabled")
    else()
        message(STATUS "snapshotdb_azure: disabled (OpenSSL not found)")
    endif()
endif()

# ---------------------------------------------------------------------------
# Tests (unit tests link optional invariant checker where needed)
# ---------------------------------------------------------------------------
enable_testing()

add_executable(snapshotdb_tests
    tests/test_commit_log.cpp
    tests/test_snapshot.cpp
    tests/test_transaction.cpp
    tests/test_crash.cpp
    tests/test_reader.cpp
    tests/test_occ.cpp
    tests/test_gc.cpp
    tests/test_recovery.cpp
    tests/test_invariants.cpp
)

target_link_libraries(snapshotdb_tests PRIVATE snapshotdb_invariant GTest::gtest_main)
include(GoogleTest)
gtest_discover_tests(snapshotdb_tests)
