duckdb_extension_load(sqlite_scanner
        LOAD_TESTS
        GIT_URL https://github.com/duckdb/duckdb-sqlite
        GIT_TAG b55a9492f04cd6fa08741e2211fcca953424a175
        SUBMODULES database-connector
        )
# Static linking on windows does not properly work due to symbol collision
if (NOT WIN32)
    duckdb_extension_statically_link(sqlite_scanner)
endif()
