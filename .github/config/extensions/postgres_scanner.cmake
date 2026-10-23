# Note: tests for postgres_scanner are currently not run. All of them need a postgres server running. One test
#       uses a remote rds server but that's not something we want to run here.
if (NOT MINGW AND NOT ${WASM_ENABLED})
    duckdb_extension_load(postgres_scanner
            GIT_URL https://github.com/duckdb/duckdb-postgres
            GIT_TAG e53270e1afdd45d6e4d6b41375ec54f5e17c1fae
            SUBMODULES database-connector
            )
 endif()
