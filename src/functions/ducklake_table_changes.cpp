#include "duckdb/catalog/default/default_table_functions.hpp"
#include "functions/ducklake_table_functions.hpp"

namespace duckdb {

// clang-format off
static const DefaultTableMacro ducklake_table_macros[] = {
	{DEFAULT_SCHEMA, "ducklake_table_changes", {"catalog", "schema_name", "table_name", "start_snapshot", "end_snapshot", nullptr}, {{nullptr, nullptr}},  R"(
SELECT i._ducklake_change_snapshot_id AS snapshot_id, i._ducklake_change_rowid AS rowid,
       CASE WHEN d._ducklake_change_rowid IS NOT NULL THEN 'update_postimage' ELSE 'insert' END AS change_type,
       i.*
FROM ducklake_table_insertions(catalog, schema_name, table_name, start_snapshot, end_snapshot) i
LEFT JOIN ducklake_table_deletions(catalog, schema_name, table_name, start_snapshot, end_snapshot) d
ON i._ducklake_change_snapshot_id = d._ducklake_change_snapshot_id
   AND i._ducklake_change_rowid = d._ducklake_change_rowid
UNION ALL
SELECT d._ducklake_change_snapshot_id AS snapshot_id, d._ducklake_change_rowid AS rowid,
       CASE WHEN i._ducklake_change_rowid IS NOT NULL THEN 'update_preimage' ELSE 'delete' END AS change_type,
       d.*
FROM ducklake_table_deletions(catalog, schema_name, table_name, start_snapshot, end_snapshot) d
LEFT JOIN ducklake_table_insertions(catalog, schema_name, table_name, start_snapshot, end_snapshot) i
ON d._ducklake_change_snapshot_id = i._ducklake_change_snapshot_id
   AND d._ducklake_change_rowid = i._ducklake_change_rowid
)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

unique_ptr<CreateMacroInfo> DuckLakeTableInsertionsFunction::GetDuckLakeTableChanges() {
	return DefaultTableFunctionGenerator::CreateTableMacroInfo(ducklake_table_macros[0]);
}

} // namespace duckdb
