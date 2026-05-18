#include "storage/ducklake_commit_temp_tables.hpp"

#include "duckdb/common/string_util.hpp"

#include <algorithm>

namespace duckdb {

static constexpr const char *TEMP_TABLE_PREFIX = "_dl_commit_";

string DuckLakeCommitTempTables::SanitizeUUID(const string &uuid) {
	auto result = uuid;
	std::replace(result.begin(), result.end(), '-', '_');
	return result;
}

const char *DuckLakeCommitTempTables::KindToString(DuckLakeCommitKind kind) {
	switch (kind) {
	case DuckLakeCommitKind::META:
		return "meta";
	case DuckLakeCommitKind::SCHEMAS:
		return "schemas";
	case DuckLakeCommitKind::TABLES:
		return "tables";
	case DuckLakeCommitKind::VIEWS:
		return "views";
	case DuckLakeCommitKind::COLUMNS:
		return "columns";
	case DuckLakeCommitKind::DATA_FILES:
		return "data_files";
	case DuckLakeCommitKind::FILE_COLUMN_STATS:
		return "file_column_stats";
	case DuckLakeCommitKind::DELETE_FILES:
		return "delete_files";
	case DuckLakeCommitKind::PARTITION_INFO:
		return "partition_info";
	case DuckLakeCommitKind::PARTITION_COLUMN:
		return "partition_column";
	case DuckLakeCommitKind::SORT_INFO:
		return "sort_info";
	case DuckLakeCommitKind::SORT_EXPRESSION:
		return "sort_expression";
	case DuckLakeCommitKind::TAGS:
		return "tags";
	case DuckLakeCommitKind::COLUMN_TAGS:
		return "column_tags";
	case DuckLakeCommitKind::COLUMN_MAPPING:
		return "column_mapping";
	case DuckLakeCommitKind::NAME_MAPPING:
		return "name_mapping";
	case DuckLakeCommitKind::TABLE_STATS:
		return "table_stats";
	case DuckLakeCommitKind::TABLE_COLUMN_STATS:
		return "table_column_stats";
	case DuckLakeCommitKind::INLINED_TABLES:
		return "inlined_tables";
	case DuckLakeCommitKind::SNAPSHOT_CHANGES:
		return "snapshot_changes";
	case DuckLakeCommitKind::DROPPED_SCHEMAS:
		return "dropped_schemas";
	case DuckLakeCommitKind::DROPPED_TABLES:
		return "dropped_tables";
	case DuckLakeCommitKind::DROPPED_VIEWS:
		return "dropped_views";
	case DuckLakeCommitKind::DROPPED_COLUMNS:
		return "dropped_columns";
	case DuckLakeCommitKind::DROPPED_DATA_FILES:
		return "dropped_data_files";
	case DuckLakeCommitKind::OVERWRITTEN_DELETE_FILES:
		return "overwritten_delete_files";
	case DuckLakeCommitKind::FILE_PARTITION_VALUES:
		return "file_partition_values";
	case DuckLakeCommitKind::FILE_VARIANT_STATS:
		return "file_variant_stats";
	default:
		throw InternalException("Unknown DuckLakeCommitKind");
	}
}

string DuckLakeCommitTempTables::TableName(const string &uuid, DuckLakeCommitKind kind) {
	return string(TEMP_TABLE_PREFIX) + SanitizeUUID(uuid) + "_" + KindToString(kind);
}

static const DuckLakeCommitTableSpec COMMIT_TABLE_SPECS[] = {
    {DuckLakeCommitKind::META, "",
     "snapshot_id_was BIGINT NOT NULL, schema_version_was BIGINT NOT NULL, schema_changed BOOLEAN NOT NULL, "
     "has_inlined_flush BOOLEAN NOT NULL, next_catalog_id BIGINT NOT NULL, next_file_id BIGINT NOT NULL, "
     "next_catalog_id_baseline BIGINT NOT NULL, next_file_id_baseline BIGINT NOT NULL",
     ""},
    {DuckLakeCommitKind::SCHEMAS, "ducklake_schema",
     "schema_id BIGINT, schema_uuid UUID, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN",
     "schema_id, schema_uuid, schema_name, path, path_is_relative"},
    {DuckLakeCommitKind::TABLES, "ducklake_table",
     "table_id BIGINT, table_uuid UUID, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN",
     "table_id, table_uuid, schema_id, table_name, path, path_is_relative"},
    {DuckLakeCommitKind::VIEWS, "ducklake_view",
     "view_id BIGINT, view_uuid UUID, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, "
     "column_aliases VARCHAR",
     "view_id, view_uuid, schema_id, view_name, dialect, sql, column_aliases"},
    {DuckLakeCommitKind::COLUMNS, "ducklake_column",
     "column_id BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, "
     "initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, "
     "default_value_type VARCHAR, default_value_dialect VARCHAR",
     "column_id, table_id, column_order, column_name, column_type, initial_default, default_value, "
     "nulls_allowed, parent_column, default_value_type, default_value_dialect"},
    {DuckLakeCommitKind::DATA_FILES, "ducklake_data_file",
     "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, "
     "file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, "
     "partition_id BIGINT, encryption_key VARCHAR, mapping_id BIGINT, partial_max BIGINT",
     "data_file_id, table_id, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, "
     "footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max"},
    {DuckLakeCommitKind::FILE_COLUMN_STATS, "ducklake_file_column_stats",
     "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, "
     "null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR",
     "data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, max_value, "
     "contains_nan, extra_stats"},
    {DuckLakeCommitKind::DELETE_FILES, "ducklake_delete_file",
     "delete_file_id BIGINT, table_id BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, "
     "format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
     "partial_max BIGINT, begin_snapshot_override BIGINT",
     "delete_file_id, table_id, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, "
     "footer_size, encryption_key, partial_max"},
    {DuckLakeCommitKind::PARTITION_INFO, "ducklake_partition_info", "partition_id BIGINT, table_id BIGINT",
     "partition_id, table_id"},
    {DuckLakeCommitKind::PARTITION_COLUMN, "ducklake_partition_column",
     "partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR",
     "partition_id, table_id, partition_key_index, column_id, transform"},
    {DuckLakeCommitKind::SORT_INFO, "ducklake_sort_info", "sort_id BIGINT, table_id BIGINT", "sort_id, table_id"},
    {DuckLakeCommitKind::SORT_EXPRESSION, "ducklake_sort_expression",
     "sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, "
     "sort_direction VARCHAR, null_order VARCHAR",
     "sort_id, table_id, sort_key_index, expression, dialect, sort_direction, null_order"},
    {DuckLakeCommitKind::TAGS, "ducklake_tag", "object_id BIGINT, key VARCHAR, value VARCHAR", "object_id, key, value"},
    {DuckLakeCommitKind::COLUMN_TAGS, "ducklake_column_tag",
     "table_id BIGINT, column_id BIGINT, key VARCHAR, value VARCHAR", "table_id, column_id, key, value"},
    {DuckLakeCommitKind::COLUMN_MAPPING, "ducklake_column_mapping", "mapping_id BIGINT, table_id BIGINT, type VARCHAR",
     "mapping_id, table_id, type"},
    {DuckLakeCommitKind::NAME_MAPPING, "ducklake_name_mapping",
     "mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, "
     "is_partition BOOLEAN",
     "mapping_id, column_id, source_name, target_field_id, parent_column, is_partition"},
    {DuckLakeCommitKind::TABLE_STATS, "ducklake_table_stats",
     "table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT",
     "table_id, record_count, next_row_id, file_size_bytes"},
    {DuckLakeCommitKind::TABLE_COLUMN_STATS, "ducklake_table_column_stats",
     "table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, "
     "max_value VARCHAR, extra_stats VARCHAR",
     "table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats"},
    {DuckLakeCommitKind::INLINED_TABLES, "ducklake_inlined_data_tables",
     "table_id BIGINT, table_name VARCHAR, schema_version BIGINT", "table_id, table_name, schema_version"},
    {DuckLakeCommitKind::SNAPSHOT_CHANGES, "ducklake_snapshot_changes",
     "changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR",
     "changes_made, author, commit_message, commit_extra_info"},
    {DuckLakeCommitKind::DROPPED_SCHEMAS, "", "schema_id BIGINT, renamed BOOLEAN", ""},
    {DuckLakeCommitKind::DROPPED_TABLES, "", "table_id BIGINT, renamed BOOLEAN", ""},
    {DuckLakeCommitKind::DROPPED_VIEWS, "", "view_id BIGINT, renamed BOOLEAN", ""},
    {DuckLakeCommitKind::DROPPED_COLUMNS, "", "column_id BIGINT, table_id BIGINT", ""},
    {DuckLakeCommitKind::DROPPED_DATA_FILES, "", "data_file_id BIGINT", ""},
    {DuckLakeCommitKind::OVERWRITTEN_DELETE_FILES, "", "delete_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN",
     ""},
    {DuckLakeCommitKind::FILE_PARTITION_VALUES, "ducklake_file_partition_value",
     "data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR",
     "data_file_id, table_id, partition_key_index, partition_value"},
    {DuckLakeCommitKind::FILE_VARIANT_STATS, "ducklake_file_variant_stats",
     "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, "
     "column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, "
     "contains_nan BOOLEAN, extra_stats VARCHAR",
     "data_file_id, table_id, column_id, variant_path, shredded_type, column_size_bytes, value_count, null_count, "
     "min_value, max_value, contains_nan, extra_stats"},
};

const DuckLakeCommitTableSpec &GetCommitTableSpec(DuckLakeCommitKind kind) {
	for (auto &spec : COMMIT_TABLE_SPECS) {
		if (spec.kind == kind) {
			return spec;
		}
	}
	throw InternalException("GetCommitTableSpec: unknown DuckLakeCommitKind");
}

string DuckLakeCommitTempTables::RenderDDL(const string &uuid, DuckLakeCommitKind kind) {
	auto &spec = GetCommitTableSpec(kind);
	return "CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}." + TableName(uuid, kind) + " (" + spec.ddl_columns + ");";
}

} // namespace duckdb
