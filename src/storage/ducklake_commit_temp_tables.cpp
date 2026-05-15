#include "storage/ducklake_commit_temp_tables.hpp"

#include "duckdb/common/string_util.hpp"

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
	default:
		throw InternalException("Unknown DuckLakeCommitKind");
	}
}

string DuckLakeCommitTempTables::TableName(const string &uuid, DuckLakeCommitKind kind) {
	return string(TEMP_TABLE_PREFIX) + SanitizeUUID(uuid) + "_" + KindToString(kind);
}

static string CreateTemp(const string &uuid, DuckLakeCommitKind kind, const string &columns) {
	return "CREATE TEMP TABLE " + DuckLakeCommitTempTables::TableName(uuid, kind) + " (" + columns + ");";
}

string DuckLakeCommitTempTables::MetaDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::META,
	                  "snapshot_id_was BIGINT NOT NULL, "
	                  "schema_version_was BIGINT NOT NULL, "
	                  "schema_changed BOOLEAN NOT NULL, "
	                  "has_inlined_flush BOOLEAN NOT NULL");
}

string DuckLakeCommitTempTables::SchemasDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::SCHEMAS,
	                  "schema_id BIGINT, "
	                  "schema_uuid UUID, "
	                  "schema_name VARCHAR, "
	                  "path VARCHAR, "
	                  "path_is_relative BOOLEAN");
}

string DuckLakeCommitTempTables::TablesDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::TABLES,
	                  "table_id BIGINT, "
	                  "table_uuid UUID, "
	                  "schema_id BIGINT, "
	                  "table_name VARCHAR, "
	                  "path VARCHAR, "
	                  "path_is_relative BOOLEAN");
}

string DuckLakeCommitTempTables::ViewsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::VIEWS,
	                  "view_id BIGINT, "
	                  "view_uuid UUID, "
	                  "schema_id BIGINT, "
	                  "view_name VARCHAR, "
	                  "dialect VARCHAR, "
	                  "sql VARCHAR, "
	                  "column_aliases VARCHAR");
}

string DuckLakeCommitTempTables::ColumnsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::COLUMNS,
	                  "column_id BIGINT, "
	                  "table_id BIGINT, "
	                  "column_order BIGINT, "
	                  "column_name VARCHAR, "
	                  "column_type VARCHAR, "
	                  "initial_default VARCHAR, "
	                  "default_value VARCHAR, "
	                  "nulls_allowed BOOLEAN, "
	                  "parent_column BIGINT, "
	                  "default_value_type VARCHAR, "
	                  "default_value_dialect VARCHAR");
}

string DuckLakeCommitTempTables::DataFilesDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DATA_FILES,
	                  "data_file_id BIGINT, "
	                  "table_id BIGINT, "
	                  "file_order BIGINT, "
	                  "path VARCHAR, "
	                  "path_is_relative BOOLEAN, "
	                  "file_format VARCHAR, "
	                  "record_count BIGINT, "
	                  "file_size_bytes BIGINT, "
	                  "footer_size BIGINT, "
	                  "row_id_start BIGINT, "
	                  "partition_id BIGINT, "
	                  "encryption_key VARCHAR, "
	                  "mapping_id BIGINT, "
	                  "partial_max BIGINT");
}

string DuckLakeCommitTempTables::FileColumnStatsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::FILE_COLUMN_STATS,
	                  "data_file_id BIGINT, "
	                  "table_id BIGINT, "
	                  "column_id BIGINT, "
	                  "column_size_bytes BIGINT, "
	                  "value_count BIGINT, "
	                  "null_count BIGINT, "
	                  "min_value VARCHAR, "
	                  "max_value VARCHAR, "
	                  "contains_nan BOOLEAN, "
	                  "extra_stats VARCHAR");
}

string DuckLakeCommitTempTables::DeleteFilesDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DELETE_FILES,
	                  "delete_file_id BIGINT, "
	                  "table_id BIGINT, "
	                  "data_file_id BIGINT, "
	                  "path VARCHAR, "
	                  "path_is_relative BOOLEAN, "
	                  "format VARCHAR, "
	                  "delete_count BIGINT, "
	                  "file_size_bytes BIGINT, "
	                  "footer_size BIGINT, "
	                  "encryption_key VARCHAR, "
	                  "partial_max BIGINT");
}

string DuckLakeCommitTempTables::PartitionInfoDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::PARTITION_INFO,
	                  "partition_id BIGINT, "
	                  "table_id BIGINT");
}

string DuckLakeCommitTempTables::PartitionColumnDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::PARTITION_COLUMN,
	                  "partition_id BIGINT, "
	                  "table_id BIGINT, "
	                  "partition_key_index BIGINT, "
	                  "column_id BIGINT, "
	                  "transform VARCHAR");
}

string DuckLakeCommitTempTables::SortInfoDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::SORT_INFO,
	                  "sort_id BIGINT, "
	                  "table_id BIGINT");
}

string DuckLakeCommitTempTables::SortExpressionDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::SORT_EXPRESSION,
	                  "sort_id BIGINT, "
	                  "table_id BIGINT, "
	                  "sort_key_index BIGINT, "
	                  "expression VARCHAR, "
	                  "dialect VARCHAR, "
	                  "sort_direction VARCHAR, "
	                  "null_order VARCHAR");
}

string DuckLakeCommitTempTables::TagsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::TAGS,
	                  "object_id BIGINT, "
	                  "key VARCHAR, "
	                  "value VARCHAR");
}

string DuckLakeCommitTempTables::ColumnTagsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::COLUMN_TAGS,
	                  "table_id BIGINT, "
	                  "column_id BIGINT, "
	                  "key VARCHAR, "
	                  "value VARCHAR");
}

string DuckLakeCommitTempTables::ColumnMappingDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::COLUMN_MAPPING,
	                  "mapping_id BIGINT, "
	                  "table_id BIGINT, "
	                  "type VARCHAR");
}

string DuckLakeCommitTempTables::NameMappingDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::NAME_MAPPING,
	                  "mapping_id BIGINT, "
	                  "column_id BIGINT, "
	                  "source_name VARCHAR, "
	                  "target_field_id BIGINT, "
	                  "parent_column BIGINT, "
	                  "is_partition BOOLEAN");
}

string DuckLakeCommitTempTables::TableStatsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::TABLE_STATS,
	                  "table_id BIGINT, "
	                  "record_count BIGINT, "
	                  "next_row_id BIGINT, "
	                  "file_size_bytes BIGINT");
}

string DuckLakeCommitTempTables::InlinedTablesDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::INLINED_TABLES,
	                  "table_id BIGINT, "
	                  "table_name VARCHAR, "
	                  "schema_version BIGINT");
}

string DuckLakeCommitTempTables::SnapshotChangesDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::SNAPSHOT_CHANGES,
	                  "changes_made VARCHAR, "
	                  "author VARCHAR, "
	                  "commit_message VARCHAR, "
	                  "commit_extra_info VARCHAR");
}

string DuckLakeCommitTempTables::DroppedSchemaIdsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DROPPED_SCHEMAS, "schema_id BIGINT, renamed BOOLEAN");
}

string DuckLakeCommitTempTables::DroppedTableIdsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DROPPED_TABLES, "table_id BIGINT, renamed BOOLEAN");
}

string DuckLakeCommitTempTables::DroppedViewIdsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DROPPED_VIEWS, "view_id BIGINT, renamed BOOLEAN");
}

string DuckLakeCommitTempTables::DroppedColumnIdsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DROPPED_COLUMNS, "column_id BIGINT, table_id BIGINT");
}

string DuckLakeCommitTempTables::DroppedDataFileIdsDDL(const string &uuid) {
	return CreateTemp(uuid, DuckLakeCommitKind::DROPPED_DATA_FILES, "data_file_id BIGINT");
}

} // namespace duckdb
