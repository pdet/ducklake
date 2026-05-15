//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_temp_tables.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

//! Kind of temp table used during DuckLake commit.
enum class DuckLakeCommitKind : uint8_t {
	META,
	SCHEMAS,
	TABLES,
	VIEWS,
	COLUMNS,
	DATA_FILES,
	FILE_COLUMN_STATS,
	DELETE_FILES,
	PARTITION_INFO,
	PARTITION_COLUMN,
	SORT_INFO,
	SORT_EXPRESSION,
	TAGS,
	COLUMN_TAGS,
	COLUMN_MAPPING,
	NAME_MAPPING,
	TABLE_STATS,
	INLINED_TABLES,
	SNAPSHOT_CHANGES,
	DROPPED_SCHEMAS,
	DROPPED_TABLES,
	DROPPED_VIEWS,
	DROPPED_COLUMNS,
	DROPPED_DATA_FILES
};

//! Temp tables used to perform DuckLake Commit
struct DuckLakeCommitTempTables {
	//! Sanitize a uuid string (hyphens -> underscores) for use as an identifier.
	static string SanitizeUUID(const string &uuid);
	//! String spelling for a commit kind (used in temp table names).
	static const char *KindToString(DuckLakeCommitKind kind);
	//! Compose the temp table name for the given uuid + kind.
	static string TableName(const string &uuid, DuckLakeCommitKind kind);

	// Per-kind CREATE TEMP TABLE DDL builders.
	static string MetaDDL(const string &uuid);
	static string SchemasDDL(const string &uuid);
	static string TablesDDL(const string &uuid);
	static string ViewsDDL(const string &uuid);
	static string ColumnsDDL(const string &uuid);
	static string DataFilesDDL(const string &uuid);
	static string FileColumnStatsDDL(const string &uuid);
	static string DeleteFilesDDL(const string &uuid);
	static string PartitionInfoDDL(const string &uuid);
	static string PartitionColumnDDL(const string &uuid);
	static string SortInfoDDL(const string &uuid);
	static string SortExpressionDDL(const string &uuid);
	static string TagsDDL(const string &uuid);
	static string ColumnTagsDDL(const string &uuid);
	static string ColumnMappingDDL(const string &uuid);
	static string NameMappingDDL(const string &uuid);
	static string TableStatsDDL(const string &uuid);
	static string InlinedTablesDDL(const string &uuid);
	static string SnapshotChangesDDL(const string &uuid);

	//! Single-row ID-list temp tables for the drop paths.
	static string DroppedSchemaIdsDDL(const string &uuid);
	static string DroppedTableIdsDDL(const string &uuid);
	static string DroppedViewIdsDDL(const string &uuid);
	static string DroppedColumnIdsDDL(const string &uuid);
	static string DroppedDataFileIdsDDL(const string &uuid);
};

} // namespace duckdb
