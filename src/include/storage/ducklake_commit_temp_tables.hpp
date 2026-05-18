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
	TABLE_COLUMN_STATS,
	INLINED_TABLES,
	SNAPSHOT_CHANGES,
	DROPPED_SCHEMAS,
	DROPPED_TABLES,
	DROPPED_VIEWS,
	DROPPED_COLUMNS,
	DROPPED_DATA_FILES,
	OVERWRITTEN_DELETE_FILES,
	FILE_PARTITION_VALUES,
	FILE_VARIANT_STATS
};

struct DuckLakeCommitTableSpec {
	DuckLakeCommitKind kind;
	const char *real_table;
	const char *ddl_columns;
	const char *insert_columns;
};

const DuckLakeCommitTableSpec &GetCommitTableSpec(DuckLakeCommitKind kind);

struct DuckLakeCommitTempTables {
	static string SanitizeUUID(const string &uuid);
	static const char *KindToString(DuckLakeCommitKind kind);
	static string TableName(const string &uuid, DuckLakeCommitKind kind);
	static string RenderDDL(const string &uuid, DuckLakeCommitKind kind);
};

} // namespace duckdb
