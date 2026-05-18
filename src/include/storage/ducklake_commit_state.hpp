//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/index.hpp"
#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/reference_map.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;
class DuckLakeViewEntry;

struct NewDataInfo {
	vector<DuckLakeFileInfo> new_files;
	vector<DuckLakeInlinedDataInfo> new_inlined_data;
};

struct TransactionChangeInformation {
	case_insensitive_set_t created_schemas;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> dropped_schemas;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_tables;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_scalar_macros;
	case_insensitive_map_t<reference_set_t<CatalogEntry>> created_table_macros;

	set<TableIndex> altered_tables;
	set<TableIndex> altered_tables_with_schema_version_changes;
	set<TableIndex> altered_views;
	set<TableIndex> dropped_tables;
	set<TableIndex> dropped_views;
	set<MacroIndex> dropped_scalar_macros;
	set<MacroIndex> dropped_table_macros;
	set<TableIndex> tables_inserted_into;
	set<TableIndex> tables_deleted_from;
	set<TableIndex> tables_inserted_inlined;
	set<TableIndex> tables_deleted_inlined;
	set<TableIndex> tables_flushed_inlined;
	set<TableIndex> tables_compacted;
	set<TableIndex> tables_merge_adjacent;
	set<TableIndex> tables_rewrite_delete;
};

struct DuckLakeCommitState {
	explicit DuckLakeCommitState(DuckLakeSnapshot &snapshot) : commit_snapshot(snapshot) {
	}

	DuckLakeSnapshot &commit_snapshot;
	map<SchemaIndex, SchemaIndex> committed_schemas;
	map<TableIndex, TableIndex> committed_tables;
	map<idx_t, idx_t> committed_partition_ids;
	map<MappingIndex, MappingIndex> committed_mapping_indexes;
	map<TableIndex, vector<DuckLakeDeleteFile>> local_delete_files;

	void RemapIdentifier(SchemaIndex &schema_id) const {
		auto entry = committed_schemas.find(schema_id);
		if (entry != committed_schemas.end()) {
			schema_id = entry->second;
		}
	}
	void RemapIdentifier(TableIndex &table_id) const {
		auto entry = committed_tables.find(table_id);
		if (entry != committed_tables.end()) {
			table_id = entry->second;
		}
	}
	void RemapPartitionId(optional_idx &partition_id) const {
		if (!partition_id.IsValid()) {
			return;
		}
		auto entry = committed_partition_ids.find(partition_id.GetIndex());
		if (entry != committed_partition_ids.end()) {
			partition_id = entry->second;
		}
	}
	void RemapMappingIndex(MappingIndex &table_id) const {
		auto entry = committed_mapping_indexes.find(table_id);
		if (entry != committed_mapping_indexes.end()) {
			table_id = entry->second;
		}
	}

	SchemaIndex GetSchemaId(DuckLakeSchemaEntry &schema) const;
	TableIndex GetTableId(DuckLakeTableEntry &table) const;
	TableIndex GetTableId(TableIndex table_id) const {
		RemapIdentifier(table_id);
		return table_id;
	}
	TableIndex GetViewId(DuckLakeViewEntry &view) const;
};

} // namespace duckdb
