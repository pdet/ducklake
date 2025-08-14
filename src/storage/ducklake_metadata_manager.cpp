#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/common/types/blob.hpp"
#include "storage/ducklake_catalog.hpp"
#include "common/ducklake_types.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

DuckLakeMetadataManager::DuckLakeMetadataManager(DuckLakeTransaction &transaction) : transaction(transaction) {
}

DuckLakeMetadataManager::~DuckLakeMetadataManager() {
}
optional_ptr<AttachedDatabase> GetDatabase(ClientContext &context, const string &name);

unique_ptr<DuckLakeMetadataManager> DuckLakeMetadataManager::Create(DuckLakeTransaction &transaction) {
	auto &catalog = transaction.GetCatalog();
	auto catalog_type = catalog.MetadataType();
	if (catalog_type == "postgres" || catalog_type == "postgres_scanner") {
		return make_uniq<PostgresMetadataManager>(transaction);
	}
	return make_uniq<DuckLakeMetadataManager>(transaction);
}

DuckLakeMetadataManager &DuckLakeMetadataManager::Get(DuckLakeTransaction &transaction) {
	return transaction.GetMetadataManager();
}

FileSystem &DuckLakeMetadataManager::GetFileSystem() {
	return FileSystem::GetFileSystem(transaction.GetCatalog().GetDatabase());
}

void DuckLakeMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	string initialize_query;
	if (has_explicit_schema) {
		// if the schema is user provided create it
		initialize_query += "CREATE SCHEMA IF NOT EXISTS {METADATA_CATALOG};\n";
	}
	// we default to false unless explicitly specified otherwise
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &base_data_path = ducklake_catalog.DataPath();
	string data_path = StorePath(base_data_path);
	string encryption_str = encryption == DuckLakeEncryption::ENCRYPTED ? "true" : "false";
	initialize_query += StringUtil::Format(R"(
CREATE TABLE {METADATA_CATALOG}.ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
CREATE TABLE {METADATA_CATALOG}.ducklake_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
CREATE TABLE {METADATA_CATALOG}.ducklake_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, partial_file_info VARCHAR, mapping_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_file_column_statistics(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN);
CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ);
CREATE TABLE {METADATA_CATALOG}.ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN);
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES (0, NOW(), 0, 1, 0);
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES (0, 'created_schema:"main"',  NULL, NULL, NULL);
INSERT INTO {METADATA_CATALOG}.ducklake_metadata (key, value) VALUES ('version', '0.3-dev1'), ('created_by', 'DuckDB %s'), ('data_path', %s), ('encrypted', '%s');
INSERT INTO {METADATA_CATALOG}.ducklake_schema VALUES (0, UUID(), 0, NULL, 'main', 'main/', true);
	)",
	                                       DuckDB::SourceID(), SQLString(data_path), encryption_str);
	// TODO: add
	//	ducklake_sorting_info
	//	ducklake_sorting_column_info
	//	ducklake_macro
	auto result = transaction.Query(initialize_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to initialize DuckLake:");
	}
}

void DuckLakeMetadataManager::MigrateV01() {
	string migrate_query = R"(
ALTER TABLE {METADATA_CATALOG}.ducklake_schema ADD COLUMN path VARCHAR DEFAULT '';
ALTER TABLE {METADATA_CATALOG}.ducklake_schema ADD COLUMN path_is_relative BOOLEAN DEFAULT TRUE;
ALTER TABLE {METADATA_CATALOG}.ducklake_table ADD COLUMN path VARCHAR DEFAULT '';
ALTER TABLE {METADATA_CATALOG}.ducklake_table ADD COLUMN path_is_relative BOOLEAN DEFAULT TRUE;
ALTER TABLE {METADATA_CATALOG}.ducklake_metadata ADD COLUMN scope VARCHAR;
ALTER TABLE {METADATA_CATALOG}.ducklake_metadata ADD COLUMN scope_id BIGINT;
ALTER TABLE {METADATA_CATALOG}.ducklake_data_file ADD COLUMN mapping_id BIGINT;
CREATE TABLE {METADATA_CATALOG}.ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT);
UPDATE {METADATA_CATALOG}.ducklake_partition_column SET column_id = (SELECT LIST(column_id ORDER BY column_order) FROM {METADATA_CATALOG}.ducklake_column WHERE table_id = ducklake_partition_column.table_id AND parent_column IS NULL AND end_snapshot IS NULL)[ducklake_partition_column.column_id + 1];
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.2' WHERE key = 'version';
	)";
	auto result = transaction.Query(migrate_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to migrate DuckLake from v0.1 to v0.2:");
	}
}

void DuckLakeMetadataManager::MigrateV02() {
	string migrate_query = R"(
ALTER TABLE {METADATA_CATALOG}.ducklake_name_mapping ADD COLUMN is_partition BOOLEAN DEFAULT false;
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot_changes ADD COLUMN author VARCHAR DEFAULT NULL;
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot_changes ADD COLUMN commit_message VARCHAR DEFAULT NULL;
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot_changes ADD COLUMN commit_extra_info VARCHAR DEFAULT NULL;
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.3-dev1' WHERE key = 'version';
	)";
	auto result = transaction.Query(migrate_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to migrate DuckLake from v0.2 to v0.3:");
	}
}

DuckLakeMetadata DuckLakeMetadataManager::LoadDuckLake() {
	auto result = transaction.Query(R"(
SELECT key, value, scope, scope_id FROM {METADATA_CATALOG}.ducklake_metadata
)");
	if (result->HasError()) {
		// we might be loading from a v0.1 database - if so we don't have scope yet
		result = transaction.Query(R"(
SELECT key, value FROM {METADATA_CATALOG}.ducklake_metadata
)");
		if (result->HasError()) {
			auto &error_obj = result->GetErrorObject();
			error_obj.Throw("Failed to load existing DuckLake: ");
		}
	}
	DuckLakeMetadata metadata;
	for (auto &row : *result) {
		DuckLakeTag tag;
		tag.key = row.GetValue<string>(0);
		tag.value = row.GetValue<string>(1);
		if (result->ColumnCount() == 2 || row.IsNull(2)) {
			// scope is NULL: global tag
			// global tag
			metadata.tags.push_back(std::move(tag));
			continue;
		}
		auto scope = row.GetValue<string>(2);
		if (scope == "schema") {
			// schema-level setting
			DuckLakeSchemaSetting schema_setting;
			schema_setting.schema_id = SchemaIndex(row.GetValue<idx_t>(3));
			schema_setting.tag = std::move(tag);
			metadata.schema_settings.push_back(std::move(schema_setting));
			continue;
		}
		if (scope == "table") {
			// table-level setting
			DuckLakeTableSetting table_setting;
			table_setting.table_id = TableIndex(row.GetValue<idx_t>(3));
			table_setting.tag = std::move(tag);
			metadata.table_settings.push_back(std::move(table_setting));
			continue;
		}
		throw InvalidInputException("Unsupported setting scope %s - only schema/table are supported", scope);
	}
	return metadata;
}

bool AddChildColumn(vector<DuckLakeColumnInfo> &columns, FieldIndex parent_id, DuckLakeColumnInfo &column_info) {
	for (auto &col : columns) {
		if (col.id == parent_id) {
			col.children.push_back(std::move(column_info));
			return true;
		}
		if (AddChildColumn(col.children, parent_id, column_info)) {
			return true;
		}
	}
	return false;
}

vector<DuckLakeTag> LoadTags(const Value &tag_map) {
	vector<DuckLakeTag> result;
	for (auto &tag : ListValue::GetChildren(tag_map)) {
		auto &struct_children = StructValue::GetChildren(tag);
		if (struct_children[1].IsNull()) {
			continue;
		}
		DuckLakeTag tag_info;
		tag_info.key = struct_children[0].ToString();
		tag_info.value = struct_children[1].ToString();
		result.push_back(std::move(tag_info));
	}
	return result;
}

vector<DuckLakeInlinedTableInfo> LoadInlinedDataTables(const Value &list) {
	vector<DuckLakeInlinedTableInfo> result;
	for (auto &val : ListValue::GetChildren(list)) {
		auto &struct_children = StructValue::GetChildren(val);
		DuckLakeInlinedTableInfo inlined_data_table;
		inlined_data_table.table_name = StringValue::Get(struct_children[0]);
		inlined_data_table.schema_version = struct_children[1].GetValue<idx_t>();
		result.push_back(std::move(inlined_data_table));
	}
	return result;
}

DuckLakeCatalogInfo DuckLakeMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &base_data_path = ducklake_catalog.DataPath();
	DuckLakeCatalogInfo catalog;
	// load the schema information
	auto result = transaction.Query(snapshot, R"(
SELECT schema_id, schema_uuid::VARCHAR, schema_name, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get schema information from DuckLake: ");
	}
	map<SchemaIndex, idx_t> schema_map;
	for (auto &row : *result) {
		DuckLakeSchemaInfo schema;
		schema.id = SchemaIndex(row.GetValue<uint64_t>(0));
		schema.uuid = row.GetValue<string>(1);
		schema.name = row.GetValue<string>(2);
		if (row.IsNull(3)) {
			// no path provided - fallback to base data path
			schema.path = base_data_path;
		} else {
			// path is provided - load it
			DuckLakePath path;
			path.path = row.template GetValue<string>(3);
			path.path_is_relative = row.template GetValue<bool>(4);

			schema.path = FromRelativePath(path);
		}
		schema_map[schema.id] = catalog.schemas.size();
		catalog.schemas.push_back(std::move(schema));
	}

	// load the table information
	result = transaction.Query(snapshot, R"(
SELECT schema_id, tbl.table_id, table_uuid::VARCHAR, table_name,
	(
		SELECT LIST({'key': key, 'value': value})
		FROM {METADATA_CATALOG}.ducklake_tag tag
		WHERE object_id=table_id AND
		      {SNAPSHOT_ID} >= tag.begin_snapshot AND ({SNAPSHOT_ID} < tag.end_snapshot OR tag.end_snapshot IS NULL)
	) AS tag,
	(
		SELECT LIST({'name': table_name, 'schema_version': schema_version})
		FROM {METADATA_CATALOG}.ducklake_inlined_data_tables inlined_data_tables
		WHERE inlined_data_tables.table_id = tbl.table_id
	) AS inlined_data_tables,
	path, path_is_relative,
	col.column_id, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column,
	(
		SELECT LIST({'key': key, 'value': value})
		FROM {METADATA_CATALOG}.ducklake_column_tag col_tag
		WHERE col_tag.table_id=tbl.table_id AND col_tag.column_id=col.column_id AND
		      {SNAPSHOT_ID} >= col_tag.begin_snapshot AND ({SNAPSHOT_ID} < col_tag.end_snapshot OR col_tag.end_snapshot IS NULL)
	) AS column_tags
FROM {METADATA_CATALOG}.ducklake_table tbl
LEFT JOIN {METADATA_CATALOG}.ducklake_column col USING (table_id)
WHERE {SNAPSHOT_ID} >= tbl.begin_snapshot AND ({SNAPSHOT_ID} < tbl.end_snapshot OR tbl.end_snapshot IS NULL)
  AND (({SNAPSHOT_ID} >= col.begin_snapshot AND ({SNAPSHOT_ID} < col.end_snapshot OR col.end_snapshot IS NULL)) OR column_id IS NULL)
ORDER BY table_id, parent_column NULLS FIRST, column_order
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table information from DuckLake: ");
	}
	const idx_t COLUMN_INDEX_START = 8;
	auto &tables = catalog.tables;
	for (auto &row : *result) {
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		// check if this column belongs to the current table or not
		if (tables.empty() || tables.back().id != table_id) {
			// new table
			DuckLakeTableInfo table_info;
			table_info.id = table_id;
			table_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(0));
			table_info.uuid = row.GetValue<string>(2);
			table_info.name = row.GetValue<string>(3);
			if (!row.IsNull(4)) {
				auto tags = row.GetValue<Value>(4);
				table_info.tags = LoadTags(tags);
			}
			if (!row.IsNull(5)) {
				auto inlined_data_tables = row.GetValue<Value>(5);
				table_info.inlined_data_tables = LoadInlinedDataTables(inlined_data_tables);
			}
			// find the schema
			auto schema_entry = schema_map.find(table_info.schema_id);
			if (schema_entry == schema_map.end()) {
				throw InvalidInputException(
				    "Failed to load DuckLake - table with id %d references schema id %d that does not exist",
				    table_info.id.index, table_info.schema_id.index);
			}
			auto &schema = catalog.schemas[schema_entry->second];
			if (row.IsNull(6)) {
				// no path provided - fallback to schema path
				table_info.path = schema.path;
			} else {
				// path is provided - load it
				DuckLakePath path;
				path.path = row.template GetValue<string>(6);
				path.path_is_relative = row.template GetValue<bool>(7);

				table_info.path = FromRelativePath(path, schema.path);
			}
			tables.push_back(std::move(table_info));
		}
		auto &table_entry = tables.back();
		if (row.GetValue<Value>(COLUMN_INDEX_START).IsNull()) {
			throw InvalidInputException("Failed to load DuckLake - Table entry \"%s\" does not have any columns",
			                            table_entry.name);
		}
		DuckLakeColumnInfo column_info;
		column_info.id = FieldIndex(row.GetValue<uint64_t>(COLUMN_INDEX_START));
		column_info.name = row.GetValue<string>(COLUMN_INDEX_START + 1);
		column_info.type = row.GetValue<string>(COLUMN_INDEX_START + 2);
		if (!row.IsNull(COLUMN_INDEX_START + 3)) {
			column_info.initial_default = Value(row.GetValue<string>(COLUMN_INDEX_START + 3));
		}
		if (!row.IsNull(COLUMN_INDEX_START + 4)) {
			column_info.default_value = Value(row.GetValue<string>(COLUMN_INDEX_START + 4));
		}
		column_info.nulls_allowed = row.GetValue<bool>(COLUMN_INDEX_START + 5);
		if (!row.IsNull(COLUMN_INDEX_START + 7)) {
			auto tags = row.GetValue<Value>(COLUMN_INDEX_START + 7);
			column_info.tags = LoadTags(tags);
		}

		if (row.IsNull(COLUMN_INDEX_START + 6)) {
			// base column - add the column to this table
			table_entry.columns.push_back(std::move(column_info));
		} else {
			auto parent_id = FieldIndex(row.GetValue<idx_t>(COLUMN_INDEX_START + 6));
			if (!AddChildColumn(table_entry.columns, parent_id, column_info)) {
				throw InvalidInputException("Failed to load DuckLake - Could not find parent column for column %s",
				                            column_info.name);
			}
		}
	}
	// load view information
	result = transaction.Query(snapshot, R"(
SELECT view_id, view_uuid, schema_id, view_name, dialect, sql, column_aliases,
	(
		SELECT LIST({'key': key, 'value': value})
		FROM {METADATA_CATALOG}.ducklake_tag tag
		WHERE object_id=view_id AND
		      {SNAPSHOT_ID} >= tag.begin_snapshot AND ({SNAPSHOT_ID} < tag.end_snapshot OR tag.end_snapshot IS NULL)
	) AS tag
FROM {METADATA_CATALOG}.ducklake_view view
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < view.end_snapshot OR view.end_snapshot IS NULL)
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get partition information from DuckLake: ");
	}
	auto &views = catalog.views;
	for (auto &row : *result) {
		DuckLakeViewInfo view_info;
		view_info.id = TableIndex(row.GetValue<uint64_t>(0));
		view_info.uuid = row.GetValue<string>(1);
		view_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(2));
		view_info.name = row.GetValue<string>(3);
		view_info.dialect = row.GetValue<string>(4);
		view_info.sql = row.GetValue<string>(5);
		view_info.column_aliases = DuckLakeUtil::ParseQuotedList(row.GetValue<string>(6));
		if (!row.IsNull(7)) {
			auto tags = row.GetValue<Value>(7);
			view_info.tags = LoadTags(tags);
		}
		views.push_back(std::move(view_info));
	}

	// load partition information
	result = transaction.Query(snapshot, R"(
SELECT partition_id, part.table_id, partition_key_index, column_id, transform
FROM {METADATA_CATALOG}.ducklake_partition_info part
JOIN {METADATA_CATALOG}.ducklake_partition_column part_col USING (partition_id)
WHERE {SNAPSHOT_ID} >= part.begin_snapshot AND ({SNAPSHOT_ID} < part.end_snapshot OR part.end_snapshot IS NULL)
ORDER BY part.table_id, partition_id, partition_key_index
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get partition information from DuckLake: ");
	}
	auto &partitions = catalog.partitions;
	for (auto &row : *result) {
		auto partition_id = row.GetValue<uint64_t>(0);
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		if (partitions.empty() || partitions.back().table_id != table_id) {
			DuckLakePartitionInfo partition_info;
			partition_info.id = partition_id;
			partition_info.table_id = table_id;
			partitions.push_back(std::move(partition_info));
		}
		auto &partition_entry = partitions.back();

		DuckLakePartitionFieldInfo partition_field;
		partition_field.partition_key_index = row.GetValue<uint64_t>(2);
		partition_field.field_id = FieldIndex(row.GetValue<uint64_t>(3));
		partition_field.transform = row.GetValue<string>(4);
		partition_entry.fields.push_back(std::move(partition_field));
	}
	return catalog;
}

vector<DuckLakeGlobalStatsInfo> DuckLakeMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot) {
	// query the most recent stats
	auto result = transaction.Query(snapshot, R"(
SELECT table_id, column_id, record_count, next_row_id, file_size_bytes, contains_null, contains_nan, min_value, max_value
FROM {METADATA_CATALOG}.ducklake_table_stats
LEFT JOIN {METADATA_CATALOG}.ducklake_table_column_stats USING (table_id)
WHERE record_count IS NOT NULL AND file_size_bytes IS NOT NULL
ORDER BY table_id;
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get global stats information from DuckLake: ");
	}
	vector<DuckLakeGlobalStatsInfo> global_stats;
	for (auto &row : *result) {
		auto table_id = TableIndex(row.GetValue<uint64_t>(0));
		if (global_stats.empty() || global_stats.back().table_id != table_id) {
			// new stats
			DuckLakeGlobalStatsInfo new_entry;

			// set up the table-level stats
			new_entry.table_id = table_id;
			new_entry.initialized = true;
			new_entry.record_count = row.GetValue<uint64_t>(2);
			new_entry.next_row_id = row.GetValue<uint64_t>(3);
			new_entry.table_size_bytes = row.GetValue<uint64_t>(4);
			global_stats.push_back(std::move(new_entry));
		}
		auto &stats_entry = global_stats.back();

		DuckLakeGlobalColumnStatsInfo column_stats;
		column_stats.column_id = FieldIndex(row.GetValue<uint64_t>(1));
		static constexpr const idx_t COLUMN_STATS_START = 5;
		if (row.IsNull(COLUMN_STATS_START)) {
			column_stats.has_contains_null = false;
		} else {
			column_stats.has_contains_null = true;
			column_stats.contains_null = row.GetValue<bool>(COLUMN_STATS_START);
		}
		if (row.IsNull(COLUMN_STATS_START + 1)) {
			column_stats.has_contains_nan = false;
		} else {
			column_stats.has_contains_nan = true;
			column_stats.contains_nan = row.GetValue<bool>(COLUMN_STATS_START + 1);
		}
		if (row.IsNull(COLUMN_STATS_START + 2)) {
			column_stats.has_min = false;
		} else {
			column_stats.has_min = true;
			column_stats.min_val = row.GetValue<string>(COLUMN_STATS_START + 2);
		}
		if (row.IsNull(COLUMN_STATS_START + 3)) {
			column_stats.has_max = false;
		} else {
			column_stats.has_max = true;
			column_stats.max_val = row.GetValue<string>(COLUMN_STATS_START + 3);
		}

		stats_entry.column_stats.push_back(std::move(column_stats));
	}
	return global_stats;
}

string DuckLakeMetadataManager::GetFileSelectList(const string &prefix) {
	auto result = StringUtil::Replace(
	    "{PREFIX}.path, {PREFIX}.path_is_relative, {PREFIX}.file_size_bytes, {PREFIX}.footer_size", "{PREFIX}", prefix);
	if (IsEncrypted()) {
		result += ", " + prefix + ".encryption_key";
	}
	return result;
}

template <class T>
DuckLakeFileData DuckLakeMetadataManager::ReadDataFile(DuckLakeTableEntry &table, T &row, idx_t &col_idx,
                                                       bool is_encrypted) {
	DuckLakeFileData data;
	if (row.IsNull(col_idx)) {
		// file is not there
		col_idx += 4;
		if (is_encrypted) {
			col_idx++;
		}
		return data;
	}
	DuckLakePath path;
	path.path = row.template GetValue<string>(col_idx++);
	path.path_is_relative = row.template GetValue<bool>(col_idx++);

	data.path = FromRelativePath(path, table.DataPath());
	data.file_size_bytes = row.template GetValue<idx_t>(col_idx++);
	if (!row.IsNull(col_idx)) {
		data.footer_size = row.template GetValue<idx_t>(col_idx);
	}
	col_idx++;
	if (is_encrypted) {
		if (row.IsNull(col_idx)) {
			throw InvalidInputException("Database is encrypted, but file %s does not have an encryption key",
			                            data.path);
		}
		data.encryption_key = Blob::FromBase64(row.template GetValue<string>(col_idx++));
	}
	return data;
}

string PartialFileInfoToString(const vector<DuckLakePartialFileInfo> &partial_file_info) {
	string result;
	for (auto &info : partial_file_info) {
		if (!result.empty()) {
			result += "|";
		}
		result += to_string(info.snapshot_id);
		result += ":";
		result += to_string(info.max_row_count);
	}
	return result;
}

vector<DuckLakePartialFileInfo> ParsePartialFileInfo(const string &str) {
	auto splits = StringUtil::Split(str, "|");
	vector<DuckLakePartialFileInfo> result;
	for (auto &split : splits) {
		auto partial_split = StringUtil::Split(split, ":");
		DuckLakePartialFileInfo file_info;
		file_info.snapshot_id = StringUtil::ToUnsigned(partial_split[0]);
		file_info.max_row_count = StringUtil::ToUnsigned(partial_split[1]);
		result.push_back(file_info);
	}
	return result;
}

idx_t GetMaxRowCount(DuckLakeSnapshot snapshot, const string &partial_file_info_str) {
	auto partial_file_info = ParsePartialFileInfo(partial_file_info_str);
	idx_t max_row_count = 0;
	for (auto &info : partial_file_info) {
		if (info.snapshot_id <= snapshot.snapshot_id) {
			max_row_count = MaxValue<idx_t>(max_row_count, info.max_row_count);
		}
	}
	return max_row_count;
}

void ParsePartialFileInfo(DuckLakeSnapshot snapshot, const string &partial_file_info_str,
                          DuckLakeFileListEntry &file_entry) {
	if (StringUtil::StartsWith(partial_file_info_str, "partial_max:")) {
		auto max_partial_file_snapshot = StringUtil::ToUnsigned(partial_file_info_str.substr(12));
		if (max_partial_file_snapshot <= snapshot.snapshot_id) {
			// all snapshot ids are included for this snapshot - skip reading partial file info
			return;
		}
		file_entry.snapshot_filter = snapshot.snapshot_id;
	} else {
		file_entry.max_row_count = GetMaxRowCount(snapshot, partial_file_info_str);
	}
}

vector<DuckLakeFileListEntry>
DuckLakeMetadataManager::GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot, const string &filter) {
	auto table_id = table.GetTableId();
	string select_list = GetFileSelectList("data") +
	                     ", data.row_id_start, data.begin_snapshot, data.partial_file_info, data.mapping_id, " +
	                     GetFileSelectList("del");
	auto query = StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.ducklake_data_file data
LEFT JOIN (
    SELECT *
    FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id=%d  AND {SNAPSHOT_ID} >= begin_snapshot
          AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
    ) del USING (data_file_id)
WHERE data.table_id=%d AND {SNAPSHOT_ID} >= data.begin_snapshot AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)
		)",
	                                select_list, table_id.index, table_id.index);
	if (!filter.empty()) {
		query += "\nAND " + filter;
	}

	auto result = transaction.Query(snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get data file list from DuckLake: ");
	}
	vector<DuckLakeFileListEntry> files;
	for (auto &row : *result) {
		DuckLakeFileListEntry file_entry;
		idx_t col_idx = 0;
		file_entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		file_entry.snapshot_id = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			auto partial_file_info = row.GetValue<string>(col_idx);
			ParsePartialFileInfo(snapshot, partial_file_info, file_entry);
		}
		col_idx++;
		if (!row.IsNull(col_idx)) {
			file_entry.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		file_entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		files.push_back(std::move(file_entry));
	}
	return files;
}

vector<DuckLakeFileListEntry> DuckLakeMetadataManager::GetTableInsertions(DuckLakeTableEntry &table,
                                                                          DuckLakeSnapshot start_snapshot,
                                                                          DuckLakeSnapshot end_snapshot) {
	auto table_id = table.GetTableId();
	string select_list = GetFileSelectList("data") +
	                     ", data.row_id_start, data.begin_snapshot, data.partial_file_info, data.mapping_id, " +
	                     GetFileSelectList("del");
	auto query = StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.ducklake_data_file data, (
	SELECT NULL path, NULL path_is_relative, NULL file_size_bytes, NULL footer_size, NULL encryption_key
) del
WHERE data.table_id=%d AND data.begin_snapshot >= %d AND data.begin_snapshot <= {SNAPSHOT_ID};
		)",
	                                select_list, table_id.index, start_snapshot.snapshot_id);

	auto result = transaction.Query(end_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table insertion file list from DuckLake: ");
	}
	vector<DuckLakeFileListEntry> files;
	for (auto &row : *result) {
		DuckLakeFileListEntry file_entry;
		idx_t col_idx = 0;
		file_entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		file_entry.snapshot_id = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			auto partial_file_info = row.GetValue<string>(col_idx);
			ParsePartialFileInfo(end_snapshot, partial_file_info, file_entry);
		}
		col_idx++;
		if (!row.IsNull(col_idx)) {
			file_entry.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		file_entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		files.push_back(std::move(file_entry));
	}
	return files;
}

vector<DuckLakeDeleteScanEntry> DuckLakeMetadataManager::GetTableDeletions(DuckLakeTableEntry &table,
                                                                           DuckLakeSnapshot start_snapshot,
                                                                           DuckLakeSnapshot end_snapshot) {
	auto table_id = table.GetTableId();
	string select_list = GetFileSelectList("data") + ", data.row_id_start, data.record_count, data.mapping_id, " +
	                     GetFileSelectList("current_delete") + ", " + GetFileSelectList("previous_delete");
	// deletes come in two flavors:
	// * deletes stored in the ducklake_delete_file table (partial deletes)
	// * data files being deleted entirely through setting end_snapshot (full file deletes)
	// we gather both of these deletes in two separate queries
	// for both deletes, we need to obtain any PREVIOUS deletes as well
	// we need these since we are only interested in rows deleted between start_snapshot and end_snapshot
	// so we need to exclude any rows that were already deleted prior to this moment
	auto query =
	    StringUtil::Format(R"(
SELECT %s, current_delete.begin_snapshot FROM (
	SELECT data_file_id, begin_snapshot, path, path_is_relative, file_size_bytes, footer_size, encryption_key
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE table_id = %d AND begin_snapshot >= %d AND begin_snapshot <= {SNAPSHOT_ID}
) AS current_delete
LEFT JOIN (
	SELECT data_file_id, MAX_BY(COLUMNS(['path', 'path_is_relative', 'file_size_bytes', 'footer_size', 'encryption_key']), begin_snapshot) AS '\0'
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE table_id = %d AND begin_snapshot < current_delete.begin_snapshot
	GROUP BY data_file_id
) AS previous_delete
USING (data_file_id)
JOIN (
	FROM {METADATA_CATALOG}.ducklake_data_file data
	WHERE table_id = %d
) AS data
USING (data_file_id)

UNION ALL

SELECT %s, data.end_snapshot FROM (
	FROM {METADATA_CATALOG}.ducklake_data_file
	WHERE table_id = %d AND end_snapshot >= %d AND end_snapshot <= {SNAPSHOT_ID}
) AS data
LEFT JOIN (
	SELECT data_file_id, MAX_BY(COLUMNS(['path', 'path_is_relative', 'file_size_bytes', 'footer_size', 'encryption_key']), begin_snapshot) AS '\0'
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE table_id = %d AND begin_snapshot < data.end_snapshot
	GROUP BY data_file_id
) AS previous_delete
USING (data_file_id), (
	SELECT NULL path, NULL path_is_relative, NULL file_size_bytes, NULL footer_size, NULL encryption_key
) current_delete;
		)",
	                       select_list, table_id.index, start_snapshot.snapshot_id, table_id.index, table_id.index,
	                       select_list, table_id.index, start_snapshot.snapshot_id, table_id.index);
	auto result = transaction.Query(end_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table insertion file list from DuckLake: ");
	}
	vector<DuckLakeDeleteScanEntry> files;
	for (auto &row : *result) {
		DuckLakeDeleteScanEntry entry;
		idx_t col_idx = 0;
		entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		entry.row_count = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			entry.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		entry.previous_delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		entry.snapshot_id = row.GetValue<idx_t>(col_idx++);
		files.push_back(std::move(entry));
	}
	return files;
}

vector<DuckLakeFileListExtendedEntry> DuckLakeMetadataManager::GetExtendedFilesForTable(DuckLakeTableEntry &table,
                                                                                        DuckLakeSnapshot snapshot,
                                                                                        const string &filter) {
	auto table_id = table.GetTableId();
	string select_list = GetFileSelectList("data") + ", data.row_id_start, " + GetFileSelectList("del");
	auto query = StringUtil::Format(R"(
SELECT data.data_file_id, del.delete_file_id, data.record_count, %s
FROM {METADATA_CATALOG}.ducklake_data_file data
LEFT JOIN (
	SELECT *
    FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id=%d  AND {SNAPSHOT_ID} >= begin_snapshot
          AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
    ) del USING (data_file_id)
WHERE data.table_id=%d AND {SNAPSHOT_ID} >= data.begin_snapshot AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)
		)",
	                                select_list, table_id.index, table_id.index);
	if (!filter.empty()) {
		query += "\nAND " + filter;
	}

	auto result = transaction.Query(snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get extended data file list from DuckLake: ");
	}
	vector<DuckLakeFileListExtendedEntry> files;
	for (auto &row : *result) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file_id = DataFileIndex(row.GetValue<idx_t>(0));
		if (!row.IsNull(1)) {
			file_entry.delete_file_id = DataFileIndex(row.GetValue<idx_t>(1));
		}
		file_entry.row_count = row.GetValue<idx_t>(2);
		idx_t col_idx = 3;
		file_entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		file_entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		files.push_back(std::move(file_entry));
	}
	return files;
}

vector<DuckLakeCompactionFileEntry> DuckLakeMetadataManager::GetFilesForCompaction(DuckLakeTableEntry &table) {
	auto table_id = table.GetTableId();
	string data_select_list = "data.data_file_id, data.record_count, data.row_id_start, data.begin_snapshot, "
	                          "data.end_snapshot, data.mapping_id, snapshot.schema_version, data.partial_file_info, "
	                          "data.partition_id, partition_info.keys, " +
	                          GetFileSelectList("data");
	string delete_select_list =
	    "del.data_file_id, del.delete_count, del.begin_snapshot, del.end_snapshot, " + GetFileSelectList("del");
	string select_list = data_select_list + ", " + delete_select_list;
	auto query = StringUtil::Format(R"(
SELECT %s,
FROM {METADATA_CATALOG}.ducklake_data_file data
JOIN {METADATA_CATALOG}.ducklake_snapshot snapshot ON (data.begin_snapshot = snapshot.snapshot_id)
LEFT JOIN (
	SELECT *
    FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id=%d
) del USING (data_file_id)
LEFT JOIN (
   SELECT data_file_id, LIST(partition_value ORDER BY partition_key_index) keys
   FROM {METADATA_CATALOG}.ducklake_file_partition_value
   GROUP BY data_file_id
) partition_info USING (data_file_id)
WHERE data.table_id=%d
ORDER BY data.begin_snapshot, data.row_id_start, data.data_file_id, del.begin_snapshot
		)",
	                                select_list, table_id.index, table_id.index);
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get compation file list from DuckLake: ");
	}
	vector<DuckLakeCompactionFileEntry> files;
	for (auto &row : *result) {
		idx_t col_idx = 0;
		DuckLakeCompactionFileEntry new_entry;
		// parse the data file
		new_entry.file.id = DataFileIndex(row.GetValue<idx_t>(col_idx++));
		new_entry.file.row_count = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			new_entry.file.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		new_entry.file.begin_snapshot = row.GetValue<idx_t>(col_idx++);
		new_entry.file.end_snapshot = row.IsNull(col_idx) ? optional_idx() : row.GetValue<idx_t>(col_idx);
		col_idx++;
		if (!row.IsNull(col_idx)) {
			new_entry.file.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		new_entry.schema_version = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			// parse the partial file info
			auto partial_file_info = row.GetValue<string>(col_idx);
			new_entry.partial_files = ParsePartialFileInfo(partial_file_info);
		}
		col_idx++;
		new_entry.file.partition_id = row.IsNull(col_idx) ? optional_idx() : row.GetValue<idx_t>(col_idx);
		col_idx++;
		if (!row.IsNull(col_idx)) {
			auto list_val = row.GetValue<Value>(col_idx);
			for (auto &entry : ListValue::GetChildren(list_val)) {
				new_entry.file.partition_values.push_back(StringValue::Get(entry));
			}
		}
		col_idx++;
		new_entry.file.data = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (files.empty() || files.back().file.id != new_entry.file.id) {
			// new file - push it into the file list
			files.push_back(std::move(new_entry));
		}
		auto &file_entry = files.back();
		// parse the delete file (if any)
		if (row.IsNull(col_idx)) {
			// no delete file
			continue;
		}
		DuckLakeCompactionDeleteFileData delete_file;
		delete_file.id = DataFileIndex(row.GetValue<idx_t>(col_idx++));
		delete_file.row_count = row.GetValue<idx_t>(col_idx++);
		delete_file.begin_snapshot = row.GetValue<idx_t>(col_idx++);
		delete_file.end_snapshot = row.IsNull(col_idx) ? optional_idx() : row.GetValue<idx_t>(col_idx);
		col_idx++;
		delete_file.data = ReadDataFile(table, row, col_idx, IsEncrypted());
		file_entry.delete_files.push_back(std::move(delete_file));
	}
	return files;
}

template <class T>
void DuckLakeMetadataManager::FlushDrop(DuckLakeSnapshot commit_snapshot, const string &metadata_table_name,
                                        const string &id_name, const set<T> &dropped_entries) {
	if (dropped_entries.empty()) {
		return;
	}
	string dropped_id_list;
	for (auto &dropped_id : dropped_entries) {
		if (!dropped_id_list.empty()) {
			dropped_id_list += ", ";
		}
		dropped_id_list += to_string(dropped_id.index);
	}
	auto dropped_id_query = StringUtil::Format(
	    R"(UPDATE {METADATA_CATALOG}.%s SET end_snapshot = {SNAPSHOT_ID} WHERE end_snapshot IS NULL AND %s IN (%s);)",
	    metadata_table_name, id_name, dropped_id_list);
	auto result = transaction.Query(commit_snapshot, dropped_id_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write drop information to DuckLake:");
	}
}

void DuckLakeMetadataManager::DropSchemas(DuckLakeSnapshot commit_snapshot, const set<SchemaIndex> &ids) {
	FlushDrop(commit_snapshot, "ducklake_schema", "schema_id", ids);
}

void DuckLakeMetadataManager::DropTables(DuckLakeSnapshot commit_snapshot, const set<TableIndex> &ids, bool renamed) {
	FlushDrop(commit_snapshot, "ducklake_table", "table_id", ids);
	if (renamed == false) {
		FlushDrop(commit_snapshot, "ducklake_partition_info", "table_id", ids);
		FlushDrop(commit_snapshot, "ducklake_column", "table_id", ids);
		FlushDrop(commit_snapshot, "ducklake_column_tag", "table_id", ids);
		FlushDrop(commit_snapshot, "ducklake_data_file", "table_id", ids);
		FlushDrop(commit_snapshot, "ducklake_delete_file", "table_id", ids);
		FlushDrop(commit_snapshot, "ducklake_tag", "object_id", ids);
	}
}

void DuckLakeMetadataManager::DropViews(DuckLakeSnapshot commit_snapshot, const set<TableIndex> &ids) {
	FlushDrop(commit_snapshot, "ducklake_view", "view_id", ids);
}

void DuckLakeMetadataManager::WriteNewSchemas(DuckLakeSnapshot commit_snapshot,
                                              const vector<DuckLakeSchemaInfo> &new_schemas) {
	if (new_schemas.empty()) {
		throw InternalException("No schemas to create - should be handled elsewhere");
	}
	string schema_insert_sql;
	for (auto &new_schema : new_schemas) {
		if (!schema_insert_sql.empty()) {
			schema_insert_sql += ",";
		}
		auto schema_id = new_schema.id.index;
		auto path = GetRelativePath(new_schema.path);
		schema_insert_sql += StringUtil::Format("(%d, '%s', {SNAPSHOT_ID}, NULL, %s, %s, %s)", schema_id,
		                                        new_schema.uuid, SQLString(new_schema.name), SQLString(path.path),
		                                        path.path_is_relative ? "true" : "false");
	}
	schema_insert_sql = "INSERT INTO {METADATA_CATALOG}.ducklake_schema VALUES " + schema_insert_sql;
	auto result = transaction.Query(commit_snapshot, schema_insert_sql);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new schemas to DuckLake: ");
	}
}

void ColumnToSQLRecursive(const DuckLakeColumnInfo &column, TableIndex table_id, optional_idx parent, string &result) {
	if (!result.empty()) {
		result += ",";
	}
	string parent_idx = parent.IsValid() ? to_string(parent.GetIndex()) : "NULL";
	string initial_default =
	    !column.initial_default.IsNull() ? KeywordHelper::WriteQuoted(column.initial_default.ToString(), '\'') : "NULL";
	string default_val =
	    !column.default_value.IsNull() ? KeywordHelper::WriteQuoted(column.default_value.ToString(), '\'') : "NULL";
	auto column_id = column.id.index;
	auto column_order = column_id;
	result += StringUtil::Format("(%d, {SNAPSHOT_ID}, NULL, %d, %d, %s, %s, %s, %s, %d, %s)", column_id, table_id.index,
	                             column_order, SQLString(column.name), SQLString(column.type), initial_default,
	                             default_val, column.nulls_allowed ? 1 : 0, parent_idx);
	for (auto &child : column.children) {
		ColumnToSQLRecursive(child, table_id, column_id, result);
	}
}

string DuckLakeMetadataManager::GetColumnType(const DuckLakeColumnInfo &col) {
	if (col.children.empty()) {
		return DuckLakeTypes::FromString(col.type).ToString();
	}
	if (col.type == "struct") {
		string result;
		for (auto &child : col.children) {
			if (!result.empty()) {
				result += ", ";
			}
			result += StringUtil::Format("%s %s", SQLIdentifier(child.name), GetColumnType(child));
		}
		return "STRUCT(" + result + ")";
	}
	if (col.type == "list") {
		return GetColumnType(col.children[0]) + "[]";
	}
	if (col.type == "map") {
		return StringUtil::Format("MAP(%s, %s)", GetColumnType(col.children[0]), GetColumnType(col.children[1]));
	}
	throw InternalException("Unsupported nested type %s in DuckLakeMetadataManager::GetColumnType", col.type);
}

string DuckLakeMetadataManager::GetInlinedTableQuery(const DuckLakeTableInfo &table, const string &table_name) {
	string columns;

	for (auto &col : table.columns) {
		if (!columns.empty()) {
			columns += ", ";
		}
		columns += StringUtil::Format("%s %s", SQLIdentifier(col.name), GetColumnType(col));
	}
	return StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.%s(row_id BIGINT, begin_snapshot BIGINT, "
	                          "end_snapshot BIGINT, %s);",
	                          SQLIdentifier(table_name), columns);
}

void DuckLakeMetadataManager::WriteNewTables(DuckLakeSnapshot commit_snapshot,
                                             const vector<DuckLakeTableInfo> &new_tables) {
	string column_insert_sql;
	string table_insert_sql;
	for (auto &table : new_tables) {
		if (!table_insert_sql.empty()) {
			table_insert_sql += ", ";
		}
		auto schema_id = table.schema_id.index;
		auto path = GetRelativePath(table.schema_id, table.path);
		table_insert_sql +=
		    StringUtil::Format("(%d, '%s', {SNAPSHOT_ID}, NULL, %d, %s, %s, %s)", table.id.index, table.uuid, schema_id,
		                       SQLString(table.name), SQLString(path.path), path.path_is_relative ? "true" : "false");
		for (auto &column : table.columns) {
			ColumnToSQLRecursive(column, table.id, optional_idx(), column_insert_sql);
		}
	}
	if (!table_insert_sql.empty()) {
		// insert table entries
		table_insert_sql = "INSERT INTO {METADATA_CATALOG}.ducklake_table VALUES " + table_insert_sql;
		auto result = transaction.Query(commit_snapshot, table_insert_sql);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to write new table to DuckLake: ");
		}
	}
	if (!column_insert_sql.empty()) {
		// insert column entries
		column_insert_sql = "INSERT INTO {METADATA_CATALOG}.ducklake_column VALUES " + column_insert_sql;
		auto result = transaction.Query(commit_snapshot, column_insert_sql);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to write column information to DuckLake: ");
		}
	}
	// write new data-inlining tables (if data-inlining is enabled)
	WriteNewInlinedTables(commit_snapshot, new_tables);
}

string GetInlinedTableName(const DuckLakeTableInfo &table, const DuckLakeSnapshot &snapshot) {
	return StringUtil::Format("ducklake_inlined_data_%d_%d", table.id.index, snapshot.schema_version);
}

string DuckLakeMetadataManager::GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
                                                       string &inlined_tables, string &inlined_table_queries) {
	if (!inlined_tables.empty()) {
		inlined_tables += ", ";
	}
	auto schema_version = commit_snapshot.schema_version;
	string inlined_table_name = GetInlinedTableName(table, commit_snapshot);
	inlined_tables += StringUtil::Format("(%d, %s, %d)", table.id.index, SQLString(inlined_table_name), schema_version);
	if (!inlined_table_queries.empty()) {
		inlined_table_queries += "\n";
	}
	inlined_table_queries += GetInlinedTableQuery(table, inlined_table_name);
	return inlined_table_name;
}

void DuckLakeMetadataManager::ExecuteInlinedTableQueries(DuckLakeSnapshot commit_snapshot, string &inlined_tables,
                                                         const string &inlined_table_queries) {
	if (inlined_tables.empty()) {
		return;
	}
	inlined_tables = "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables VALUES " + inlined_tables;
	auto result = transaction.Query(commit_snapshot, inlined_tables);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new inlined tables table to DuckLake: ");
	}
	result = transaction.Query(commit_snapshot, inlined_table_queries);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new inlined tables to DuckLake: ");
	}
}

void DuckLakeMetadataManager::WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot,
                                                    const vector<DuckLakeTableInfo> &new_tables) {
	auto &catalog = transaction.GetCatalog();
	string inlined_tables;
	string inlined_table_queries;
	for (auto &table : new_tables) {
		if (catalog.DataInliningRowLimit(table.schema_id, table.id) == 0) {
			// not inlining for this table - skip it
			continue;
		}
		GetInlinedTableQueries(commit_snapshot, table, inlined_tables, inlined_table_queries);
	}
	ExecuteInlinedTableQueries(commit_snapshot, inlined_tables, inlined_table_queries);
}

void DuckLakeMetadataManager::WriteDroppedColumns(DuckLakeSnapshot commit_snapshot,
                                                  const vector<DuckLakeDroppedColumn> &dropped_columns) {
	if (dropped_columns.empty()) {
		return;
	}
	string dropped_cols;
	for (auto &dropped_col : dropped_columns) {
		if (!dropped_cols.empty()) {
			dropped_cols += ", ";
		}
		dropped_cols += StringUtil::Format("(%d, %d)", dropped_col.table_id.index, dropped_col.field_id.index);
	}
	// overwrite the snapshot for the old columns
	auto result = transaction.Query(commit_snapshot, StringUtil::Format(R"(
WITH dropped_cols(tid, cid) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_column
SET end_snapshot = {SNAPSHOT_ID}
FROM dropped_cols
WHERE table_id=tid AND column_id=cid
)",
	                                                                    dropped_cols));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to drop columns in DuckLake: ");
	}
}

void DuckLakeMetadataManager::WriteNewColumns(DuckLakeSnapshot commit_snapshot,
                                              const vector<DuckLakeNewColumn> &new_columns) {
	if (new_columns.empty()) {
		return;
	}
	string column_insert_sql;
	for (auto &new_col : new_columns) {
		ColumnToSQLRecursive(new_col.column_info, new_col.table_id, new_col.parent_idx, column_insert_sql);
	}

	// insert column entries
	column_insert_sql = "INSERT INTO {METADATA_CATALOG}.ducklake_column VALUES " + column_insert_sql;
	auto result = transaction.Query(commit_snapshot, column_insert_sql);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write column information to DuckLake: ");
	}
}

void DuckLakeMetadataManager::WriteNewViews(DuckLakeSnapshot commit_snapshot,
                                            const vector<DuckLakeViewInfo> &new_views) {
	string view_insert_sql;
	for (auto &view : new_views) {
		if (!view_insert_sql.empty()) {
			view_insert_sql += ", ";
		}
		auto schema_id = view.schema_id.index;
		view_insert_sql +=
		    StringUtil::Format("(%d, '%s', {SNAPSHOT_ID}, NULL, %d, %s, %s, %s, %s)", view.id.index, view.uuid,
		                       schema_id, SQLString(view.name), SQLString(view.dialect), SQLString(view.sql),
		                       SQLString(DuckLakeUtil::ToQuotedList(view.column_aliases)));
	}
	if (!view_insert_sql.empty()) {
		// insert table entries
		view_insert_sql = "INSERT INTO {METADATA_CATALOG}.ducklake_view VALUES " + view_insert_sql;
		auto result = transaction.Query(commit_snapshot, view_insert_sql);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to write new view to DuckLake: ");
		}
	}
}

void DuckLakeMetadataManager::WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
                                                  const vector<DuckLakeInlinedDataInfo> &new_data) {
	if (new_data.empty()) {
		return;
	}
	for (auto &entry : new_data) {
		// get the latest table to insert into
		// FIXME: we could keep this cached some other way to avoid the round-trip/dependency
		string inlined_table_name;
		auto query = StringUtil::Format(R"(
SELECT table_name
FROM {METADATA_CATALOG}.ducklake_inlined_data_tables
WHERE table_id = %d AND schema_version=(
    SELECT MAX(schema_version)
    FROM {METADATA_CATALOG}.ducklake_inlined_data_tables
    WHERE table_id=%d
);)",
		                                entry.table_id.index, entry.table_id.index);
		auto result = transaction.Query(commit_snapshot, query);
		for (auto &row : *result) {
			if (!inlined_table_name.empty()) {
				throw InvalidInputException("Multiple inlined data table names found for table id %d",
				                            entry.table_id.index);
			}
			inlined_table_name = row.GetValue<string>(0);
		}
		if (inlined_table_name.empty()) {
			// no inlined table yet - create a new one
			// first fetch the table info
			auto current_snapshot = transaction.GetSnapshot();
			auto table_entry = transaction.GetCatalog().GetEntryById(transaction, current_snapshot, entry.table_id);
			if (!table_entry) {
				throw InternalException("Writing inlined data for a table that cannot be found in the catalog");
			}
			auto &table = table_entry->Cast<DuckLakeTableEntry>();
			auto table_info = table.GetTableInfo();
			table_info.columns = table.GetTableColumns();

			// write the new inlined table
			string inlined_tables;
			string inlined_table_queries;
			commit_snapshot.schema_version++;
			inlined_table_name =
			    GetInlinedTableQueries(commit_snapshot, table_info, inlined_tables, inlined_table_queries);
			ExecuteInlinedTableQueries(commit_snapshot, inlined_tables, inlined_table_queries);
		}

		// append the data
		// FIXME: we can do a much faster append than this
		string values;
		idx_t row_id = entry.row_id_start;
		for (auto &chunk : entry.data->data->Chunks()) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				if (!values.empty()) {
					values += ", ";
				}
				values += "(";
				values += to_string(row_id);
				values += ", {SNAPSHOT_ID}, NULL";
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					values += ", ";
					values += DuckLakeUtil::ValueToSQL(chunk.GetValue(c, r));
				}
				values += ")";
				row_id++;
			}
		}
		string append_query = StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s VALUES %s",
		                                         SQLIdentifier(inlined_table_name), values);
		result = transaction.Query(commit_snapshot, append_query);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to write inlined data to DuckLake: ");
		}
	}
}

void DuckLakeMetadataManager::WriteNewInlinedDeletes(DuckLakeSnapshot commit_snapshot,
                                                     const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes) {
	if (new_deletes.empty()) {
		return;
	}
	for (auto &entry : new_deletes) {
		// get a list of all deleted row-ids for this table
		string row_id_list;
		for (auto &deleted_id : entry.deleted_row_ids) {
			if (!row_id_list.empty()) {
				row_id_list += ", ";
			}
			row_id_list += StringUtil::Format("(%d)", deleted_id);
		}
		// overwrite the snapshot for the old tags
		auto result = transaction.Query(commit_snapshot, StringUtil::Format(R"(
WITH deleted_row_list(deleted_row_id) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.%s
SET end_snapshot = {SNAPSHOT_ID}
FROM deleted_row_list
WHERE row_id=deleted_row_id
)",
		                                                                    row_id_list, entry.table_name));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to write inlined delete information in DuckLake: ");
		}
	}
}

shared_ptr<DuckLakeInlinedData> DuckLakeMetadataManager::TransformInlinedData(QueryResult &result) {
	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}

	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, result.types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		data->Append(*chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

static string GetProjection(const vector<string> &columns_to_read) {
	string result;
	for (auto &entry : columns_to_read) {
		if (!result.empty()) {
			result += ", ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(entry);
	}
	return result;
}

shared_ptr<DuckLakeInlinedData> DuckLakeMetadataManager::ReadInlinedData(DuckLakeSnapshot snapshot,
                                                                         const string &inlined_table_name,
                                                                         const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL);)",
	                                                             projection, inlined_table_name));
	return TransformInlinedData(*result);
}

shared_ptr<DuckLakeInlinedData>
DuckLakeMetadataManager::ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
                                                   const string &inlined_table_name,
                                                   const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result =
	    transaction.Query(end_snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE inlined_data.begin_snapshot >= %d AND inlined_data.begin_snapshot <= {SNAPSHOT_ID};)",
	                                                       projection, inlined_table_name, start_snapshot.snapshot_id));
	return TransformInlinedData(*result);
}

shared_ptr<DuckLakeInlinedData>
DuckLakeMetadataManager::ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
                                                  const string &inlined_table_name,
                                                  const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result =
	    transaction.Query(end_snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE inlined_data.end_snapshot >= %d AND inlined_data.end_snapshot <= {SNAPSHOT_ID};)",
	                                                       projection, inlined_table_name, start_snapshot.snapshot_id));
	return TransformInlinedData(*result);
}

string DuckLakeMetadataManager::GetPathForSchema(SchemaIndex schema_id) {
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema
WHERE schema_id = %d;)",
	                                                   schema_id.index));
	for (auto &row : *result) {
		DuckLakePath path;
		path.path = row.GetValue<string>(0);
		path.path_is_relative = row.GetValue<bool>(1);
		return FromRelativePath(path);
	}
	throw InvalidInputException("Failed to get path for schema with id %d - schema not found in metadata catalog",
	                            schema_id.index);
}

string DuckLakeMetadataManager::GetPathForTable(TableIndex table_id) {
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT s.path, s.path_is_relative, t.path, t.path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema s
JOIN {METADATA_CATALOG}.ducklake_table t
USING (schema_id)
WHERE table_id = %d;)",
	                                                   table_id.index));
	for (auto &row : *result) {
		DuckLakePath schema_path;
		schema_path.path = row.GetValue<string>(0);
		schema_path.path_is_relative = row.GetValue<bool>(1);
		auto resolved_schema_path = FromRelativePath(schema_path);

		DuckLakePath table_path;
		table_path.path = row.GetValue<string>(2);
		table_path.path_is_relative = row.GetValue<bool>(3);
		return FromRelativePath(table_path, resolved_schema_path);
	}
	throw InvalidInputException("Failed to get path for table with id %d - table not found in metadata catalog",
	                            table_id.index);
}

string DuckLakeMetadataManager::GetPath(SchemaIndex schema_id) {
	lock_guard<mutex> guard(paths_lock);
	// get the path from the list of cached paths
	auto entry = schema_paths.find(schema_id);
	if (entry != schema_paths.end()) {
		return entry->second;
	}
	// get the path from the current snapshot if possible
	// otherwise fetch it from the metadata catalog
	auto &catalog = transaction.GetCatalog();
	auto schema = catalog.GetEntryById(transaction, transaction.GetSnapshot(), schema_id);
	string path;
	if (schema) {
		path = schema->Cast<DuckLakeSchemaEntry>().DataPath();
	} else {
		path = GetPathForSchema(schema_id);
	}
	schema_paths.emplace(schema_id, path);
	return path;
}

string DuckLakeMetadataManager::GetPath(TableIndex table_id) {
	lock_guard<mutex> guard(paths_lock);
	// get the path from the list of cached paths
	auto entry = table_paths.find(table_id);
	if (entry != table_paths.end()) {
		return entry->second;
	}
	// get the path from the current snapshot if possible
	auto &catalog = transaction.GetCatalog();
	auto table = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table_id);
	string path;
	if (table) {
		path = table->Cast<DuckLakeTableEntry>().DataPath();
	} else {
		path = GetPathForTable(table_id);
	}
	table_paths.emplace(table_id, path);
	return path;
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(const string &path) {
	auto &data_path = transaction.GetCatalog().DataPath();
	return GetRelativePath(path, data_path);
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(SchemaIndex schema_id, const string &path) {
	return GetRelativePath(path, GetPath(schema_id));
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(TableIndex table_id, const string &path) {
	return GetRelativePath(path, GetPath(table_id));
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(const string &path, const string &data_path) {
	DuckLakePath result;
	if (StringUtil::StartsWith(path, data_path)) {
		result.path = path.substr(data_path.size());
		result.path_is_relative = true;
	} else {
		result.path = path;
		result.path_is_relative = false;
	}
	result.path = StorePath(std::move(result.path));
	return result;
}

string DuckLakeMetadataManager::StorePath(string path) {
	auto &fs = GetFileSystem();
	auto separator = fs.PathSeparator(path);
	if (separator == "/") {
		return path;
	}
	return StringUtil::Replace(path, separator, "/");
}

string DuckLakeMetadataManager::LoadPath(string path) {
	auto &fs = GetFileSystem();
	auto separator = fs.PathSeparator(path);
	if (separator == "/") {
		return path;
	}
	return StringUtil::Replace(path, "/", separator);
}

string DuckLakeMetadataManager::FromRelativePath(const DuckLakePath &path, const string &base_path) {
	if (!path.path_is_relative) {
		return LoadPath(path.path);
	}
	return LoadPath(base_path + path.path);
}

string DuckLakeMetadataManager::FromRelativePath(const DuckLakePath &path) {
	return FromRelativePath(path, transaction.GetCatalog().DataPath());
}

string DuckLakeMetadataManager::FromRelativePath(TableIndex table_id, const DuckLakePath &path) {
	return FromRelativePath(path, GetPath(table_id));
}

void DuckLakeMetadataManager::WriteNewDataFiles(DuckLakeSnapshot commit_snapshot,
                                                const vector<DuckLakeFileInfo> &new_files) {
	if (new_files.empty()) {
		return;
	}
	string data_file_insert_query;
	string column_stats_insert_query;
	string partition_insert_query;

	for (auto &file : new_files) {
		if (!data_file_insert_query.empty()) {
			data_file_insert_query += ",";
		}
		auto row_id = file.row_id_start.IsValid() ? to_string(file.row_id_start.GetIndex()) : "NULL";
		auto partition_id = file.partition_id.IsValid() ? to_string(file.partition_id.GetIndex()) : "NULL";
		auto begin_snapshot =
		    file.begin_snapshot.IsValid() ? to_string(file.begin_snapshot.GetIndex()) : "{SNAPSHOT_ID}";
		auto data_file_index = file.id.index;
		auto table_id = file.table_id.index;
		auto encryption_key =
		    file.encryption_key.empty() ? "NULL" : "'" + Blob::ToBase64(string_t(file.encryption_key)) + "'";
		string partial_file_info = "NULL";
		if (!file.partial_file_info.empty()) {
			if (file.max_partial_file_snapshot.IsValid()) {
				throw InternalException("Either partial_file_info or max_partial_file_snapshot can be set - not both");
			}
			partial_file_info = "'" + PartialFileInfoToString(file.partial_file_info) + "'";
		} else if (file.max_partial_file_snapshot.IsValid()) {
			partial_file_info = "'partial_max:" + to_string(file.max_partial_file_snapshot.GetIndex()) + "'";
		}
		string footer_size = file.footer_size.IsValid() ? to_string(file.footer_size.GetIndex()) : "NULL";
		string mapping = file.mapping_id.IsValid() ? to_string(file.mapping_id.index) : "NULL";
		auto path = GetRelativePath(file.table_id, file.file_name);
		data_file_insert_query += StringUtil::Format(
		    "(%d, %d, %s, NULL, NULL, %s, %s, 'parquet', %d, %d, %s, %s, %s, %s, %s, %s)", data_file_index, table_id,
		    begin_snapshot, SQLString(path.path), path.path_is_relative ? "true" : "false", file.row_count,
		    file.file_size_bytes, footer_size, row_id, partition_id, encryption_key, partial_file_info, mapping);
		for (auto &column_stats : file.column_stats) {
			if (!column_stats_insert_query.empty()) {
				column_stats_insert_query += ",";
			}
			auto column_id = column_stats.column_id.index;
			column_stats_insert_query +=
			    StringUtil::Format("(%d, %d, %d, %s, %s, %s, %s, %s, %s)", data_file_index, table_id, column_id,
			                       column_stats.column_size_bytes, column_stats.value_count, column_stats.null_count,
			                       column_stats.min_val, column_stats.max_val, column_stats.contains_nan);
		}
		if (file.partition_id.IsValid() == file.partition_values.empty()) {
			throw InternalException("File should either not be partitioned, or have partition values");
		}
		for (auto &part_val : file.partition_values) {
			if (!partition_insert_query.empty()) {
				partition_insert_query += ",";
			}
			partition_insert_query +=
			    StringUtil::Format("(%d, %d, %d, %s)", data_file_index, table_id, part_val.partition_column_idx,
			                       SQLString(part_val.partition_value));
		}
	}
	if (data_file_insert_query.empty()) {
		throw InternalException("No files found!?");
	}
	// insert the data files
	data_file_insert_query =
	    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_data_file VALUES %s", data_file_insert_query);
	auto result = transaction.Query(commit_snapshot, data_file_insert_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write data file information to DuckLake: ");
	}
	// insert the column stats
	column_stats_insert_query = StringUtil::Format(
	    "INSERT INTO {METADATA_CATALOG}.ducklake_file_column_statistics VALUES %s", column_stats_insert_query);
	result = transaction.Query(commit_snapshot, column_stats_insert_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write column stats information to DuckLake: ");
	}
	if (!partition_insert_query.empty()) {
		// insert the partition values
		partition_insert_query = StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_file_partition_value VALUES %s", partition_insert_query);
		result = transaction.Query(commit_snapshot, partition_insert_query);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to write partition value information to DuckLake: ");
		}
	}
}

void DuckLakeMetadataManager::DropDataFiles(DuckLakeSnapshot commit_snapshot, const set<DataFileIndex> &dropped_files) {
	FlushDrop(commit_snapshot, "ducklake_data_file", "data_file_id", dropped_files);
}

void DuckLakeMetadataManager::DropDeleteFiles(DuckLakeSnapshot commit_snapshot,
                                              const set<DataFileIndex> &dropped_files) {
	FlushDrop(commit_snapshot, "ducklake_delete_file", "data_file_id", dropped_files);
}

void DuckLakeMetadataManager::WriteNewDeleteFiles(DuckLakeSnapshot commit_snapshot,
                                                  const vector<DuckLakeDeleteFileInfo> &new_files) {
	if (new_files.empty()) {
		return;
	}
	string delete_file_insert_query;
	for (auto &file : new_files) {
		if (!delete_file_insert_query.empty()) {
			delete_file_insert_query += ",";
		}
		auto delete_file_index = file.id.index;
		auto table_id = file.table_id.index;
		auto data_file_index = file.data_file_id.index;
		auto encryption_key =
		    file.encryption_key.empty() ? "NULL" : "'" + Blob::ToBase64(string_t(file.encryption_key)) + "'";
		auto path = GetRelativePath(file.table_id, file.path);
		delete_file_insert_query += StringUtil::Format(
		    "(%d, %d, {SNAPSHOT_ID}, NULL, %d, %s, %s, 'parquet', %d, %d, %d, %s)", delete_file_index, table_id,
		    data_file_index, SQLString(path.path), path.path_is_relative ? "true" : "false", file.delete_count,
		    file.file_size_bytes, file.footer_size, encryption_key);
	}

	// insert the data files
	delete_file_insert_query =
	    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_delete_file VALUES %s", delete_file_insert_query);
	auto result = transaction.Query(commit_snapshot, delete_file_insert_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write delete file information to DuckLake: ");
	}
}

vector<DuckLakeColumnMappingInfo> DuckLakeMetadataManager::GetColumnMappings(optional_idx start_from) {
	string filter;
	if (start_from.IsValid()) {
		filter = "WHERE mapping_id >= " + to_string(start_from.GetIndex());
	}
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT mapping_id, table_id, type, column_id, source_name, target_field_id, parent_column, is_partition
FROM {METADATA_CATALOG}.ducklake_column_mapping
JOIN {METADATA_CATALOG}.ducklake_name_mapping USING (mapping_id)
%s
ORDER BY mapping_id, parent_column NULLS FIRST
)",
	                                                   filter));
	vector<DuckLakeColumnMappingInfo> column_maps;
	for (auto &row : *result) {
		MappingIndex mapping_id(row.GetValue<idx_t>(0));
		if (column_maps.empty() || column_maps.back().mapping_id != mapping_id) {
			DuckLakeColumnMappingInfo mapping_info;
			mapping_info.mapping_id = mapping_id;
			mapping_info.table_id = TableIndex(row.GetValue<idx_t>(1));
			mapping_info.map_type = row.GetValue<string>(2);
			column_maps.push_back(std::move(mapping_info));
		}
		auto &mapping_info = column_maps.back();
		DuckLakeNameMapColumnInfo name_map_column;
		name_map_column.column_id = row.GetValue<idx_t>(3);
		name_map_column.source_name = row.GetValue<string>(4);
		name_map_column.target_field_id = FieldIndex(row.GetValue<idx_t>(5));
		if (!row.IsNull(6)) {
			name_map_column.parent_column = row.GetValue<idx_t>(6);
		}
		name_map_column.hive_partition = row.GetValue<bool>(7);
		mapping_info.map_columns.push_back(std::move(name_map_column));
	}
	return column_maps;
}

void DuckLakeMetadataManager::WriteNewColumnMappings(DuckLakeSnapshot commit_snapshot,
                                                     const vector<DuckLakeColumnMappingInfo> &new_column_mappings) {
	string column_mapping_insert_query;
	string name_map_insert_query;
	for (auto &column_mapping : new_column_mappings) {
		if (!column_mapping_insert_query.empty()) {
			column_mapping_insert_query += ", ";
		}
		column_mapping_insert_query +=
		    StringUtil::Format("(%d, %d, %s)", column_mapping.mapping_id.index, column_mapping.table_id.index,
		                       SQLString(column_mapping.map_type));
		for (auto &name_map_column : column_mapping.map_columns) {
			if (!name_map_insert_query.empty()) {
				name_map_insert_query += ", ";
			}
			string parent_column =
			    name_map_column.parent_column.IsValid() ? to_string(name_map_column.parent_column.GetIndex()) : "NULL";
			string is_partition = name_map_column.hive_partition ? "true" : "false";
			name_map_insert_query +=
			    StringUtil::Format("(%d, %d, %s, %d, %s, %s)", column_mapping.mapping_id.index,
			                       name_map_column.column_id, SQLString(name_map_column.source_name),
			                       name_map_column.target_field_id.index, parent_column, is_partition);
		}
	}
	column_mapping_insert_query =
	    "INSERT INTO {METADATA_CATALOG}.ducklake_column_mapping VALUES " + column_mapping_insert_query;
	auto result = transaction.Query(commit_snapshot, column_mapping_insert_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new column mapping information to DuckLake: ");
	}
	name_map_insert_query = "INSERT INTO {METADATA_CATALOG}.ducklake_name_mapping VALUES " + name_map_insert_query;
	result = transaction.Query(commit_snapshot, name_map_insert_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new column mapping information to DuckLake: ");
	}
}

void DuckLakeMetadataManager::InsertSnapshot(DuckLakeSnapshot commit_snapshot) {
	auto result = transaction.Query(
	    std::move(commit_snapshot),
	    R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES ({SNAPSHOT_ID}, NOW(), {SCHEMA_VERSION}, {NEXT_CATALOG_ID}, {NEXT_FILE_ID});)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new snapshot to DuckLake: ");
	}
}

string SQLStringOrNull(const string &str) {
	if (str.empty()) {
		return "NULL";
	}
	return KeywordHelper::WriteQuoted(str, '\'');
}

void DuckLakeMetadataManager::WriteSnapshotChanges(DuckLakeSnapshot commit_snapshot,
                                                   const SnapshotChangeInfo &change_info,
                                                   const DuckLakeSnapshotCommit &commit_info) {
	// insert the snapshot changes
	auto query = StringUtil::Format(
	    R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES ({SNAPSHOT_ID}, %s, %s, %s, %s);)",
	    SQLStringOrNull(change_info.changes_made), commit_info.author.ToSQLString(),
	    commit_info.commit_message.ToSQLString(), commit_info.commit_extra_info.ToSQLString());
	auto result = transaction.Query(commit_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to write new snapshot to DuckLake:");
	}
}

SnapshotChangeInfo DuckLakeMetadataManager::GetChangesMadeAfterSnapshot(DuckLakeSnapshot start_snapshot) {
	// get all changes made to the system after the snapshot was started
	auto result = transaction.Query(start_snapshot, R"(
	SELECT COALESCE(STRING_AGG(changes_made), '')
	FROM {METADATA_CATALOG}.ducklake_snapshot_changes
	WHERE snapshot_id > {SNAPSHOT_ID}
	)");
	if (result->HasError()) {
		result->GetErrorObject().Throw(
		    "Failed to commit DuckLake transaction - failed to get snapshot changes for conflict resolution:");
	}
	// parse changes made by other transactions
	SnapshotChangeInfo change_info;
	for (auto &row : *result) {
		change_info.changes_made = row.GetValue<string>(0);
	}
	return change_info;
}

unique_ptr<DuckLakeSnapshot> TryGetSnapshotInternal(QueryResult &result) {
	unique_ptr<DuckLakeSnapshot> snapshot;
	for (auto &row : result) {
		if (snapshot) {
			throw InvalidInputException("Corrupt DuckLake - multiple snapshots returned from database");
		}
		auto snapshot_id = row.GetValue<idx_t>(0);
		auto schema_version = row.GetValue<idx_t>(1);
		auto next_catalog_id = row.GetValue<idx_t>(2);
		auto next_file_id = row.GetValue<idx_t>(3);
		snapshot = make_uniq<DuckLakeSnapshot>(snapshot_id, schema_version, next_catalog_id, next_file_id);
	}
	return snapshot;
}

string DuckLakeMetadataManager::GetLatestSnapshotQuery() const {
	return R"(SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM {METADATA_CATALOG}.ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM {METADATA_CATALOG}.ducklake_snapshot);)";
}

unique_ptr<DuckLakeSnapshot> DuckLakeMetadataManager::GetSnapshot() {
	auto result = transaction.Query(GetLatestSnapshotQuery());
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to query most recent snapshot for DuckLake: ");
	}
	auto snapshot = TryGetSnapshotInternal(*result);
	if (!snapshot) {
		throw InvalidInputException("No snapshot found in DuckLake");
	}
	return snapshot;
}

unique_ptr<DuckLakeSnapshot> DuckLakeMetadataManager::GetSnapshot(BoundAtClause &at_clause) {
	auto &unit = at_clause.Unit();
	auto &val = at_clause.GetValue();
	unique_ptr<QueryResult> result;
	if (StringUtil::CIEquals(unit, "version")) {
		result = transaction.Query(StringUtil::Format(R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_CATALOG}.ducklake_snapshot
WHERE snapshot_id = %llu;)",
		                                              val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>()));
	} else if (StringUtil::CIEquals(unit, "timestamp")) {
		result = transaction.Query(StringUtil::Format(R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_CATALOG}.ducklake_snapshot
WHERE snapshot_id = (
	SELECT MAX_BY(snapshot_id, snapshot_time)
	FROM {METADATA_CATALOG}.ducklake_snapshot
	WHERE snapshot_time <= %s);)",
		                                              val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString()));
		auto result_2 = transaction.Query(
		    StringUtil::Format(R"(SELECT snapshot_id, snapshot_time FROM {METADATA_CATALOG}.ducklake_snapshot)"));
		idx_t i = 0;
	} else {
		throw InvalidInputException("Unsupported AT clause unit - %s", unit);
	}
	if (result->HasError()) {
		result->GetErrorObject().Throw(StringUtil::Format(
		    "Failed to query snapshot at %s %s for DuckLake: ", StringUtil::Lower(unit), val.ToString()));
	}
	auto snapshot = TryGetSnapshotInternal(*result);
	if (!snapshot) {
		throw InvalidInputException("No snapshot found at %s %s", StringUtil::Lower(unit), val.ToString());
	}
	return snapshot;
}

unordered_map<idx_t, DuckLakePartitionInfo> GetNewPartitions(const vector<DuckLakePartitionInfo> &old_partitions,
                                                             const vector<DuckLakePartitionInfo> &new_partitions) {

	unordered_map<idx_t, DuckLakePartitionInfo> new_partition_map;

	for (auto &partition : new_partitions) {
		new_partition_map[partition.table_id.index] = partition;
	}

	unordered_set<idx_t> old_partition_set;
	for (auto &partition : old_partitions) {
		old_partition_set.insert(partition.table_id.index);
		if (new_partition_map.find(partition.table_id.index) != new_partition_map.end()) {
			if (new_partition_map[partition.table_id.index] == partition) {
				// If a new partition already exists in an old partition, it's a nop, we can remove it
				new_partition_map.erase(partition.table_id.index);
			}
		}
	}

	vector<idx_t> partition_ids_to_erase;
	for (auto &partition : new_partitions) {
		if (old_partition_set.find(partition.table_id.index) == old_partition_set.end() && partition.fields.empty()) {
			// If a map does not exist on the old partition and the partition has no fields, this is an reset over
			// and empty partition definition, hence also a nop
			partition_ids_to_erase.push_back(partition.table_id.index);
		}
	}
	for (auto &id : partition_ids_to_erase) {
		new_partition_map.erase(id);
	}
	return new_partition_map;
}

void DuckLakeMetadataManager::WriteNewPartitionKeys(DuckLakeSnapshot commit_snapshot,
                                                    const vector<DuckLakePartitionInfo> &new_partitions) {
	if (new_partitions.empty()) {
		return;
	}
	auto catalog = GetCatalogForSnapshot(commit_snapshot);

	string old_partition_table_ids;
	string new_partition_values;
	string insert_partition_cols;

	auto new_partition_map = GetNewPartitions(catalog.partitions, new_partitions);
	if (new_partition_map.empty()) {
		return;
	}
	for (auto &new_partition : new_partition_map) {
		// set old partition data as no longer valid
		if (!old_partition_table_ids.empty()) {
			old_partition_table_ids += ", ";
		}
		old_partition_table_ids += to_string(new_partition.second.table_id.index);
		if (!new_partition.second.id.IsValid()) {
			// dropping partition data - we don't need to do anything
			return;
		}
		auto partition_id = new_partition.second.id.GetIndex();
		if (!new_partition_values.empty()) {
			new_partition_values += ", ";
		}
		new_partition_values +=
		    StringUtil::Format(R"((%d, %d, {SNAPSHOT_ID}, NULL))", partition_id, new_partition.second.table_id.index);
		for (auto &field : new_partition.second.fields) {
			if (!insert_partition_cols.empty()) {
				insert_partition_cols += ", ";
			}
			insert_partition_cols +=
			    StringUtil::Format("(%d, %d, %d, %d, %s)", partition_id, new_partition.second.table_id.index,
			                       field.partition_key_index, field.field_id.index, SQLString(field.transform));
		}
	}
	// update old partition information for any tables that have been altered
	auto update_partition_query = StringUtil::Format(R"(
UPDATE {METADATA_CATALOG}.ducklake_partition_info
SET end_snapshot = {SNAPSHOT_ID}
WHERE table_id IN (%s) AND end_snapshot IS NULL)",
	                                                 old_partition_table_ids);
	auto result = transaction.Query(commit_snapshot, update_partition_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to update old partition information in DuckLake: ");
	}
	if (!new_partition_values.empty()) {
		new_partition_values = "INSERT INTO {METADATA_CATALOG}.ducklake_partition_info VALUES " + new_partition_values;
		auto result = transaction.Query(commit_snapshot, new_partition_values);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to insert new partition information in DuckLake: ");
		}
	}
	if (!insert_partition_cols.empty()) {
		insert_partition_cols =
		    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_column VALUES " + insert_partition_cols;

		auto result = transaction.Query(commit_snapshot, insert_partition_cols);
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to insert new partition information in DuckLake:");
		}
	}
}

void DuckLakeMetadataManager::WriteNewTags(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTagInfo> &new_tags) {
	if (new_tags.empty()) {
		return;
	}
	// update old tags (if there were any)
	// get a list of all tags
	string tags_list;
	for (auto &tag : new_tags) {
		if (!tags_list.empty()) {
			tags_list += ", ";
		}
		tags_list += StringUtil::Format("(%d, %s)", tag.id, SQLString(tag.key));
	}
	// overwrite the snapshot for the old tags
	auto result = transaction.Query(commit_snapshot, StringUtil::Format(R"(
WITH overwritten_tags(tid, key) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_tag
SET end_snapshot = {SNAPSHOT_ID}
FROM overwritten_tags
WHERE object_id=tid
)",
	                                                                    tags_list));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to update tag information in DuckLake: ");
	}
	// now insert the new tags
	string new_tag_query;
	for (auto &tag : new_tags) {
		if (!new_tag_query.empty()) {
			new_tag_query += ", ";
		}
		new_tag_query += StringUtil::Format("(%d, {SNAPSHOT_ID}, NULL, %s, %s)", tag.id, SQLString(tag.key),
		                                    tag.value.ToSQLString());
	}

	new_tag_query = "INSERT INTO {METADATA_CATALOG}.ducklake_tag VALUES " + new_tag_query;

	result = transaction.Query(commit_snapshot, new_tag_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to insert new tag information in DuckLake:");
	}
}

void DuckLakeMetadataManager::WriteNewColumnTags(DuckLakeSnapshot commit_snapshot,
                                                 const vector<DuckLakeColumnTagInfo> &new_tags) {
	if (new_tags.empty()) {
		return;
	}
	// update old tags (if there were any)
	// get a list of all tags
	string tags_list;
	for (auto &tag : new_tags) {
		if (!tags_list.empty()) {
			tags_list += ", ";
		}
		tags_list += StringUtil::Format("(%d, %d, %s)", tag.table_id.index, tag.field_index.index, SQLString(tag.key));
	}
	// overwrite the snapshot for the old tags
	auto result = transaction.Query(commit_snapshot, StringUtil::Format(R"(
WITH overwritten_tags(tid, cid, key) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_column_tag
SET end_snapshot = {SNAPSHOT_ID}
FROM overwritten_tags
WHERE table_id=tid AND column_id=cid
)",
	                                                                    tags_list));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to update column tag information in DuckLake: ");
	}
	// now insert the new tags
	string new_tag_query;
	for (auto &tag : new_tags) {
		if (!new_tag_query.empty()) {
			new_tag_query += ", ";
		}
		new_tag_query += StringUtil::Format("(%d, %d, {SNAPSHOT_ID}, NULL, %s, %s)", tag.table_id.index,
		                                    tag.field_index.index, SQLString(tag.key), tag.value.ToSQLString());
	}

	new_tag_query = "INSERT INTO {METADATA_CATALOG}.ducklake_column_tag VALUES " + new_tag_query;

	result = transaction.Query(commit_snapshot, new_tag_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to insert new column tag information in DuckLake:");
	}
}

void DuckLakeMetadataManager::UpdateGlobalTableStats(const DuckLakeGlobalStatsInfo &stats) {
	string column_stats_values;
	for (auto &col_stats : stats.column_stats) {
		if (!column_stats_values.empty()) {
			column_stats_values += ",";
		}
		string contains_null;
		if (col_stats.has_contains_null) {
			contains_null = col_stats.contains_null ? "true" : "false";
		} else {
			contains_null = "NULL";
		}
		string contains_nan;
		if (col_stats.has_contains_nan) {
			contains_nan = col_stats.contains_nan ? "true" : "false";
		} else {
			contains_nan = "NULL";
		}
		string min_val = col_stats.has_min ? DuckLakeUtil::StatsToString(col_stats.min_val) : "NULL";
		string max_val = col_stats.has_max ? DuckLakeUtil::StatsToString(col_stats.max_val) : "NULL";
		column_stats_values +=
		    StringUtil::Format("(%d, %d, %s, %s, %s, %s)", stats.table_id.index, col_stats.column_id.index,
		                       contains_null, contains_nan, min_val, max_val);
	}

	if (!stats.initialized) {
		// stats have not been initialized yet - insert them
		auto result = transaction.Query(
		    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_stats VALUES (%d, %d, %d, %d);",
		                       stats.table_id.index, stats.record_count, stats.next_row_id, stats.table_size_bytes));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to insert stats information in DuckLake: ");
		}

		result = transaction.Query(StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats VALUES %s;", column_stats_values));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to insert stats information in DuckLake: ");
		}
		return;
	}
	// stats have been initialized - update them
	auto result = transaction.Query(
	    StringUtil::Format("UPDATE {METADATA_CATALOG}.ducklake_table_stats SET record_count=%d, file_size_bytes=%d, "
	                       "next_row_id=%d WHERE table_id=%d;",
	                       stats.record_count, stats.table_size_bytes, stats.next_row_id, stats.table_id.index));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to update stats information in DuckLake: ");
	}
	result = transaction.Query(StringUtil::Format(R"(
WITH new_values(tid, cid, new_contains_null, new_contains_nan, new_min, new_max) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_table_column_stats
SET contains_null=new_contains_null, contains_nan=new_contains_nan, min_value=new_min, max_value=new_max
FROM new_values
WHERE table_id=tid AND column_id=cid
)",
	                                              column_stats_values));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to update stats information in DuckLake: ");
	}
}

template <class T>
timestamp_tz_t GetTimestampTZFromRow(ClientContext &context, const T &row, idx_t col_idx) {
	auto val = row.iterator.chunk->GetValue(col_idx, row.row);
	return val.CastAs(context, LogicalType::TIMESTAMP_TZ).template GetValue<timestamp_tz_t>();
}

vector<DuckLakeSnapshotInfo> DuckLakeMetadataManager::GetAllSnapshots(const string &filter) {

	auto res = transaction.Query(StringUtil::Format(R"(
SELECT snapshot_id, snapshot_time, schema_version, changes_made, author, commit_message, commit_extra_info
FROM {METADATA_CATALOG}.ducklake_snapshot
LEFT JOIN {METADATA_CATALOG}.ducklake_snapshot_changes USING (snapshot_id)
%s %s
ORDER BY snapshot_id
)",
	                                                filter.empty() ? "" : "WHERE", filter));
	if (res->HasError()) {
		res->GetErrorObject().Throw("Failed to get snapshot information from DuckLake: ");
	}
	auto context = transaction.context.lock();
	vector<DuckLakeSnapshotInfo> snapshots;

	for (auto &row : *res) {
		DuckLakeSnapshotInfo snapshot_info;
		snapshot_info.id = row.GetValue<idx_t>(0);
		snapshot_info.time = GetTimestampTZFromRow(*context, row, 1);
		snapshot_info.schema_version = row.GetValue<idx_t>(2);
		snapshot_info.change_info.changes_made = row.IsNull(3) ? string() : row.GetValue<string>(3);
		snapshot_info.author = row.iterator.chunk->GetValue(4, row.row);
		snapshot_info.commit_message = row.iterator.chunk->GetValue(5, row.row);
		snapshot_info.commit_extra_info = row.iterator.chunk->GetValue(6, row.row);
		snapshots.push_back(std::move(snapshot_info));
	}
	return snapshots;
}

vector<DuckLakeFileScheduledForCleanup> DuckLakeMetadataManager::GetFilesScheduledForCleanup(const string &filter) {
	auto res = transaction.Query(R"(
SELECT data_file_id, path, path_is_relative, schedule_start
FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
)" + filter);
	if (res->HasError()) {
		res->GetErrorObject().Throw("Failed to get files scheduled for deletion from DuckLake: ");
	}
	auto context = transaction.context.lock();
	vector<DuckLakeFileScheduledForCleanup> result;
	for (auto &row : *res) {
		DuckLakeFileScheduledForCleanup info;
		info.id = DataFileIndex(row.GetValue<idx_t>(0));
		DuckLakePath path;
		path.path = row.GetValue<string>(1);
		path.path_is_relative = row.GetValue<bool>(2);
		info.path = FromRelativePath(path);
		info.time = GetTimestampTZFromRow(*context, row, 3);

		result.push_back(std::move(info));
	}
	return result;
}

void DuckLakeMetadataManager::RemoveFilesScheduledForCleanup(
    const vector<DuckLakeFileScheduledForCleanup> &cleaned_up_files) {
	string deleted_file_ids;
	for (auto &file : cleaned_up_files) {
		if (!deleted_file_ids.empty()) {
			deleted_file_ids += ", ";
		}
		deleted_file_ids += to_string(file.id.index);
	}
	auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
WHERE data_file_id IN (%s);
)",
	                                                   deleted_file_ids));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to delete scheduled cleanup files in DuckLake: ");
	}
}

idx_t DuckLakeMetadataManager::GetNextColumnId(TableIndex table_id) {
	auto result = transaction.Query(StringUtil::Format(R"(
	SELECT MAX(column_id)
	FROM {METADATA_CATALOG}.ducklake_column
	WHERE table_id=%d
)",
	                                                   table_id.index));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get next column id in DuckLake: ");
	}
	for (auto &row : *result) {
		if (row.IsNull(0)) {
			break;
		}
		return row.GetValue<idx_t>(00) + 1;
	}
	throw InternalException("Invalid result for GetNextColumnId");
}

void DuckLakeMetadataManager::WriteCompactions(const vector<DuckLakeCompactedFileInfo> &compactions) {
	if (compactions.empty()) {
		return;
	}
	string deleted_file_ids;
	string scheduled_deletions;
	for (auto &compaction : compactions) {
		D_ASSERT(!compaction.path.empty());
		// add data file id to list of files to delete
		if (!deleted_file_ids.empty()) {
			deleted_file_ids += ", ";
		}
		deleted_file_ids += to_string(compaction.source_id.index);

		// schedule the file for deletion
		if (!scheduled_deletions.empty()) {
			scheduled_deletions += ", ";
		}
		auto path = GetRelativePath(compaction.path);
		scheduled_deletions += StringUtil::Format("(%d, %s, %s, NOW())", compaction.source_id.index,
		                                          SQLString(path.path), path.path_is_relative ? "true" : "false");
	}
	// for each file that has been compacted - delete it from the list of data files entirely
	// including all other info (stats, delete files, partition values, etc)
	vector<string> tables_to_delete_from {"ducklake_data_file", "ducklake_file_column_statistics",
	                                      "ducklake_delete_file", "ducklake_file_partition_value"};
	for (auto &delete_from_tbl : tables_to_delete_from) {
		auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE data_file_id IN (%s);
)",
		                                                   delete_from_tbl, deleted_file_ids));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete old data file information in DuckLake: ");
		}
	}
	// add the files we cleared to the deletion schedule
	scheduled_deletions =
	    "INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion VALUES " + scheduled_deletions;
	auto result = transaction.Query(scheduled_deletions);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to insert files scheduled for deletions in DuckLake: ");
	}
}

void DuckLakeMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	unique_ptr<QueryResult> result;
	// first delete the actual snapshots
	string snapshot_ids;
	for (auto &snapshot : snapshots) {
		if (!snapshot_ids.empty()) {
			snapshot_ids += ", ";
		}
		snapshot_ids += to_string(snapshot.id);
	}
	vector<string> tables_to_delete_from {"ducklake_snapshot", "ducklake_snapshot_changes"};
	for (auto &delete_tbl : tables_to_delete_from) {
		result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE snapshot_id IN (%s);
)",
		                                              delete_tbl, snapshot_ids));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete snapshots in DuckLake: ");
		}
	}
	// get a list of tables that are no longer required after these deletions
	result = transaction.Query(R"(
SELECT table_id
FROM {METADATA_CATALOG}.ducklake_table
WHERE end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
);)");
	vector<TableIndex> cleanup_tables;
	for (auto &row : *result) {
		cleanup_tables.push_back(TableIndex(row.GetValue<idx_t>(0)));
	}
	string deleted_table_ids;
	for (auto &table_id : cleanup_tables) {
		if (!deleted_table_ids.empty()) {
			deleted_table_ids += ", ";
		}
		deleted_table_ids += to_string(table_id.index);
	}

	// get a list of files that are no longer required after these deletions
	string table_id_filter;
	if (!deleted_table_ids.empty()) {
		table_id_filter = StringUtil::Format("table_id IN (%s) OR", deleted_table_ids);
	}

	result = transaction.Query(StringUtil::Format(R"(
SELECT data_file_id, table_id, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_data_file
WHERE %s (end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
));)",
	                                              table_id_filter));
	vector<DuckLakeFileScheduledForCleanup> cleanup_files;
	for (auto &row : *result) {
		DuckLakeFileScheduledForCleanup info;
		info.id = DataFileIndex(row.GetValue<idx_t>(0));
		TableIndex table_id(row.GetValue<idx_t>(1));
		DuckLakePath path;
		path.path = row.GetValue<string>(2);
		path.path_is_relative = row.GetValue<bool>(3);
		info.path = FromRelativePath(table_id, path);

		cleanup_files.push_back(std::move(info));
	}
	string deleted_file_ids;
	if (!cleanup_files.empty()) {
		string files_scheduled_for_cleanup;
		for (auto &file : cleanup_files) {
			if (!deleted_file_ids.empty()) {
				deleted_file_ids += ", ";
			}
			deleted_file_ids += to_string(file.id.index);

			if (!files_scheduled_for_cleanup.empty()) {
				files_scheduled_for_cleanup += ", ";
			}
			auto path = GetRelativePath(file.path);
			files_scheduled_for_cleanup += StringUtil::Format(
			    "(%d, %s, %s, NOW())", file.id.index, SQLString(path.path), path.path_is_relative ? "true" : "false");
		}

		// delete the data files
		tables_to_delete_from = {"ducklake_data_file", "ducklake_file_column_statistics"};
		for (auto &delete_tbl : tables_to_delete_from) {
			result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE data_file_id IN (%s);
)",
			                                              delete_tbl, deleted_file_ids));
			if (result->HasError()) {
				result->GetErrorObject().Throw("Failed to delete old data file information in DuckLake: ");
			}
		}
		// insert the to-be-cleaned-up files
		result = transaction.Query(StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
VALUES %s;
)",
		                                              files_scheduled_for_cleanup));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to schedule files for clean-up in DuckLake: ");
		}
	}

	// get a list of delete files that are no longer required after these deletions
	string file_id_filter;
	if (!deleted_file_ids.empty()) {
		file_id_filter = StringUtil::Format("data_file_id IN (%s) OR", deleted_file_ids);
	}

	result = transaction.Query(StringUtil::Format(R"(
SELECT delete_file_id, table_id, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_delete_file
WHERE %s %s (end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
));)",
	                                              table_id_filter, file_id_filter));
	vector<DuckLakeFileScheduledForCleanup> cleanup_deletes;
	for (auto &row : *result) {
		DuckLakeFileScheduledForCleanup info;
		info.id = DataFileIndex(row.GetValue<idx_t>(0));
		TableIndex table_id(row.GetValue<idx_t>(1));

		DuckLakePath path;
		path.path = row.GetValue<string>(2);
		path.path_is_relative = row.GetValue<bool>(3);
		info.path = FromRelativePath(table_id, path);

		cleanup_deletes.push_back(std::move(info));
	}
	if (!cleanup_deletes.empty()) {
		string deleted_delete_ids;
		string files_scheduled_for_cleanup;
		for (auto &file : cleanup_deletes) {
			if (!deleted_delete_ids.empty()) {
				deleted_delete_ids += ", ";
			}
			deleted_delete_ids += to_string(file.id.index);

			if (!files_scheduled_for_cleanup.empty()) {
				files_scheduled_for_cleanup += ", ";
			}
			auto path = GetRelativePath(file.path);
			files_scheduled_for_cleanup += StringUtil::Format(
			    "(%d, %s, %s, NOW())", file.id.index, SQLString(path.path), path.path_is_relative ? "true" : "false");
		}
		// delete the delete files
		result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_delete_file
WHERE delete_file_id IN (%s);
)",
		                                              deleted_delete_ids));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete old delete file information in DuckLake: ");
		}
		// insert the to-be-cleaned-up files
		result = transaction.Query(StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
VALUES %s;
)",
		                                              files_scheduled_for_cleanup));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to schedule files for clean-up in DuckLake: ");
		}
	}

	// delete based on table id -> ducklake_table_stats, ducklake_table_column_stats, ducklake_partition_info
	if (!deleted_table_ids.empty()) {
		tables_to_delete_from = {"ducklake_table",          "ducklake_table_stats",      "ducklake_table_column_stats",
		                         "ducklake_partition_info", "ducklake_partition_column", "ducklake_column",
		                         "ducklake_column_tag"};
		for (auto &delete_tbl : tables_to_delete_from) {
			auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE table_id IN (%s);)",
			                                                   delete_tbl, deleted_table_ids));
			if (result->HasError()) {
				result->GetErrorObject().Throw("Failed to delete from " + delete_tbl + " in DuckLake: ");
			}
		}
	}

	// delete any views, schemas, etc that are no longer referenced
	tables_to_delete_from = {"ducklake_schema", "ducklake_view", "ducklake_tag"};
	for (auto &delete_tbl : tables_to_delete_from) {
		auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
);)",
		                                                   delete_tbl));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete from " + delete_tbl + " in DuckLake: ");
		}
	}
}

void DuckLakeMetadataManager::DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table) {
	auto result = transaction.Query(StringUtil::Format(R"(
		DELETE FROM {METADATA_CATALOG}.%s
)",
	                                                   SQLIdentifier(inlined_table.table_name)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to delete inlined data in DuckLake from table " +
		                               inlined_table.table_name + ": ");
	}
}

vector<DuckLakeTableSizeInfo> DuckLakeMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	vector<DuckLakeTableSizeInfo> table_sizes;
	auto result = transaction.Query(snapshot, R"(
SELECT schema_id, table_id, table_name, table_uuid, data_file_info.file_count, data_file_info.total_file_size, delete_file_info.file_count, delete_file_info.total_file_size
FROM {METADATA_CATALOG}.ducklake_table tbl, LATERAL (
	SELECT COUNT(*) file_count, SUM(file_size_bytes) total_file_size
	FROM {METADATA_CATALOG}.ducklake_data_file df
	WHERE df.table_id = tbl.table_id AND {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
) data_file_info, LATERAL (
	SELECT COUNT(*) file_count, SUM(file_size_bytes) total_file_size
	FROM {METADATA_CATALOG}.ducklake_delete_file df
	WHERE df.table_id = tbl.table_id AND {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
) delete_file_info
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
)");
	for (auto &row : *result) {
		DuckLakeTableSizeInfo table_size;
		table_size.schema_id = SchemaIndex(row.GetValue<idx_t>(0));
		table_size.table_id = TableIndex(row.GetValue<idx_t>(1));
		table_size.table_name = row.GetValue<string>(2);
		table_size.table_uuid = row.GetValue<string>(3);
		if (!row.IsNull(4)) {
			table_size.file_count = row.GetValue<idx_t>(4);
		}
		if (!row.IsNull(5)) {
			table_size.file_size_bytes = row.GetValue<idx_t>(5);
		}
		if (!row.IsNull(6)) {
			table_size.delete_file_count = row.GetValue<idx_t>(6);
		}
		if (!row.IsNull(7)) {
			table_size.delete_file_size_bytes = row.GetValue<idx_t>(7);
		}
		table_sizes.push_back(std::move(table_size));
	}
	return table_sizes;
}

void DuckLakeMetadataManager::SetConfigOption(const DuckLakeConfigOption &option) {
	// check if the option already exists
	auto &option_key = option.option.key;
	auto &option_value = option.option.value;
	string scope;
	string scope_id;
	string scope_filter;
	if (option.table_id.IsValid()) {
		scope = "'table'";
		scope_id = to_string(option.table_id.index);
		scope_filter = StringUtil::Format("scope = 'table' AND scope_id = %d", option.table_id.index);
	} else if (option.schema_id.IsValid()) {
		scope = "'schema'";
		scope_id = to_string(option.schema_id.index);
		scope_filter = StringUtil::Format("scope = 'schema' AND scope_id = %d", option.schema_id.index);
	} else {
		scope = "NULL";
		scope_id = "NULL";
		scope_filter = "scope IS NULL";
	}
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT COUNT(*)
FROM {METADATA_CATALOG}.ducklake_metadata
WHERE key = %s AND %s
)",
	                                                   SQLString(option_key), scope_filter));

	auto count = result->Fetch()->GetValue(0, 0).GetValue<idx_t>();
	if (count == 0) {
		// option does not yet exist - insert the value
		result = transaction.Query(StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_metadata VALUES (%s, %s, %s, %s)
)",
		                                              SQLString(option_key), SQLString(option_value), scope, scope_id));
	} else {
		// option already exists - update it
		result = transaction.Query(StringUtil::Format(R"(
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value=%s WHERE key=%s AND %s
)",
		                                              SQLString(option_value), SQLString(option_key), scope_filter));
	}
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to insert config option in DuckLake: ");
	}
}

bool DuckLakeMetadataManager::IsEncrypted() const {
	return transaction.GetCatalog().Encryption() == DuckLakeEncryption::ENCRYPTED;
}

} // namespace duckdb
