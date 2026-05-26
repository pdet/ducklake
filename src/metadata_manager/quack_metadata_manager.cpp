#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_staged_commit.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

static bool AddChildColumn(vector<DuckLakeColumnInfo> &columns, FieldIndex parent_id, DuckLakeColumnInfo &column_info) {
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

QuackMetadataManager::QuackMetadataManager(DuckLakeTransaction &transaction) : DuckLakeMetadataManager(transaction) {
}

unique_ptr<QueryResult> QuackMetadataManager::Query(string &query) {
	round_trip_count++;
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	SubstituteCatalogPlaceholders(query);

	auto metadata_catalog_name_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto wrapper = StringUtil::Format("CALL system.main.quack_query_by_name(%s, %s)", metadata_catalog_name_literal,
	                                  SQLString(query));
	auto result = transaction.ExecuteRaw(std::move(wrapper));
	if (result->HasError()) {
		string reset = "ROLLBACK; BEGIN TRANSACTION;";
		transaction.ExecuteRaw(reset);
	}
	return result;
}

unique_ptr<QueryResult> QuackMetadataManager::AttachMetadata(const string &attach_query) {
	round_trip_count++;
	auto query = attach_query;
	SubstituteCatalogPlaceholders(query);
	Connection fresh_conn(transaction.GetCatalog().GetDatabase());
	auto result = fresh_conn.Query(query);

	for (idx_t attempt = 0; attempt < 5 && result->HasError(); attempt++) {
		auto raw_message = result->GetErrorObject().RawMessage();
		const bool retryable = StringUtil::Contains(raw_message, "Invalid connection id") ||
		                       StringUtil::Contains(raw_message, "Couldn't connect to server") ||
		                       StringUtil::Contains(raw_message, "Failed to send message");
		if (!retryable) {
			break;
		}
		result = fresh_conn.Query(query);
	}
	return result;
}

unique_ptr<QueryResult> QuackMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	SubstituteSnapshotPlaceholders(snapshot, query);
	return Query(query);
}

unique_ptr<QueryResult> QuackMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return Query(snapshot, query);
}

string QuackMetadataManager::MetadataExistsQuery() const {
	return "SELECT COUNT(*) FROM information_schema.tables "
	       "WHERE table_name = 'ducklake_metadata' AND table_schema = {METADATA_SCHEMA_NAME_LITERAL}";
}

void QuackMetadataManager::ClearCache() {
	string clear = "CALL quack_clear_cache();";
	transaction.ExecuteRaw(clear);
}

//===--------------------------------------------------------------------===//
// Combined Phase 1: MetadataExists + LoadDuckLake + ProbeServerCapabilities
//===--------------------------------------------------------------------===//

void QuackMetadataManager::RunCombinedInit() {
	if (cached_init.valid) {
		return;
	}
	string query = R"(
SELECT
    (SELECT COUNT(*) FROM duckdb_secrets()) AS _secrets_probe,
    (SELECT COUNT(*) FROM duckdb_functions()
     WHERE function_name = 'ducklake_commit' LIMIT 1) AS has_ducklake_commit,
    (SELECT LIST(struct_pack(key := key, value := value, scope := scope, scope_id := scope_id))
     FROM {METADATA_CATALOG}.ducklake_metadata) AS metadata_entries
)";
	auto result = Query(query);
	if (result->HasError()) {
		auto &error_obj = result->GetErrorObject();
		auto msg = error_obj.RawMessage();
		if (error_obj.Type() == ExceptionType::CATALOG ||
		    StringUtil::Contains(msg, "does not exist") ||
		    StringUtil::Contains(msg, "Table with name")) {
			cached_init.valid = true;
			cached_init.metadata_exists = false;
			cached_init.has_ducklake_commit = false;
			return;
		}
		error_obj.Throw("Failed to initialize DuckLake: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("Combined init query returned no rows");
	}
	cached_init.valid = true;
	cached_init.metadata_exists = true;
	cached_init.has_ducklake_commit = chunk->GetValue(1, 0).GetValue<int64_t>() > 0;

	auto metadata_list = chunk->GetValue(2, 0);
	if (!metadata_list.IsNull()) {
		for (auto &entry : ListValue::GetChildren(metadata_list)) {
			auto &fields = StructValue::GetChildren(entry);
			DuckLakeTag tag;
			tag.key = fields[0].ToString();
			tag.value = fields[1].ToString();
			if (fields[2].IsNull()) {
				cached_init.metadata.tags.push_back(std::move(tag));
			} else {
				auto scope = fields[2].ToString();
				if (scope == "schema") {
					DuckLakeSchemaSetting setting;
					setting.schema_id = SchemaIndex(fields[3].GetValue<idx_t>());
					setting.tag = std::move(tag);
					cached_init.metadata.schema_settings.push_back(std::move(setting));
				} else if (scope == "table") {
					DuckLakeTableSetting setting;
					setting.table_id = TableIndex(fields[3].GetValue<idx_t>());
					setting.tag = std::move(tag);
					cached_init.metadata.table_settings.push_back(std::move(setting));
				}
			}
		}
	}
}

bool QuackMetadataManager::MetadataExists() {
	RunCombinedInit();
	return cached_init.metadata_exists;
}

DuckLakeMetadata QuackMetadataManager::LoadDuckLake() {
	RunCombinedInit();
	return std::move(cached_init.metadata);
}

void QuackMetadataManager::ProbeServerCapabilities() {
	RunCombinedInit();
	if (cached_init.has_ducklake_commit) {
		transaction.GetCatalog().SetRetrialsServerSide(true);
	}
}

//===--------------------------------------------------------------------===//
// Combined Phase 2: Snapshot + Catalog + Stats
//===--------------------------------------------------------------------===//

static string ListAgg(const vector<pair<string, string>> &fields) {
	string result = "LIST(struct_pack(";
	for (idx_t i = 0; i < fields.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += fields[i].first + " := " + fields[i].second;
	}
	result += "))";
	return result;
}

void QuackMetadataManager::RunCombinedCatalogLoad() {
	if (cached_catalog.valid) {
		return;
	}
	auto &ducklake_catalog = transaction.GetCatalog();

	static const vector<pair<string, string>> TAG_FIELDS = {{"key", "key"}, {"value", "value"}};
	static const vector<pair<string, string>> INLINED_DATA_FIELDS = {
	    {"name", "table_name"}, {"schema_version", "schema_version"}};

	string tag_agg = ListAgg(TAG_FIELDS);
	string inlined_agg = ListAgg(INLINED_DATA_FIELDS);
	string query = StringUtil::Format(R"(
SELECT
    snapshot.snapshot_id, snapshot.schema_version, snapshot.next_catalog_id, snapshot.next_file_id,

    (SELECT LIST(struct_pack(schema_id := schema_id, schema_uuid := schema_uuid::VARCHAR,
        schema_name := schema_name, path := path, path_is_relative := path_is_relative))
    FROM {METADATA_CATALOG}.ducklake_schema
    WHERE snapshot.snapshot_id >= begin_snapshot
      AND (snapshot.snapshot_id < end_snapshot OR end_snapshot IS NULL)) AS schemas,

    (SELECT LIST(struct_pack(schema_id := tbl.schema_id, table_id := tbl.table_id,
        table_uuid := tbl.table_uuid::VARCHAR, table_name := tbl.table_name,
        path := tbl.path, path_is_relative := tbl.path_is_relative,
        tags := (SELECT %s FROM {METADATA_CATALOG}.ducklake_tag tag
                 WHERE object_id = tbl.table_id
                   AND snapshot.snapshot_id >= tag.begin_snapshot
                   AND (snapshot.snapshot_id < tag.end_snapshot OR tag.end_snapshot IS NULL)),
        inlined_data_tables := (SELECT %s FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt
                                WHERE idt.table_id = tbl.table_id),
        columns := (SELECT LIST(struct_pack(column_id := col.column_id, column_name := col.column_name,
            column_type := col.column_type, initial_default := col.initial_default,
            default_value := col.default_value, nulls_allowed := col.nulls_allowed,
            parent_column := col.parent_column, default_value_type := col.default_value_type,
            column_tags := (SELECT %s FROM {METADATA_CATALOG}.ducklake_column_tag ct
                           WHERE ct.table_id = tbl.table_id AND ct.column_id = col.column_id
                             AND snapshot.snapshot_id >= ct.begin_snapshot
                             AND (snapshot.snapshot_id < ct.end_snapshot OR ct.end_snapshot IS NULL)))
        ORDER BY col.parent_column NULLS FIRST, col.column_order)
        FROM {METADATA_CATALOG}.ducklake_column col
        WHERE col.table_id = tbl.table_id
          AND snapshot.snapshot_id >= col.begin_snapshot
          AND (snapshot.snapshot_id < col.end_snapshot OR col.end_snapshot IS NULL))))
    FROM {METADATA_CATALOG}.ducklake_table tbl
    WHERE snapshot.snapshot_id >= tbl.begin_snapshot
      AND (snapshot.snapshot_id < tbl.end_snapshot OR tbl.end_snapshot IS NULL)) AS tables,

    (SELECT LIST(struct_pack(view_id := v.view_id, view_uuid := v.view_uuid, schema_id := v.schema_id,
        view_name := v.view_name, dialect := v.dialect, sql := v.sql,
        column_aliases := v.column_aliases,
        tags := (SELECT %s FROM {METADATA_CATALOG}.ducklake_tag tag
                 WHERE object_id = v.view_id
                   AND snapshot.snapshot_id >= tag.begin_snapshot
                   AND (snapshot.snapshot_id < tag.end_snapshot OR tag.end_snapshot IS NULL))))
    FROM {METADATA_CATALOG}.ducklake_view v
    WHERE snapshot.snapshot_id >= v.begin_snapshot
      AND (snapshot.snapshot_id < v.end_snapshot OR v.end_snapshot IS NULL)) AS views,

    (SELECT LIST(struct_pack(schema_id := m.schema_id, macro_id := m.macro_id, macro_name := m.macro_name,
        implementations := (SELECT LIST(struct_pack(dialect := impl.dialect, sql := impl.sql, type := impl.type,
            params := (SELECT LIST(struct_pack(parameter_name := p.parameter_name, parameter_type := p.parameter_type,
                default_value := p.default_value, default_value_type := p.default_value_type))
            FROM {METADATA_CATALOG}.ducklake_macro_parameters p
            WHERE p.macro_id = impl.macro_id AND p.impl_id = impl.impl_id)))
        FROM {METADATA_CATALOG}.ducklake_macro_impl impl
        WHERE impl.macro_id = m.macro_id)))
    FROM {METADATA_CATALOG}.ducklake_macro m
    WHERE snapshot.snapshot_id >= m.begin_snapshot
      AND (snapshot.snapshot_id < m.end_snapshot OR m.end_snapshot IS NULL)) AS macros,

    (SELECT LIST(struct_pack(partition_id := part.partition_id, table_id := part.table_id,
        fields := (SELECT LIST(struct_pack(partition_key_index := pc.partition_key_index,
            column_id := pc.column_id, transform := pc.transform) ORDER BY pc.partition_key_index)
        FROM {METADATA_CATALOG}.ducklake_partition_column pc
        WHERE pc.partition_id = part.partition_id)))
    FROM {METADATA_CATALOG}.ducklake_partition_info part
    WHERE snapshot.snapshot_id >= part.begin_snapshot
      AND (snapshot.snapshot_id < part.end_snapshot OR part.end_snapshot IS NULL)) AS partitions,

    (SELECT LIST(struct_pack(sort_id := sort.sort_id, table_id := sort.table_id,
        fields := (SELECT LIST(struct_pack(sort_key_index := se.sort_key_index, expression := se.expression,
            dialect := se.dialect, sort_direction := se.sort_direction, null_order := se.null_order)
        ORDER BY se.sort_key_index)
        FROM {METADATA_CATALOG}.ducklake_sort_expression se
        WHERE se.sort_id = sort.sort_id)))
    FROM {METADATA_CATALOG}.ducklake_sort_info sort
    WHERE snapshot.snapshot_id >= sort.begin_snapshot
      AND (snapshot.snapshot_id < sort.end_snapshot OR sort.end_snapshot IS NULL)) AS sorts,

    (SELECT LIST(struct_pack(table_id := ts.table_id, column_id := tcs.column_id,
        record_count := ts.record_count, next_row_id := ts.next_row_id,
        file_size_bytes := ts.file_size_bytes, contains_null := tcs.contains_null,
        contains_nan := tcs.contains_nan, min_value := tcs.min_value,
        max_value := tcs.max_value, extra_stats := tcs.extra_stats) ORDER BY ts.table_id)
    FROM {METADATA_CATALOG}.ducklake_table_stats ts
    JOIN {METADATA_CATALOG}.ducklake_table_column_stats tcs USING (table_id)
    WHERE ts.record_count IS NOT NULL AND ts.file_size_bytes IS NOT NULL) AS global_stats

FROM {METADATA_CATALOG}.ducklake_snapshot snapshot
WHERE snapshot.snapshot_id = (SELECT MAX(snapshot_id) FROM {METADATA_CATALOG}.ducklake_snapshot)
)", tag_agg, inlined_agg, tag_agg, tag_agg);

	auto result = Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to load DuckLake catalog: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("No snapshot found in DuckLake metadata");
	}

	cached_catalog.snapshot.snapshot_id = chunk->GetValue(0, 0).GetValue<idx_t>();
	cached_catalog.snapshot.schema_version = chunk->GetValue(1, 0).GetValue<idx_t>();
	cached_catalog.snapshot.next_catalog_id = chunk->GetValue(2, 0).GetValue<idx_t>();
	cached_catalog.snapshot.next_file_id = chunk->GetValue(3, 0).GetValue<idx_t>();

	const auto &data_path = ducklake_catalog.DataPath();
	const auto &separator = ducklake_catalog.Separator();

	auto schemas_val = chunk->GetValue(4, 0);
	map<SchemaIndex, idx_t> schema_map;
	if (!schemas_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(schemas_val)) {
			auto &f = StructValue::GetChildren(entry);
			DuckLakeSchemaInfo schema;
			schema.id = SchemaIndex(f[0].GetValue<uint64_t>());
			schema.uuid = f[1].ToString();
			schema.name = f[2].ToString();
			if (f[3].IsNull()) {
				schema.path = data_path;
			} else {
				DuckLakePath path;
				path.path = f[3].ToString();
				path.path_is_relative = f[4].GetValue<bool>();
				schema.path = FromRelativePath(path, data_path, separator);
			}
			schema_map[schema.id] = cached_catalog.catalog.schemas.size();
			cached_catalog.catalog.schemas.push_back(std::move(schema));
		}
	}

	auto tables_val = chunk->GetValue(5, 0);
	if (!tables_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(tables_val)) {
			auto &f = StructValue::GetChildren(entry);
			DuckLakeTableInfo table_info;
			table_info.schema_id = SchemaIndex(f[0].GetValue<uint64_t>());
			table_info.id = TableIndex(f[1].GetValue<uint64_t>());
			table_info.uuid = f[2].ToString();
			table_info.name = f[3].ToString();

			if (!f[4].IsNull()) {
				DuckLakePath path;
				path.path = f[4].ToString();
				path.path_is_relative = f[5].GetValue<bool>();
				auto schema_it = schema_map.find(table_info.schema_id);
				string base = schema_it != schema_map.end()
				                  ? cached_catalog.catalog.schemas[schema_it->second].path
				                  : data_path;
				table_info.path = FromRelativePath(path, base, separator);
			} else {
				auto schema_it = schema_map.find(table_info.schema_id);
				table_info.path = schema_it != schema_map.end()
				                      ? cached_catalog.catalog.schemas[schema_it->second].path
				                      : data_path;
			}

			if (!f[6].IsNull()) {
				table_info.tags = LoadTags(f[6]);
			}
			if (!f[7].IsNull()) {
				table_info.inlined_data_tables = LoadInlinedDataTables(f[7]);
			}

			if (!f[8].IsNull()) {
				for (auto &col_entry : ListValue::GetChildren(f[8])) {
					auto &cf = StructValue::GetChildren(col_entry);
					DuckLakeColumnInfo col;
					col.id = FieldIndex(cf[0].GetValue<uint64_t>());
					col.name = cf[1].ToString();
					col.type = cf[2].ToString();
					if (!cf[3].IsNull()) {
						col.initial_default = Value(cf[3].ToString());
					}
					if (!cf[4].IsNull()) {
						auto val = cf[4].ToString();
						col.default_value = val == "NULL" ? Value() : Value(val);
					}
					col.nulls_allowed = cf[5].IsNull() || cf[5].GetValue<bool>();
					if (!cf[7].IsNull()) {
						col.default_value_type = cf[7].ToString();
					}
					if (!cf[8].IsNull()) {
						col.tags = LoadTags(cf[8]);
					}
					if (!cf[6].IsNull()) {
						auto parent_id = FieldIndex(cf[6].GetValue<uint64_t>());
						if (!AddChildColumn(table_info.columns, parent_id, col)) {
							table_info.columns.push_back(std::move(col));
						}
					} else {
						table_info.columns.push_back(std::move(col));
					}
				}
			}
			cached_catalog.catalog.tables.push_back(std::move(table_info));
		}
	}

	auto views_val = chunk->GetValue(6, 0);
	if (!views_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(views_val)) {
			auto &f = StructValue::GetChildren(entry);
			DuckLakeViewInfo view;
			view.id = TableIndex(f[0].GetValue<uint64_t>());
			view.uuid = f[1].ToString();
			view.schema_id = SchemaIndex(f[2].GetValue<uint64_t>());
			view.name = f[3].ToString();
			view.dialect = f[4].ToString();
			view.sql = f[5].ToString();
			view.column_aliases = DuckLakeUtil::ParseQuotedList(f[6].ToString());
			if (!f[7].IsNull()) {
				view.tags = LoadTags(f[7]);
			}
			cached_catalog.catalog.views.push_back(std::move(view));
		}
	}

	auto macros_val = chunk->GetValue(7, 0);
	if (!macros_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(macros_val)) {
			auto &f = StructValue::GetChildren(entry);
			DuckLakeMacroInfo macro;
			macro.schema_id = SchemaIndex(f[0].GetValue<uint64_t>());
			macro.macro_id = MacroIndex(f[1].GetValue<uint64_t>());
			macro.macro_name = f[2].ToString();
			if (!f[3].IsNull()) {
				macro.implementations = LoadMacroImplementations(f[3]);
			}
			cached_catalog.catalog.macros.push_back(std::move(macro));
		}
	}

	auto partitions_val = chunk->GetValue(8, 0);
	if (!partitions_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(partitions_val)) {
			auto &f = StructValue::GetChildren(entry);
			DuckLakePartitionInfo partition;
			partition.id = f[0].GetValue<uint64_t>();
			partition.table_id = TableIndex(f[1].GetValue<uint64_t>());
			if (!f[2].IsNull()) {
				for (auto &field_entry : ListValue::GetChildren(f[2])) {
					auto &pf = StructValue::GetChildren(field_entry);
					DuckLakePartitionFieldInfo field;
					field.partition_key_index = pf[0].GetValue<uint64_t>();
					field.field_id = FieldIndex(pf[1].GetValue<uint64_t>());
					field.transform = pf[2].ToString();
					partition.fields.push_back(std::move(field));
				}
			}
			cached_catalog.catalog.partitions.push_back(std::move(partition));
		}
	}

	auto sorts_val = chunk->GetValue(9, 0);
	if (!sorts_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(sorts_val)) {
			auto &f = StructValue::GetChildren(entry);
			DuckLakeSortInfo sort;
			sort.id = f[0].GetValue<uint64_t>();
			sort.table_id = TableIndex(f[1].GetValue<uint64_t>());
			if (!f[2].IsNull()) {
				for (auto &field_entry : ListValue::GetChildren(f[2])) {
					auto &sf = StructValue::GetChildren(field_entry);
					DuckLakeSortFieldInfo field;
					field.sort_key_index = sf[0].GetValue<uint64_t>();
					field.expression = sf[1].ToString();
					field.dialect = sf[2].ToString();
					field.sort_direction = StringUtil::CIEquals(sf[3].ToString(), "DESC")
					                          ? OrderType::DESCENDING
					                          : OrderType::ASCENDING;
					field.null_order = StringUtil::CIEquals(sf[4].ToString(), "NULLS_FIRST")
					                       ? OrderByNullType::NULLS_FIRST
					                       : OrderByNullType::NULLS_LAST;
					sort.fields.push_back(std::move(field));
				}
			}
			cached_catalog.catalog.sorts.push_back(std::move(sort));
		}
	}

	auto stats_val = chunk->GetValue(10, 0);
	if (!stats_val.IsNull()) {
		for (auto &entry : ListValue::GetChildren(stats_val)) {
			auto &f = StructValue::GetChildren(entry);
			auto table_id = TableIndex(f[0].GetValue<uint64_t>());
			if (cached_catalog.global_stats.empty() || cached_catalog.global_stats.back().table_id != table_id) {
				DuckLakeGlobalStatsInfo gs;
				gs.table_id = table_id;
				gs.initialized = true;
				gs.record_count = f[2].GetValue<uint64_t>();
				gs.next_row_id = f[3].GetValue<uint64_t>();
				gs.table_size_bytes = f[4].GetValue<uint64_t>();
				cached_catalog.global_stats.push_back(std::move(gs));
			}
			DuckLakeGlobalColumnStatsInfo col_stats;
			col_stats.column_id = FieldIndex(f[1].GetValue<uint64_t>());
			col_stats.has_contains_null = !f[5].IsNull();
			if (col_stats.has_contains_null) {
				col_stats.contains_null = f[5].GetValue<bool>();
			}
			col_stats.has_contains_nan = !f[6].IsNull();
			if (col_stats.has_contains_nan) {
				col_stats.contains_nan = f[6].GetValue<bool>();
			}
			col_stats.has_min = !f[7].IsNull();
			if (col_stats.has_min) {
				col_stats.min_val = f[7].ToString();
			}
			col_stats.has_max = !f[8].IsNull();
			if (col_stats.has_max) {
				col_stats.max_val = f[8].ToString();
			}
			col_stats.has_extra_stats = !f[9].IsNull();
			if (col_stats.has_extra_stats) {
				col_stats.extra_stats = f[9].ToString();
			}
			cached_catalog.global_stats.back().column_stats.push_back(std::move(col_stats));
		}
	}

	cached_catalog.valid = true;
}

unique_ptr<DuckLakeSnapshot> QuackMetadataManager::GetSnapshot() {
	RunCombinedCatalogLoad();
	return make_uniq<DuckLakeSnapshot>(cached_catalog.snapshot);
}

DuckLakeCatalogInfo QuackMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	RunCombinedCatalogLoad();
	if (cached_catalog.valid && cached_catalog.snapshot.snapshot_id == snapshot.snapshot_id) {
		return std::move(cached_catalog.catalog);
	}
	return DuckLakeMetadataManager::GetCatalogForSnapshot(snapshot);
}

vector<DuckLakeGlobalStatsInfo> QuackMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot) {
	RunCombinedCatalogLoad();
	if (cached_catalog.valid && cached_catalog.snapshot.snapshot_id == snapshot.snapshot_id) {
		return std::move(cached_catalog.global_stats);
	}
	return DuckLakeMetadataManager::GetGlobalTableStats(snapshot);
}

//===--------------------------------------------------------------------===//
// Server-side commit
//===--------------------------------------------------------------------===//

static bool IsDataOnlyCommit(const TransactionChangeInformation &c) {
	return c.created_schemas.empty() && c.dropped_schemas.empty() && c.created_tables.empty() &&
	       c.created_scalar_macros.empty() && c.created_table_macros.empty() && c.altered_tables.empty() &&
	       c.altered_tables_with_schema_version_changes.empty() && c.altered_views.empty() &&
	       c.dropped_tables.empty() && c.dropped_views.empty() && c.dropped_scalar_macros.empty() &&
	       c.dropped_table_macros.empty();
}

bool QuackMetadataManager::CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const {
	return ExecuteRetrialsServerSide() && IsDataOnlyCommit(changes);
}

void QuackMetadataManager::FlushChangesServerSide(DuckLakeTransaction &flush_transaction,
                                                  DuckLakeSnapshot transaction_snapshot,
                                                  const TransactionChangeInformation &transaction_changes,
                                                  const DuckLakeRetryConfig &retry_config) {
	if (!IsDataOnlyCommit(transaction_changes)) {
		flush_transaction.RunCommitLoop(transaction_snapshot, transaction_changes, retry_config);
		return;
	}
	transaction.GetCatalog().EnsureCommitInfoProvided(flush_transaction.GetCommitInfo());
	DuckLakeStagedCommit staged(flush_transaction.GenerateUUID());
	string batch = staged.Build(flush_transaction, transaction_changes, transaction_snapshot, retry_config);
	auto result = Query(batch);
	if (!result || result->HasError()) {
		if (result) {
			result->GetErrorObject().Throw("Failed to invoke server-side ducklake_commit: ");
		}
		throw IOException("Failed to invoke server-side ducklake_commit: empty result");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("Server-side ducklake_commit returned no rows");
	}
	auto committed_snapshot_id = chunk->GetValue(0, 0).GetValue<int64_t>();
	auto committed_schema_version = chunk->GetValue(1, 0).GetValue<int64_t>();
	auto had_flushes = !chunk->GetValue(2, 0).IsNull() && chunk->GetValue(2, 0).GetValue<bool>();
	auto next_catalog_id = chunk->GetValue(3, 0).GetValue<int64_t>();
	auto next_file_id = chunk->GetValue(4, 0).GetValue<int64_t>();
	DuckLakeSnapshot committed_snapshot(static_cast<idx_t>(committed_snapshot_id),
	                                    static_cast<idx_t>(committed_schema_version),
	                                    static_cast<idx_t>(next_catalog_id), static_cast<idx_t>(next_file_id));
	flush_transaction.GetCatalog().SetCommittedSnapshot(committed_snapshot);
	flush_transaction.ApplyServerSideCommit(static_cast<idx_t>(committed_schema_version));
	if (had_flushes) {
		flush_transaction.DropEmptySupersededInlinedTablesClientSide();
	}
	ClearCache();
}

} // namespace duckdb
