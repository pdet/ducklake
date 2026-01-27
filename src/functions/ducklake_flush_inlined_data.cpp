#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_multi_file_reader.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_copy_to_file.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "storage/ducklake_compaction.hpp"
#include "duckdb/common/multi_file/multi_file_function.hpp"
#include "storage/ducklake_multi_file_list.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "storage/ducklake_flush_data.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_delete_filter.hpp"

namespace duckdb {

static void AttachDeleteFilesToWrittenFiles(vector<DuckLakeDeleteFile> &delete_files,
                                            vector<DuckLakeDataFile> &written_files) {
	for (auto &delete_file : delete_files) {
		for (auto &written_file : written_files) {
			if (written_file.file_name == delete_file.data_file_path) {
				written_file.delete_files.push_back(std::move(delete_file));
				break;
			}
		}
	}
}

//===--------------------------------------------------------------------===//
// Base Flush Operator
//===--------------------------------------------------------------------===//
DuckLakeFlushOperator::DuckLakeFlushOperator(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
                                             DuckLakeTableEntry &table_p, string encryption_key_p)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, types, 0), table(table_p),
      encryption_key(std::move(encryption_key_p)) {
}

//===--------------------------------------------------------------------===//
// Flush Inlined Insertions Operator
//===--------------------------------------------------------------------===//
DuckLakeFlushInlinedInsertions::DuckLakeFlushInlinedInsertions(PhysicalPlan &physical_plan,
                                                               const vector<LogicalType> &types,
                                                               DuckLakeTableEntry &table,
                                                               DuckLakeInlinedTableInfo inlined_table_p,
                                                               string encryption_key_p, optional_idx partition_id_p,
                                                               PhysicalOperator &child)
    : DuckLakeFlushOperator(physical_plan, types, table, std::move(encryption_key_p)),
      inlined_table(std::move(inlined_table_p)), partition_id(partition_id_p) {
	children.push_back(child);
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeFlushInlinedInsertions::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                 OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> DuckLakeFlushInlinedInsertions::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<DuckLakeInsertGlobalState>(table);
}

SinkResultType DuckLakeFlushInlinedInsertions::Sink(ExecutionContext &context, DataChunk &chunk,
                                                    OperatorSinkInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	DuckLakeInsert::AddWrittenFiles(global_state, chunk, encryption_key, partition_id, true);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
using DeletesPerFile = unordered_map<string, set<PositionWithSnapshot>>;

static DeletesPerFile GroupDeletesByFile(QueryResult &deleted_rows_result, vector<DuckLakeDataFile> &written_files,
                                         vector<idx_t> &file_start_row_ids, idx_t inlined_row_id_start) {
	DeletesPerFile deletes_per_file;
	for (auto &row : deleted_rows_result) {
		auto end_snap = row.GetValue<int64_t>(0);
		auto row_id = row.GetValue<int64_t>(1);

		if (written_files.size() == 1) {
			// Convert global row_id to file position by subtracting the inlined data's row_id_start
			int64_t pos_in_file = row_id - static_cast<int64_t>(inlined_row_id_start);
			PositionWithSnapshot pos_with_snap {pos_in_file, end_snap};
			deletes_per_file[written_files[0].file_name].insert(pos_with_snap);
		} else {
			// lets write the deletes to the right files, in case we have multiple files
			for (idx_t file_idx = 0; file_idx < written_files.size(); file_idx++) {
				const int64_t file_start = static_cast<int64_t>(file_start_row_ids[file_idx] + inlined_row_id_start);
				const int64_t file_end = static_cast<int64_t>(file_start + written_files[file_idx].row_count);
				if (row_id >= file_start && row_id < file_end) {
					int64_t pos_in_file = row_id - file_start;
					PositionWithSnapshot pos_with_snap {pos_in_file, end_snap};
					deletes_per_file[written_files[file_idx].file_name].insert(pos_with_snap);
					break;
				}
			}
		}
	}
	return deletes_per_file;
}

SinkFinalizeType DuckLakeFlushInlinedInsertions::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         OperatorSinkFinalizeInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeInsertGlobalState>();
	auto &transaction = DuckLakeTransaction::Get(context, global_state.table.catalog);
	auto snapshot = transaction.GetSnapshot();

	if (!global_state.written_files.empty()) {
		// query all deleted rows with their snapshot IDs
		auto deleted_rows_result = transaction.Query(snapshot, StringUtil::Format(R"(
			SELECT end_snapshot, row_id
			FROM {METADATA_CATALOG}.%s
			WHERE end_snapshot IS NOT NULL
			ORDER BY row_id;)",
		                                                                          inlined_table.table_name));

		// query the minimum row_id to know where the inlined data starts in the global row_id space
		auto min_row_id_result = transaction.Query(snapshot, StringUtil::Format(R"(
			SELECT MIN(row_id)
			FROM {METADATA_CATALOG}.%s;)",
		                                                                        inlined_table.table_name));
		idx_t inlined_row_id_start = 0;
		for (auto &row : *min_row_id_result) {
			if (!row.IsNull(0)) {
				inlined_row_id_start = row.GetValue<idx_t>(0);
			}
		}

		// lets figure out where each file ends, so we know where to place ze deletes
		vector<idx_t> file_start_row_ids;
		idx_t current_pos = 0;
		for (auto &written_file : global_state.written_files) {
			file_start_row_ids.push_back(current_pos);
			current_pos += written_file.row_count;
		}

		auto deletes_per_file =
		    GroupDeletesByFile(*deleted_rows_result, global_state.written_files, file_start_row_ids, inlined_row_id_start);

		if (!deletes_per_file.empty()) {
			auto &fs = FileSystem::GetFileSystem(context);
			vector<DuckLakeDeleteFile> delete_files;

			for (auto &file_entry : deletes_per_file) {
				// write single file, begin_snapshot is the minimum snapshot
				WriteDeleteFileWithSnapshotsInput file_input {context,
				                                              transaction,
				                                              fs,
				                                              table.DataPath(),
				                                              encryption_key,
				                                              file_entry.first,
				                                              file_entry.second,
				                                              DeleteFileSource::FLUSH};
				delete_files.push_back(DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(context, file_input));
			}
			AttachDeleteFilesToWrittenFiles(delete_files, global_state.written_files);
		}
	}

	transaction.AppendFiles(global_state.table.GetTableId(), std::move(global_state.written_files));
	transaction.DeleteInlinedData(inlined_table);
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeFlushInlinedInsertions::GetName() const {
	return "DUCKLAKE_FLUSH_INLINED_INSERTIONS";
}

//===--------------------------------------------------------------------===//
// DuckLakeFlushInlinedFileDeletions
//===--------------------------------------------------------------------===//
DuckLakeFlushInlinedFileDeletions::DuckLakeFlushInlinedFileDeletions(PhysicalPlan &physical_plan,
                                                                     const vector<LogicalType> &types,
                                                                     DuckLakeTableEntry &table_p,
                                                                     string inlined_table_name_p,
                                                                     string encryption_key_p)
    : DuckLakeFlushOperator(physical_plan, types, table_p, std::move(encryption_key_p)),
      inlined_table_name(std::move(inlined_table_name_p)) {
}

SourceResultType DuckLakeFlushInlinedFileDeletions::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                                    OperatorSourceInput &input) const {
	auto &client_context = context.client;
	auto &transaction = DuckLakeTransaction::Get(client_context, table.catalog);
	auto &metadata_manager = transaction.GetMetadataManager();

	// Read all inlined file deletions for this table
	auto inlined_deletions = metadata_manager.ReadInlinedFileDeletionsForFlush(table.GetTableId(), inlined_table_name);
	if (inlined_deletions.empty()) {
		return SourceResultType::FINISHED;
	}

	auto &fs = FileSystem::GetFileSystem(client_context);
	vector<DuckLakeDeleteFile> delete_files;

	// Write delete files for each data file with inlined deletions
	for (auto &file_entry : inlined_deletions) {
		// Convert to set of PositionWithSnapshot
		set<PositionWithSnapshot> positions;
		for (idx_t i = 0; i < file_entry.inlined_deletions.deleted_rows.size(); i++) {
			PositionWithSnapshot pos_with_snap {static_cast<int64_t>(file_entry.inlined_deletions.deleted_rows[i]),
			                                    static_cast<int64_t>(file_entry.inlined_deletions.snapshot_ids[i])};
			positions.insert(pos_with_snap);
		}

		WriteDeleteFileWithSnapshotsInput file_input {client_context,
		                                              transaction,
		                                              fs,
		                                              table.DataPath(),
		                                              encryption_key,
		                                              file_entry.file_path,
		                                              positions,
		                                              DeleteFileSource::FLUSH};
		auto delete_file = DuckLakeDeleteFileWriter::WriteDeleteFileWithSnapshots(client_context, file_input);
		delete_file.data_file_id = file_entry.file_id;
		delete_file.overwrites_existing_delete = file_entry.overwrites_existing;
		delete_files.push_back(std::move(delete_file));
	}

	// Add delete files to transaction
	transaction.AddDeletes(table.GetTableId(), std::move(delete_files));

	// Clear the inlined file deletions
	transaction.DeleteInlinedFileDeletions(inlined_table_name);

	return SourceResultType::FINISHED;
}

string DuckLakeFlushInlinedFileDeletions::GetName() const {
	return "DUCKLAKE_FLUSH_INLINED_FILE_DELETIONS";
}

//===--------------------------------------------------------------------===//
// Logical Operator for Flushing Inlined File Deletions
//===--------------------------------------------------------------------===//
class DuckLakeLogicalFlushInlinedFileDeletions : public LogicalExtensionOperator {
public:
	DuckLakeLogicalFlushInlinedFileDeletions(idx_t table_index_p, DuckLakeTableEntry &table_p,
	                                         string inlined_table_name_p, string encryption_key_p)
	    : table_index(table_index_p), table(table_p), inlined_table_name(std::move(inlined_table_name_p)),
	      encryption_key(std::move(encryption_key_p)) {
	}

	idx_t table_index;
	DuckLakeTableEntry &table;
	string inlined_table_name;
	string encryption_key;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<DuckLakeFlushInlinedFileDeletions>(types, table, std::move(inlined_table_name),
		                                                       std::move(encryption_key));
	}

	string GetExtensionName() const override {
		return "ducklake";
	}
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> result;
		result.emplace_back(table_index, 0);
		return result;
	}

	void ResolveTypes() override {
		types = {LogicalType::BOOLEAN};
	}
};

//===--------------------------------------------------------------------===//
// Logical Operator for Flushing Inlined Insertions
//===--------------------------------------------------------------------===//
class DuckLakeLogicalFlushInlinedInsertions : public LogicalExtensionOperator {
public:
	DuckLakeLogicalFlushInlinedInsertions(idx_t table_index, DuckLakeTableEntry &table,
	                                      DuckLakeInlinedTableInfo inlined_table_p, string encryption_key_p,
	                                      optional_idx partition_id_p)
	    : table_index(table_index), table(table), inlined_table(std::move(inlined_table_p)),
	      encryption_key(std::move(encryption_key_p)), partition_id(partition_id_p) {
	}

	idx_t table_index;
	DuckLakeTableEntry &table;
	DuckLakeInlinedTableInfo inlined_table;
	string encryption_key;
	optional_idx partition_id;

public:
	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		auto &child = planner.CreatePlan(*children[0]);
		return planner.Make<DuckLakeFlushInlinedInsertions>(types, table, std::move(inlined_table),
		                                                    std::move(encryption_key), partition_id, child);
	}

	string GetExtensionName() const override {
		return "ducklake";
	}
	vector<ColumnBinding> GetColumnBindings() override {
		vector<ColumnBinding> result;
		result.emplace_back(table_index, 0);
		return result;
	}

	void ResolveTypes() override {
		types = {LogicalType::BOOLEAN};
	}
};

////===--------------------------------------------------------------------===//
//// Compaction Command Generator
////===--------------------------------------------------------------------===//
class DuckLakeDataFlusher {
public:
	DuckLakeDataFlusher(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeTransaction &transaction,
	                    Binder &binder, TableIndex table_id, const DuckLakeInlinedTableInfo &inlined_table);

	unique_ptr<LogicalOperator> GenerateFlushCommand();

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeTransaction &transaction;
	Binder &binder;
	TableIndex table_id;
	const DuckLakeInlinedTableInfo &inlined_table;
};

DuckLakeDataFlusher::DuckLakeDataFlusher(ClientContext &context, DuckLakeCatalog &catalog,
                                         DuckLakeTransaction &transaction, Binder &binder, TableIndex table_id,
                                         const DuckLakeInlinedTableInfo &inlined_table_p)
    : context(context), catalog(catalog), transaction(transaction), binder(binder), table_id(table_id),
      inlined_table(inlined_table_p) {
}

unique_ptr<LogicalOperator> DuckLakeDataFlusher::GenerateFlushCommand() {
	// get the table entry at the specified snapshot
	DuckLakeSnapshot snapshot(catalog.GetBeginSnapshotForTable(table_id, transaction), inlined_table.schema_version, 0,
	                          0);

	auto entry = catalog.GetEntryById(transaction, snapshot, table_id);
	if (!entry) {
		throw InternalException("DuckLakeCompactor: failed to find table entry for given snapshot id");
	}
	auto &table = entry->Cast<DuckLakeTableEntry>();

	auto table_idx = binder.GenerateTableIndex();
	unique_ptr<FunctionData> bind_data;
	EntryLookupInfo info(CatalogType::TABLE_ENTRY, table.name);
	auto scan_function = table.GetScanFunction(context, bind_data, info);

	auto &multi_file_bind_data = bind_data->Cast<MultiFileBindData>();
	auto &read_info = scan_function.function_info->Cast<DuckLakeFunctionInfo>();
	read_info.scan_type = DuckLakeScanType::SCAN_FOR_FLUSH;
	multi_file_bind_data.file_list = make_uniq<DuckLakeMultiFileList>(read_info, inlined_table);

	optional_idx partition_id;
	auto partition_data = table.GetPartitionData();
	if (partition_data) {
		partition_id = partition_data->partition_id;
	}

	// generate the LogicalGet
	auto &columns = table.GetColumns();

	DuckLakeCopyInput copy_input(context, table);
	copy_input.get_table_index = table_idx;
	copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID_AND_SNAPSHOT_ID;

	auto copy_options = DuckLakeInsert::GetCopyOptions(context, copy_input);

	auto virtual_columns = table.GetVirtualColumns();
	auto ducklake_scan =
	    make_uniq<LogicalGet>(table_idx, std::move(scan_function), std::move(bind_data), copy_options.expected_types,
	                          copy_options.names, std::move(virtual_columns));
	auto &column_ids = ducklake_scan->GetMutableColumnIds();
	for (idx_t i = 0; i < columns.PhysicalColumnCount(); i++) {
		column_ids.emplace_back(i);
	}
	column_ids.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	column_ids.emplace_back(DuckLakeMultiFileReader::COLUMN_IDENTIFIER_SNAPSHOT_ID);

	auto root = unique_ptr_cast<LogicalGet, LogicalOperator>(std::move(ducklake_scan));

	if (!copy_options.projection_list.empty()) {
		// push a projection
		auto proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(copy_options.projection_list));
		proj->children.push_back(std::move(root));
		root = std::move(proj);
	}

	// Add another projection with casts if necessary
	root->ResolveOperatorTypes();
	if (DuckLakeTypes::RequiresCast(root->types)) {
		root = DuckLakeInsert::InsertCasts(binder, root);
	}

	// generate the LogicalCopyToFile
	auto copy = make_uniq<LogicalCopyToFile>(std::move(copy_options.copy_function), std::move(copy_options.bind_data),
	                                         std::move(copy_options.info));

	copy->file_path = std::move(copy_options.file_path);
	copy->use_tmp_file = copy_options.use_tmp_file;
	copy->filename_pattern = std::move(copy_options.filename_pattern);
	copy->file_extension = std::move(copy_options.file_extension);
	copy->overwrite_mode = copy_options.overwrite_mode;
	copy->per_thread_output = copy_options.per_thread_output;
	copy->file_size_bytes = copy_options.file_size_bytes;
	copy->rotate = copy_options.rotate;
	copy->return_type = copy_options.return_type;

	copy->partition_output = copy_options.partition_output;
	copy->write_partition_columns = copy_options.write_partition_columns;
	copy->write_empty_file = copy_options.write_empty_file;
	copy->partition_columns = std::move(copy_options.partition_columns);
	copy->names = std::move(copy_options.names);
	copy->expected_types = std::move(copy_options.expected_types);

	copy->children.push_back(std::move(root));

	// followed by the compaction operator (that writes the results back to the
	auto compaction = make_uniq<DuckLakeLogicalFlushInlinedInsertions>(binder.GenerateTableIndex(), table, inlined_table,
	                                                                   std::move(copy_input.encryption_key), partition_id);
	compaction->children.push_back(std::move(copy));
	return std::move(compaction);
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
static unique_ptr<LogicalOperator> FlushInlinedDataBind(ClientContext &context, TableFunctionBindInput &input,
                                                        idx_t bind_index, vector<string> &return_names) {
	input.binder->SetAlwaysRequireRebind();
	// gather a list of files to compact
	auto &catalog = BaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	auto &ducklake_catalog = catalog.Cast<DuckLakeCatalog>();
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);

	auto &named_parameters = input.named_parameters;

	unordered_map<idx_t, vector<reference<DuckLakeTableEntry>>> schema_table_map;
	string schema, table;

	auto schema_entry = named_parameters.find("schema_name");
	if (schema_entry != named_parameters.end()) {
		// specific schema
		schema = StringValue::Get(schema_entry->second);
	}
	auto table_entry = named_parameters.find("table_name");
	if (table_entry != named_parameters.end()) {
		table = StringValue::Get(table_entry->second);
	}

	// no or table schema specified - scan all schemas
	if (table.empty()) {
		// no specific table
		// scan all tables from schemas
		vector<reference<SchemaCatalogEntry>> schemas;
		if (schema.empty()) {
			// no specific schema - fetch all schemas
			schemas = ducklake_catalog.GetSchemas(context);
		} else {
			// specific schema - fetch it
			schemas.push_back(ducklake_catalog.GetSchema(context, schema));
		}

		// - scan all tables from the relevant schemas
		for (auto &schema_catalog_entry : schemas) {
			schema_catalog_entry.get().Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					auto &dl_schema = schema_catalog_entry.get().Cast<DuckLakeSchemaEntry>();
					schema_table_map[dl_schema.GetSchemaId().index].push_back(entry.Cast<DuckLakeTableEntry>());
				}
			});
		}
	} else {
		// specific table - fetch the table
		auto table_catalog_entry =
		    ducklake_catalog.GetEntry<TableCatalogEntry>(context, schema, table, OnEntryNotFound::THROW_EXCEPTION);
		auto &dl_schema = table_catalog_entry->schema.Cast<DuckLakeSchemaEntry>();
		schema_table_map[dl_schema.Cast<DuckLakeSchemaEntry>().GetSchemaId().index].push_back(
		    table_catalog_entry.get()->Cast<DuckLakeTableEntry>());
	}
	// try to compact all tables
	vector<unique_ptr<LogicalOperator>> flushes;
	for (auto &schema_table : schema_table_map) {
		for (auto &table_ref : schema_table.second) {
			SchemaIndex schema_index {schema_table.first};
			if (ducklake_catalog.GetConfigOption<string>("auto_compact", schema_index, table_ref.get().GetTableId(),
			                                             "true") != "true") {
				continue;
			}
			auto &table = table_ref.get();
			auto &inlined_tables = table.GetInlinedDataTables();
			for (auto &inlined_table : inlined_tables) {
				DuckLakeDataFlusher compactor(context, ducklake_catalog, transaction, *input.binder, table.GetTableId(),
				                              inlined_table);
				flushes.push_back(compactor.GenerateFlushCommand());
			}

			// Also check for and flush inlined file deletions
			auto &inlined_delete_table_name = table.GetInlinedDeletionTable();
			if (!inlined_delete_table_name.empty()) {
				auto &metadata_manager = transaction.GetMetadataManager();
				auto inlined_file_deletions =
				    metadata_manager.ReadInlinedFileDeletionsForFlush(table.GetTableId(), inlined_delete_table_name);
				if (!inlined_file_deletions.empty()) {
					// Get encryption key if needed
					string encryption_key = ducklake_catalog.GenerateEncryptionKey(context);
					auto flush_op = make_uniq<DuckLakeLogicalFlushInlinedFileDeletions>(
					    input.binder->GenerateTableIndex(), table, inlined_delete_table_name,
					    std::move(encryption_key));
					flushes.push_back(std::move(flush_op));
				}
			}
		}
	}
	return_names.push_back("Success");
	if (flushes.empty()) {
		// nothing to write - generate empty result
		vector<ColumnBinding> bindings;
		vector<LogicalType> return_types;
		bindings.emplace_back(bind_index, 0);
		return_types.emplace_back(LogicalType::BOOLEAN);
		return make_uniq<LogicalEmptyResult>(std::move(return_types), std::move(bindings));
	}
	if (flushes.size() == 1) {
		// Get the table_index from whichever type of flush we have
		if (auto *data_flush = dynamic_cast<DuckLakeLogicalFlushInlinedInsertions *>(flushes[0].get())) {
			data_flush->table_index = bind_index;
		} else if (auto *deletion_flush = dynamic_cast<DuckLakeLogicalFlushInlinedFileDeletions *>(flushes[0].get())) {
			deletion_flush->table_index = bind_index;
		}
		return std::move(flushes[0]);
	}
	auto union_op = input.binder->UnionOperators(std::move(flushes));
	union_op->Cast<LogicalSetOperation>().table_index = bind_index;
	return union_op;
}

DuckLakeFlushInlinedDataFunction::DuckLakeFlushInlinedDataFunction()
    : TableFunction("ducklake_flush_inlined_data", {LogicalType::VARCHAR}, nullptr, nullptr, nullptr) {
	named_parameters["schema_name"] = LogicalType::VARCHAR;
	named_parameters["table_name"] = LogicalType::VARCHAR;
	bind_operator = FlushInlinedDataBind;
}

} // namespace duckdb
