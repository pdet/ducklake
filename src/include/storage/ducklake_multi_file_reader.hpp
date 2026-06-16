//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_multi_file_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_inlined_data.hpp"

namespace duckdb {
class DuckLakeMultiFileList;
struct DuckLakeDeleteMap;
class DuckLakeFieldData;

//! Scan-wide constants derived from the projected columns + scan type. These are the same for every file in a scan,
//! so they are computed once in InitializeGlobalState rather than stored on the (shared) reader.
struct DuckLakeScanProjection {
	//! Whether row_id was internally projected (not in the user's query) - needed for DCF queries over inlined
	//! deletions
	bool internally_projected_rowid = false;
	//! For deletion scans: output_chunk column index of rowid in global_column_ids order, if projected
	optional_idx deletion_scan_rowid_col;
	//! For deletion scans: output_chunk column index of snapshot_id in global_column_ids order, if projected
	optional_idx deletion_scan_snapshot_col;
};

//! Per-scan reader state. Created once (single-threaded) in InitializeGlobalState and read read-only by all parallel
//! FinalizeChunk calls, so the per-scan projection state no longer races on the shared reader.
struct DuckLakeMultiFileReaderGlobalState : public MultiFileReaderGlobalState {
	DuckLakeMultiFileReaderGlobalState(vector<LogicalType> extra_columns_p,
	                                   optional_ptr<const MultiFileList> file_list_p,
	                                   DuckLakeScanProjection projection_p)
	    : MultiFileReaderGlobalState(std::move(extra_columns_p), file_list_p), projection(projection_p) {
	}
	DuckLakeScanProjection projection;
};

struct DuckLakeMultiFileReader : public MultiFileReader {
public:
	static constexpr column_t COLUMN_IDENTIFIER_SNAPSHOT_ID = UINT64_C(10000000000000000000);

public:
	explicit DuckLakeMultiFileReader(DuckLakeFunctionInfo &read_info);
	~DuckLakeMultiFileReader() override;

	DuckLakeFunctionInfo &read_info;
	shared_ptr<DuckLakeDeleteMap> delete_map;

public:
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function);
	//! Return a DuckLakeMultiFileList
	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &options) override;

	//! Override the regular parquet bind using the MultiFileReader Bind. The bind from these are what DuckDB's file
	//! readers will try read
	bool Bind(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	          vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	//! Override the Options bind
	void BindOptions(MultiFileOptions &options, MultiFileList &files, vector<LogicalType> &return_types,
	                 vector<Identifier> &names, MultiFileReaderBindData &bind_data) override;

	ReaderInitializeType InitializeReader(MultiFileReaderData &reader_data, const MultiFileBindData &bind_data,
	                                      const vector<MultiFileColumnDefinition> &global_columns,
	                                      const vector<ColumnIndex> &global_column_ids,
	                                      optional_ptr<TableFilterSet> table_filters, ClientContext &context,
	                                      MultiFileGlobalState &gstate) override;

	unique_ptr<MultiFileReaderGlobalState>
	InitializeGlobalState(ClientContext &context, const MultiFileOptions &file_options,
	                      const MultiFileReaderBindData &bind_data, const MultiFileList &file_list,
	                      const vector<MultiFileColumnDefinition> &global_columns,
	                      const vector<ColumnIndex> &global_column_ids) override;

	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, GlobalTableFunctionState &gstate,
	                                        const OpenFileInfo &file, idx_t file_idx,
	                                        const MultiFileBindData &bind_data) override;
	shared_ptr<BaseFileReader> CreateReader(ClientContext &context, const OpenFileInfo &file,
	                                        BaseFileReaderOptions &options, const MultiFileOptions &file_options,
	                                        MultiFileReaderInterface &interface) override;

	ReaderInitializeType CreateMapping(ClientContext &context, MultiFileReaderData &reader_data,
	                                   const vector<MultiFileColumnDefinition> &global_columns,
	                                   const vector<ColumnIndex> &global_column_ids,
	                                   optional_ptr<TableFilterSet> filters, MultiFileList &multi_file_list,
	                                   const MultiFileReaderBindData &bind_data,
	                                   const virtual_column_map_t &virtual_columns) override;

	unique_ptr<Expression>
	GetVirtualColumnExpression(ClientContext &context, MultiFileReaderData &reader_data,
	                           const vector<MultiFileColumnDefinition> &local_columns, idx_t &column_id,
	                           const LogicalType &type, MultiFileLocalIndex local_index,
	                           optional_ptr<MultiFileColumnDefinition> &global_column_reference) override;

	unique_ptr<MultiFileReader> Copy() const override;

	void FinalizeChunk(ClientContext &context, const MultiFileBindData &bind_data, BaseFileReader &reader,
	                   const MultiFileReaderData &reader_data, DataChunk &input_chunk, DataChunk &output_chunk,
	                   ExpressionExecutor &executor, optional_ptr<MultiFileReaderGlobalState> global_state) override;

	static vector<MultiFileColumnDefinition> ColumnsFromFieldData(const DuckLakeFieldData &field_data,
	                                                              bool emit_key_value = false);

private:
	//! Compute the scan-wide projection state (whether row_id is internally projected, and the output-chunk positions
	//! of the rowid / snapshot_id columns for deletion scans). Depends only on the projected columns and the scan type,
	//! so it is identical for every file in a scan.
	static DuckLakeScanProjection ComputeScanProjection(const DuckLakeMultiFileList &file_list,
	                                                    const vector<ColumnIndex> &global_column_ids);
	shared_ptr<BaseFileReader> TryCreateInlinedDataReader(const OpenFileInfo &file);
	//! For deletion scans we need to get the snapshot_id values using per-row snapshot information
	void GatherDeletionScanSnapshots(BaseFileReader &reader, const MultiFileReaderData &reader_data, DataChunk &chunk,
	                                 const DuckLakeScanProjection &projection,
	                                 optional_idx rowid_col_override = optional_idx()) const;

private:
	unique_ptr<MultiFileColumnDefinition> row_id_column;
	unique_ptr<MultiFileColumnDefinition> snapshot_id_column;
	//! Inlined transaction-local data
	shared_ptr<DuckLakeInlinedData> transaction_local_data;
};

} // namespace duckdb
