//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_flush_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/index_vector.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {
class DuckLakeTableEntry;

//! Base class for DuckLake flush operators
class DuckLakeFlushOperator : public PhysicalOperator {
public:
	DuckLakeFlushOperator(PhysicalPlan &physical_plan, const vector<LogicalType> &types, DuckLakeTableEntry &table,
	                      string encryption_key);

	DuckLakeTableEntry &table;
	string encryption_key;

	bool IsSource() const override {
		return true;
	}
};

//! Operator for flushing inlined insertions (small inserts stored in catalog) to parquet files
class DuckLakeFlushInlinedInsertions : public DuckLakeFlushOperator {
public:
	DuckLakeFlushInlinedInsertions(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
	                               DuckLakeTableEntry &table, DuckLakeInlinedTableInfo inlined_table,
	                               string encryption_key, optional_idx partition_id, PhysicalOperator &child);

	DuckLakeInlinedTableInfo inlined_table;
	optional_idx partition_id;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return false;
	}

	string GetName() const override;
};

//! Operator for flushing inlined file deletions (positions deleted from parquet files stored in catalog)
class DuckLakeFlushInlinedFileDeletions : public DuckLakeFlushOperator {
public:
	DuckLakeFlushInlinedFileDeletions(PhysicalPlan &physical_plan, const vector<LogicalType> &types,
	                                  DuckLakeTableEntry &table, string inlined_table_name, string encryption_key);

	string inlined_table_name;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	string GetName() const override;
};

} // namespace duckdb
