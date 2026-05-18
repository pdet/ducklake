#include "storage/ducklake_commit_serializer.hpp"

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "common/ducklake_types.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_variant_stats.hpp"

#include <sstream>

namespace duckdb {

namespace {

string SQLLiteral(const string &s) {
	return DuckLakeUtil::SQLLiteralToString(s);
}

string IntLit(int64_t v) {
	return std::to_string(v);
}

string OptInt(optional_idx v) {
	return v.IsValid() ? std::to_string(static_cast<int64_t>(v.GetIndex())) : string("NULL");
}

string OptString(const string &s) {
	return s.empty() ? string("NULL") : SQLLiteral(s);
}

string OptBase64(const string &s) {
	return s.empty() ? string("NULL") : SQLLiteral(Blob::ToBase64(string_t(s)));
}

string BoolLit(bool v) {
	return v ? "1" : "0";
}

} // namespace

DuckLakeCommitSerializer::DuckLakeCommitSerializer(DuckLakeTransaction &transaction_p,
                                                   DuckLakeMetadataManager &metadata_manager_p, string uuid_p)
    : transaction(transaction_p), metadata_manager(metadata_manager_p), uuid(std::move(uuid_p)) {
}

string DuckLakeCommitSerializer::TempTableName(DuckLakeCommitKind kind) const {
	return "{METADATA_CATALOG}." + DuckLakeCommitTempTables::TableName(uuid, kind);
}

bool DuckLakeCommitSerializer::CanHandle() const {
	return transaction.HasOnlyDataChanges();
}

void DuckLakeCommitSerializer::Append(const string &sql) {
	pending_sql.append(sql);
	if (!sql.empty() && sql.back() != ';') {
		pending_sql.push_back(';');
	}
}

void DuckLakeCommitSerializer::Flush(const char *context) {
	(void)context;
}

void DuckLakeCommitSerializer::EnsureCreated(DuckLakeCommitKind kind) {
	if (created_kinds.find(kind) != created_kinds.end()) {
		return;
	}
	created_kinds.insert(kind);
	result.populated_table_names.insert(TempTableName(kind));
	auto &session = transaction.GetCatalog().CommitSession();
	if (session.IsKindCreated(kind)) {
		return;
	}
	Append(DuckLakeCommitTempTables::RenderDDL(uuid, kind));
	session.MarkKindCreated(kind);
}

DuckLakeCommitSerializerResult DuckLakeCommitSerializer::Serialize(TransactionChangeInformation &txn_changes) {
	auto transaction_snapshot = transaction.GetSnapshot();
	DuckLakeSnapshot commit_snapshot = transaction_snapshot;
	DuckLakeCommitState commit_state(commit_snapshot);

	StageDataFilesAndStats(commit_state);
	StageDeleteFiles(commit_state);
	StageSnapshotChanges(commit_state, txn_changes);

	bool has_inlined_flush = transaction.HasFlushedInlinedTables();
	bool schema_changed = transaction.SchemaChangesMade();
	result.meta.snapshot_id_was = static_cast<int64_t>(transaction_snapshot.snapshot_id);
	result.meta.schema_version_was = static_cast<int64_t>(transaction_snapshot.schema_version);
	result.meta.schema_changed = schema_changed;
	result.meta.has_inlined_flush = has_inlined_flush;
	result.meta.next_catalog_id = static_cast<int64_t>(commit_state.commit_snapshot.next_catalog_id);
	result.meta.next_file_id = static_cast<int64_t>(commit_state.commit_snapshot.next_file_id);
	result.meta.next_catalog_id_baseline = static_cast<int64_t>(transaction_snapshot.next_catalog_id);
	result.meta.next_file_id_baseline = static_cast<int64_t>(transaction_snapshot.next_file_id);

	result.staging_sql = std::move(pending_sql);
	pending_sql.clear();

	result.next_catalog_id = commit_snapshot.next_catalog_id;
	result.next_file_id = commit_snapshot.next_file_id;
	return result;
}

void DuckLakeCommitSerializer::StageDataFilesAndStats(DuckLakeCommitState &commit_state) {
	string throwaway_batch;
	auto new_data_info = transaction.GetNewDataFiles(throwaway_batch, commit_state, nullptr);
	if (new_data_info.new_files.empty()) {
		return;
	}

	EnsureCreated(DuckLakeCommitKind::DATA_FILES);
	EnsureCreated(DuckLakeCommitKind::FILE_COLUMN_STATS);

	vector<DuckLakeTableInfo> empty_tables;
	vector<DuckLakeSchemaInfo> empty_schemas;

	std::ostringstream data_file_values;
	std::ostringstream column_stats_values;
	std::ostringstream partition_value_values;
	std::ostringstream variant_stats_values;
	bool first_data_file = true;
	bool first_column_stat = true;
	bool first_partition_value = true;
	bool first_variant_stat = true;

	for (auto &file : new_data_info.new_files) {
		auto data_file_index = static_cast<int64_t>(file.id.index);
		auto table_id = static_cast<int64_t>(file.table_id.index);
		auto path = metadata_manager.GetRelativePath(file.table_id, file.file_name, empty_tables, empty_schemas);

		if (!first_data_file) {
			data_file_values << ", ";
		}
		first_data_file = false;
		data_file_values << "(" << data_file_index << ", " << table_id << ", NULL, " << SQLLiteral(path.path) << ", "
		                 << BoolLit(path.path_is_relative) << ", 'parquet', " << static_cast<int64_t>(file.row_count)
		                 << ", " << static_cast<int64_t>(file.file_size_bytes) << ", " << OptInt(file.footer_size)
		                 << ", " << OptInt(file.row_id_start) << ", " << OptInt(file.partition_id) << ", "
		                 << OptBase64(file.encryption_key) << ", "
		                 << (file.mapping_id.IsValid() ? IntLit(static_cast<int64_t>(file.mapping_id.index)) : "NULL")
		                 << ", " << OptInt(file.max_partial_file_snapshot) << ")";

		for (auto &stats_entry : file.column_stats) {
			auto column_id = static_cast<int64_t>(stats_entry.first.index);
			auto &stats = stats_entry.second;
			if (!first_column_stat) {
				column_stats_values << ", ";
			}
			first_column_stat = false;
			column_stats_values << "(" << data_file_index << ", " << table_id << ", " << column_id << ", "
			                    << static_cast<int64_t>(stats.column_size_bytes) << ", ";
			if (stats.has_null_count && stats.has_num_values && stats.null_count <= stats.num_values) {
				column_stats_values << static_cast<int64_t>(stats.num_values - stats.null_count) << ", "
				                    << static_cast<int64_t>(stats.null_count);
			} else {
				column_stats_values << "NULL, NULL";
			}
			column_stats_values << ", " << (stats.has_min ? SQLLiteral(stats.min) : "NULL") << ", "
			                    << (stats.has_max ? SQLLiteral(stats.max) : "NULL") << ", ";
			if (stats.has_contains_nan) {
				column_stats_values << BoolLit(stats.contains_nan);
			} else {
				column_stats_values << "NULL";
			}
			string extra_stats_str;
			if (stats.extra_stats && stats.extra_stats->TrySerialize(extra_stats_str)) {
				column_stats_values << ", " << extra_stats_str;
			} else {
				column_stats_values << ", NULL";
			}
			column_stats_values << ")";

			if (stats.extra_stats && stats.extra_stats->GetStatsType() == DuckLakeExtraStatsType::VARIANT) {
				auto &variant_extra = static_cast<DuckLakeColumnVariantStats &>(*stats.extra_stats);
				for (auto &variant_entry : variant_extra.shredded_field_stats) {
					auto &field_stats = variant_entry.second.field_stats;
					if (!first_variant_stat) {
						variant_stats_values << ", ";
					}
					first_variant_stat = false;
					variant_stats_values << "(" << data_file_index << ", " << table_id << ", " << column_id << ", "
					                     << SQLLiteral(variant_entry.first) << ", "
					                     << SQLLiteral(DuckLakeTypes::ToString(variant_entry.second.shredded_type))
					                     << ", " << static_cast<int64_t>(field_stats.column_size_bytes) << ", ";
					if (field_stats.has_null_count && field_stats.has_num_values &&
					    field_stats.null_count <= field_stats.num_values) {
						variant_stats_values << static_cast<int64_t>(field_stats.num_values - field_stats.null_count)
						                     << ", " << static_cast<int64_t>(field_stats.null_count);
					} else {
						variant_stats_values << "NULL, NULL";
					}
					variant_stats_values << ", " << (field_stats.has_min ? SQLLiteral(field_stats.min) : "NULL") << ", "
					                     << (field_stats.has_max ? SQLLiteral(field_stats.max) : "NULL") << ", ";
					if (field_stats.has_contains_nan) {
						variant_stats_values << BoolLit(field_stats.contains_nan);
					} else {
						variant_stats_values << "NULL";
					}
					string field_extra_stats_str;
					if (field_stats.extra_stats && field_stats.extra_stats->TrySerialize(field_extra_stats_str)) {
						variant_stats_values << ", " << field_extra_stats_str;
					} else {
						variant_stats_values << ", NULL";
					}
					variant_stats_values << ")";
				}
			}
		}

		for (auto &partition : file.partition_values) {
			if (!first_partition_value) {
				partition_value_values << ", ";
			}
			first_partition_value = false;
			partition_value_values << "(" << data_file_index << ", " << table_id << ", "
			                       << static_cast<int64_t>(partition.partition_column_idx) << ", "
			                       << (partition.partition_value.IsNull()
			                               ? "NULL"
			                               : SQLLiteral(partition.partition_value.ToString()))
			                       << ")";
		}
	}

	Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::DATA_FILES) + " VALUES " + data_file_values.str());
	if (!first_column_stat) {
		Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::FILE_COLUMN_STATS) + " VALUES " +
		       column_stats_values.str());
	}
	if (!first_partition_value) {
		EnsureCreated(DuckLakeCommitKind::FILE_PARTITION_VALUES);
		Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::FILE_PARTITION_VALUES) + " VALUES " +
		       partition_value_values.str());
	}
	if (!first_variant_stat) {
		EnsureCreated(DuckLakeCommitKind::FILE_VARIANT_STATS);
		Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::FILE_VARIANT_STATS) + " VALUES " +
		       variant_stats_values.str());
	}

	map<TableIndex, DuckLakeTableStats> per_table;
	for (auto &file : new_data_info.new_files) {
		auto entry = per_table.find(file.table_id);
		if (entry == per_table.end()) {
			DuckLakeTableStats baseline;
			auto current = transaction.GetCatalog().GetTableStats(transaction, file.table_id);
			if (current) {
				baseline = *current;
			}
			entry = per_table.emplace(file.table_id, std::move(baseline)).first;
		}
		auto &stats = entry->second;
		stats.table_size_bytes += file.file_size_bytes;
		if (!file.max_partial_file_snapshot.IsValid()) {
			stats.record_count += file.row_count;
			stats.next_row_id += file.row_count;
		}
		for (auto &col_stats : file.column_stats) {
			stats.MergeStats(col_stats.first, col_stats.second);
		}
	}

	if (per_table.empty()) {
		return;
	}

	EnsureCreated(DuckLakeCommitKind::TABLE_STATS);
	std::ostringstream table_ins;
	table_ins << "INSERT INTO " << TempTableName(DuckLakeCommitKind::TABLE_STATS) << " VALUES ";
	bool first = true;
	for (auto &entry : per_table) {
		auto table_id = entry.first;
		auto &stats = entry.second;
		if (!first) {
			table_ins << ", ";
		}
		first = false;
		table_ins << "(" << static_cast<int64_t>(table_id.index) << ", " << static_cast<int64_t>(stats.record_count)
		          << ", " << static_cast<int64_t>(stats.next_row_id) << ", "
		          << static_cast<int64_t>(stats.table_size_bytes) << ")";
	}
	Append(table_ins.str());

	std::ostringstream col_ins;
	bool col_first = true;
	for (auto &entry : per_table) {
		auto table_id = entry.first;
		auto &stats = entry.second;
		for (auto &col : stats.column_stats) {
			auto column_id = col.first;
			auto &cs = col.second;
			if (!col_first) {
				col_ins << ", ";
			}
			col_first = false;
			string contains_null;
			if (cs.has_null_count) {
				contains_null = BoolLit(cs.null_count > 0);
			} else {
				contains_null = "NULL";
			}
			string contains_nan = cs.has_contains_nan ? BoolLit(cs.contains_nan) : string("NULL");
			string min_val = cs.has_min ? DuckLakeUtil::StatsToString(cs.min) : string("NULL");
			string max_val = cs.has_max ? DuckLakeUtil::StatsToString(cs.max) : string("NULL");
			string extra_stats_str;
			if (cs.extra_stats && cs.extra_stats->TrySerialize(extra_stats_str)) {
			} else {
				extra_stats_str = "NULL";
			}
			col_ins << "(" << static_cast<int64_t>(table_id.index) << ", " << static_cast<int64_t>(column_id.index)
			        << ", " << contains_null << ", " << contains_nan << ", " << min_val << ", " << max_val << ", "
			        << extra_stats_str << ")";
		}
	}
	if (!col_first) {
		EnsureCreated(DuckLakeCommitKind::TABLE_COLUMN_STATS);
		Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::TABLE_COLUMN_STATS) + " VALUES " + col_ins.str());
	}
}

void DuckLakeCommitSerializer::StageDeleteFiles(DuckLakeCommitState &commit_state) {
	vector<DuckLakeOverwrittenDeleteFile> overwritten;
	auto delete_files = transaction.GetNewDeleteFiles(commit_state, overwritten);

	if (!overwritten.empty()) {
		EnsureCreated(DuckLakeCommitKind::OVERWRITTEN_DELETE_FILES);
		std::ostringstream values;
		bool first = true;
		for (auto &file : overwritten) {
			auto path = metadata_manager.GetRelativePath(file.path);
			if (!first) {
				values << ", ";
			}
			first = false;
			values << "(" << static_cast<int64_t>(file.delete_file_id.index) << ", " << SQLLiteral(path.path) << ", "
			       << BoolLit(path.path_is_relative) << ")";
		}
		Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::OVERWRITTEN_DELETE_FILES) + " VALUES " +
		       values.str());
	}

	if (delete_files.empty()) {
		return;
	}

	EnsureCreated(DuckLakeCommitKind::DELETE_FILES);

	vector<DuckLakeTableInfo> empty_tables;
	vector<DuckLakeSchemaInfo> empty_schemas;

	std::ostringstream values;
	bool first = true;
	for (auto &file : delete_files) {
		auto path = metadata_manager.GetRelativePath(file.table_id, file.path, empty_tables, empty_schemas);
		if (!first) {
			values << ", ";
		}
		first = false;
		values << "(" << static_cast<int64_t>(file.id.index) << ", " << static_cast<int64_t>(file.table_id.index)
		       << ", " << static_cast<int64_t>(file.data_file_id.index) << ", " << SQLLiteral(path.path) << ", "
		       << BoolLit(path.path_is_relative) << ", " << SQLLiteral(DeleteFileFormatToString(file.format)) << ", "
		       << static_cast<int64_t>(file.delete_count) << ", " << static_cast<int64_t>(file.file_size_bytes) << ", "
		       << static_cast<int64_t>(file.footer_size) << ", " << OptBase64(file.encryption_key) << ", "
		       << OptInt(file.max_snapshot) << ", " << OptInt(file.begin_snapshot) << ")";
	}
	Append("INSERT INTO " + TempTableName(DuckLakeCommitKind::DELETE_FILES) + " VALUES " + values.str());
}

void DuckLakeCommitSerializer::StageSnapshotChanges(DuckLakeCommitState &commit_state,
                                                    TransactionChangeInformation &txn_changes) {
	(void)commit_state;

	string changes_made;
	auto append_change = [&](const string &chunk) {
		if (!changes_made.empty()) {
			changes_made += ",";
		}
		changes_made += chunk;
	};
	auto stamp_set = [&](const set<TableIndex> &ids, const char *label) {
		for (auto &id : ids) {
			append_change(string(label) + ":" + to_string(commit_state.GetTableId(id).index));
		}
	};

	stamp_set(txn_changes.tables_inserted_into, "inserted_into_table");
	stamp_set(txn_changes.tables_deleted_from, "deleted_from_table");
	stamp_set(txn_changes.altered_tables, "altered_table");
	stamp_set(txn_changes.altered_views, "altered_view");
	stamp_set(txn_changes.tables_inserted_inlined, "inlined_insert");
	stamp_set(txn_changes.tables_deleted_inlined, "inlined_delete");
	stamp_set(txn_changes.tables_flushed_inlined, "inline_flush");

	if (changes_made.empty()) {
		return;
	}

	EnsureCreated(DuckLakeCommitKind::SNAPSHOT_CHANGES);

	auto &commit_info = transaction.GetCommitInfo();
	std::ostringstream sql;
	sql << "INSERT INTO " << TempTableName(DuckLakeCommitKind::SNAPSHOT_CHANGES)
	    << "(changes_made, author, commit_message, commit_extra_info) VALUES (" << SQLLiteral(changes_made) << ", "
	    << commit_info.author.ToSQLString() << ", " << commit_info.commit_message.ToSQLString() << ", "
	    << commit_info.commit_extra_info.ToSQLString() << ")";
	Append(sql.str());
}

} // namespace duckdb
