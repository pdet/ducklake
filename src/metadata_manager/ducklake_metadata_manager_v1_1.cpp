#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_version.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetCreateTableStatements() {
	string result = Base::GetCreateTableStatements();
	result += "\nCREATE TABLE ";
	result += DUCKLAKE_DELETE_VECTOR_TABLE_DDL;
	result += ";\n";
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V1_1_DEV_1;
	return DuckLakeVersionToString(VERSION);
}

// --- Delete vector query support ---

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetDeleteFileSelectList(const string &prefix) {
	return Base::GetDeleteFileSelectList(prefix) + ", " + prefix + ".delete_vector_offset AS " + prefix +
	       "_delete_vector_offset" + ", " + prefix + ".delete_vector_size AS " + prefix + "_delete_vector_size";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetDeleteFileWithVectorJoin(idx_t table_id_val, const string &snapshot_filter,
                                                                      const string &dv_snapshot_filter) {
	return StringUtil::Format(R"(SELECT df.*, dv.delete_vector_offset, dv.delete_vector_size
    FROM {METADATA_CATALOG}.ducklake_delete_file df
    LEFT JOIN (
        SELECT DISTINCT ON (delete_file_id) delete_file_id, delete_vector_offset, delete_vector_size
        FROM {METADATA_CATALOG}.ducklake_delete_vector
        WHERE %s
        ORDER BY delete_file_id, begin_snapshot DESC
    ) dv USING (delete_file_id)
    WHERE df.table_id=%d AND %s)",
	                          dv_snapshot_filter, table_id_val, snapshot_filter);
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetDeleteFileLateralJoinSQL(idx_t table_id_val, const string &where_clause,
                                                                      const string &dv_filter) {
	return StringUtil::Format(R"(
	SELECT DISTINCT ON (df.data_file_id) df.*, dv.delete_vector_offset, dv.delete_vector_size
	FROM {METADATA_CATALOG}.ducklake_delete_file df
	LEFT JOIN (
		SELECT DISTINCT ON (delete_file_id) delete_file_id, delete_vector_offset, delete_vector_size
		FROM {METADATA_CATALOG}.ducklake_delete_vector WHERE %s ORDER BY delete_file_id, begin_snapshot DESC
	) dv USING (delete_file_id)
	WHERE %s
	ORDER BY df.data_file_id, df.begin_snapshot DESC
)",
	                          dv_filter, where_clause);
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetNullDeleteSentinel() const {
	return Base::GetNullDeleteSentinel() + R"(,
		CAST(NULL AS BIGINT) AS delete_vector_offset,
		CAST(NULL AS BIGINT) AS delete_vector_size
)";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetNullDeleteFileColumns() const {
	return Base::GetNullDeleteFileColumns() + ", NULL delete_vector_offset, NULL delete_vector_size";
}

template <typename Base>
bool DuckLakeMetadataManagerV1_1<Base>::HasDeleteVectorColumns() const {
	return true;
}

// --- Delete vector read/write ---

template <typename Base>
vector<DuckLakeDeleteVectorInfo> DuckLakeMetadataManagerV1_1<Base>::GetDeleteVectors(DataFileIndex delete_file_id) {
	auto result = this->transaction.Query(StringUtil::Format(R"(
SELECT begin_snapshot, end_snapshot, delete_vector_offset, delete_vector_size, delete_count
FROM {METADATA_CATALOG}.ducklake_delete_vector
WHERE delete_file_id = %d
ORDER BY begin_snapshot
)",
	                                                         delete_file_id.index));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get delete vectors from DuckLake: ");
	}
	vector<DuckLakeDeleteVectorInfo> vectors;
	for (auto &row : *result) {
		DuckLakeDeleteVectorInfo vec;
		vec.begin_snapshot = row.template GetValue<idx_t>(0);
		if (!row.IsNull(1)) {
			vec.end_snapshot = row.template GetValue<idx_t>(1);
		}
		vec.delete_vector_offset = row.template GetValue<idx_t>(2);
		vec.delete_vector_size = row.template GetValue<idx_t>(3);
		vec.delete_count = row.template GetValue<idx_t>(4);
		vectors.push_back(vec);
	}
	return vectors;
}

template <typename Base>
string
DuckLakeMetadataManagerV1_1<Base>::WriteNewDeleteVectors(const vector<DuckLakeDeleteFileInfo> &new_delete_files) {
	string insert_query;
	for (auto &file : new_delete_files) {
		if (file.delete_vectors.empty()) {
			continue;
		}
		for (idx_t i = 0; i < file.delete_vectors.size(); i++) {
			auto &vec = file.delete_vectors[i];
			if (!insert_query.empty()) {
				insert_query += ",";
			}
			string begin_snapshot_str =
			    vec.begin_snapshot.IsValid() ? to_string(vec.begin_snapshot.GetIndex()) : "{SNAPSHOT_ID}";
			string end_snapshot_str;
			if (vec.end_snapshot.IsValid()) {
				end_snapshot_str = to_string(vec.end_snapshot.GetIndex());
			} else if (i + 1 < file.delete_vectors.size()) {
				end_snapshot_str = "{SNAPSHOT_ID}";
			} else {
				end_snapshot_str = "NULL";
			}
			insert_query +=
			    StringUtil::Format("(%d, %s, %s, %d, %d, %d)", file.id.index, begin_snapshot_str, end_snapshot_str,
			                       vec.delete_vector_offset, vec.delete_vector_size, vec.delete_count);
		}
	}
	if (insert_query.empty()) {
		return {};
	}
	return StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_delete_vector VALUES %s;", insert_query);
}

// --- Delete vector cleanup hooks ---

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::DeleteVectorCleanupSQL(const string &delete_file_ids) {
	return StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_delete_vector
WHERE delete_file_id IN (%s);
)",
	                          delete_file_ids);
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::DeleteVectorCleanupForDataFilesSQL(const string &data_file_ids) {
	return StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_delete_vector
WHERE delete_file_id IN (SELECT delete_file_id FROM {METADATA_CATALOG}.ducklake_delete_file WHERE data_file_id IN (%s));
)",
	                          data_file_ids);
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::EndSnapshotDeleteVectorSQL(idx_t delete_file_id, idx_t snapshot) {
	return StringUtil::Format(R"(
			UPDATE {METADATA_CATALOG}.ducklake_delete_vector SET end_snapshot = %llu
			WHERE delete_file_id = %llu AND end_snapshot IS NULL;
			)",
	                          snapshot, delete_file_id);
}

template <typename Base>
void DuckLakeMetadataManagerV1_1<Base>::CleanupDeleteVectorsForSnapshotDeletion(const string &deleted_delete_ids) {
	auto result = this->transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_delete_vector
WHERE delete_file_id IN (%s);
)",
	                                                         deleted_delete_ids));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to delete old delete vector information in DuckLake: ");
	}
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV1_1<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV1_1<SQLiteMetadataManager>;
template class DuckLakeMetadataManagerV1_1<PostgresMetadataManager>;

} // namespace duckdb
