#include "metadata_manager/ducklake_metadata_manager_v1_2.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_version.hpp"

namespace duckdb {

template <typename Base>
string DuckLakeMetadataManagerV1_2<Base>::GetCreateTableStatements() {
	auto result = DuckLakeMetadataManagerV1_1<Base>::GetCreateTableStatements();
	result += DuckLakeMetadataManager::GetSnapshotTagTableStatement() + "\n";
	result += DuckLakeMetadataManager::CatalogMutexRowSql();
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV1_2<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V1_2_DEV1;
	return DuckLakeVersionToString(VERSION);
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV1_2<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV1_2<SQLiteMetadataManager>;
template class DuckLakeMetadataManagerV1_2<PostgresMetadataManager>;
template class DuckLakeMetadataManagerV1_2<QuackMetadataManager>;

} // namespace duckdb
