#include "metadata_manager/ducklake_metadata_manager_v2_0_dev2.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_version.hpp"

namespace duckdb {

template <typename Base>
string DuckLakeMetadataManagerV2_0Dev2<Base>::GetCreateTableStatements() {
	auto result = DuckLakeMetadataManagerV2_0<Base>::GetCreateTableStatements();
	result += DuckLakeMetadataManager::GetBranchTableStatement() + "\n";
	result += DuckLakeMetadataManager::GetSnapshotRemapTableStatement() + "\n";
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV2_0Dev2<Base>::GetSnapshotTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(" + DuckLakeMetadataManager::GetSnapshotTableColumns() +
	       ", branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0Dev2<Base>::GetInitialDataStatements() {
	return DuckLakeMetadataManager::GetMainBranchRowStatement() + "\n";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0Dev2<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V2_0_DEV2;
	return DuckLakeVersionToString(VERSION);
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV2_0Dev2<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV2_0Dev2<SQLiteMetadataManager>;
template class DuckLakeMetadataManagerV2_0Dev2<PostgresMetadataManager>;
template class DuckLakeMetadataManagerV2_0Dev2<QuackMetadataManager>;

} // namespace duckdb
