//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_initializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_catalog.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {
class DuckLakeTransaction;

class DuckLakeInitializer {
public:
	DuckLakeInitializer(ClientContext &context, DuckLakeCatalog &catalog, DuckLakeOptions &options);

public:
	void Initialize();

private:
	void InitializeNewDuckLake(DuckLakeTransaction &transaction, bool has_explicit_schema);
	void LoadExistingDuckLake(DuckLakeTransaction &transaction);
	//! Loads an existing DuckLake, retrying once without the migration of a development catalog
	void LoadDuckLakeRetryWithoutDevMigration(DuckLakeTransaction &transaction);
	void AttachMetadata(DuckLakeTransaction &transaction);
	//! Whether the metadata catalog holds a complete DuckLake
	bool DuckLakeIsInitialized(DuckLakeTransaction &transaction);
	void InitializeDataPath();
	string GetAttachOptions();
	void SetVersionedMetadataManager(DuckLakeTransaction &transaction, DuckLakeVersion version);
	DuckLakeVersion ResolveTargetVersion(DuckLakeVersion catalog_version, const string &catalog_version_str);

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeOptions &options;
	//! Set when the migration of a development catalog must not run again
	bool skip_dev_migration = false;
};

} // namespace duckdb
