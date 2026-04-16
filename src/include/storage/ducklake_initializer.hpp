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
	//! Wrap the metadata manager in the appropriate versioned wrapper for the given version
	static void SetVersionedMetadataManager(DuckLakeTransaction &transaction, DuckLakeVersion version);

private:
	void InitializeNewDuckLake(DuckLakeTransaction &transaction, bool has_explicit_schema);
	void LoadExistingDuckLake(DuckLakeTransaction &transaction);
	void InitializeDataPath();
	string GetAttachOptions();
	void CheckAndAutoloadedRequiredExtension(const string &pattern);
	DuckLakeVersion ResolveTargetVersion(DuckLakeVersion catalog_version, const string &catalog_version_str);

private:
	ClientContext &context;
	DuckLakeCatalog &catalog;
	DuckLakeOptions &options;
};

} // namespace duckdb
