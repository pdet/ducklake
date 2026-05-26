//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/quack_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

class QuackMetadataManager : public DuckLakeMetadataManager {
public:
	explicit QuackMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<QuackMetadataManager>(transaction);
	}

	bool SupportsAppender() const override {
		return false;
	}
	void ProbeServerCapabilities() override;
	bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const override;
	void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                            const TransactionChangeInformation &transaction_changes,
	                            const DuckLakeRetryConfig &retry_config) override;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(string &query) override;
	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	void ClearCache() override;

	bool MetadataExists() override;
	DuckLakeMetadata LoadDuckLake() override;
	DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot) override;
	unique_ptr<DuckLakeSnapshot> GetSnapshot() override;
	vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot) override;

	idx_t GetRoundTripCount() const {
		return round_trip_count;
	}
	void ResetRoundTripCount() {
		round_trip_count = 0;
	}

protected:
	string MetadataExistsQuery() const override;

private:
	idx_t round_trip_count = 0;

	struct CachedInitData {
		bool valid = false;
		bool metadata_exists = false;
		bool has_ducklake_commit = false;
		DuckLakeMetadata metadata;
	};
	CachedInitData cached_init;

	struct CachedCatalogData {
		bool valid = false;
		DuckLakeSnapshot snapshot;
		DuckLakeCatalogInfo catalog;
		vector<DuckLakeGlobalStatsInfo> global_stats;
	};
	CachedCatalogData cached_catalog;

	void RunCombinedInit();
	void RunCombinedCatalogLoad();
	static DuckLakeCatalogInfo ParseCatalogFromRow(QueryResult &result, const string &data_path,
	                                               const string &separator);
};

} // namespace duckdb
