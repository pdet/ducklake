//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/quack_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
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
	bool SupportsTempTableCommit() const override;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(string &query) override;
	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	void ClearCache() override;

	bool MetadataExists() override;

protected:
	string MetadataExistsQuery() const override;

private:
	mutable mutex probe_lock;
	mutable bool checked_ducklake_in_server = false;
	mutable bool is_ducklake_in_server = false;
};

} // namespace duckdb
