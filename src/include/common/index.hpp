//===----------------------------------------------------------------------===//
//                         DuckDB
//
// common/index.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

struct DuckLakeConstants {
	static constexpr const idx_t TRANSACTION_LOCAL_ID_START = 9223372036854775808ULL;
};

template <typename Derived>
struct DuckLakeIndex {
	DuckLakeIndex() : index(DConstants::INVALID_INDEX) {
	}
	explicit DuckLakeIndex(idx_t index) : index(index) {
	}

	idx_t index;

	inline bool operator==(const Derived &rhs) const {
		return index == rhs.index;
	};
	inline bool operator!=(const Derived &rhs) const {
		return index != rhs.index;
	};
	inline bool operator<(const Derived &rhs) const {
		return index < rhs.index;
	};
	bool IsValid() const {
		return index != DConstants::INVALID_INDEX;
	}
};

template <typename Derived>
struct DuckLakeTransactionLocalIndex : public DuckLakeIndex<Derived> {
	using DuckLakeIndex<Derived>::DuckLakeIndex;
	bool IsTransactionLocal() const {
		D_ASSERT(this->IsValid());
		return this->index >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
};

struct SchemaIndex : public DuckLakeTransactionLocalIndex<SchemaIndex> {
	using DuckLakeTransactionLocalIndex::DuckLakeTransactionLocalIndex;
};

struct TableIndex : public DuckLakeTransactionLocalIndex<TableIndex> {
	using DuckLakeTransactionLocalIndex::DuckLakeTransactionLocalIndex;
};

struct MacroIndex : public DuckLakeTransactionLocalIndex<MacroIndex> {
	using DuckLakeTransactionLocalIndex::DuckLakeTransactionLocalIndex;
};

struct FieldIndex : public DuckLakeIndex<FieldIndex> {
	using DuckLakeIndex::DuckLakeIndex;
};

struct DataFileIndex : public DuckLakeIndex<DataFileIndex> {
	using DuckLakeIndex::DuckLakeIndex;
};

struct MappingIndex : public DuckLakeIndex<MappingIndex> {
	using DuckLakeIndex::DuckLakeIndex;
};

} // namespace duckdb
