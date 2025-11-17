//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_macro_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/mutex.hpp"
#include "common/index.hpp"
#include "common/local_change.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {
struct SetCommentInfo;
class DuckLakeTransaction;

class DuckLakeMacroEntry : public MacroCatalogEntry {
public:
	DuckLakeMacroEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateMacroInfo &info, MacroIndex &macro_index)
	    : MacroCatalogEntry(catalog, schema, info), index(macro_index) {};

	MacroIndex GetIndex() {
		return index;
	}

private:
	MacroIndex index;
};

} // namespace duckdb
