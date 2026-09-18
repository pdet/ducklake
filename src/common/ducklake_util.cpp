#include "common/ducklake_util.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/common/file_system.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/function/scalar/struct_utils.hpp"
#include "duckdb/function/scalar/variant_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector/list_vector.hpp"
#include "duckdb/common/type_visitor.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "duckdb/main/database.hpp"

#include <cmath>

namespace duckdb {

string DuckLakeUtil::ParseQuotedValue(const string &input, idx_t &pos) {
	if (pos >= input.size() || input[pos] != '"') {
		throw InvalidInputException("Failed to parse quoted value - expected a quote");
	}
	string result;
	pos++;
	for (; pos < input.size(); pos++) {
		if (input[pos] == '"') {
			pos++;
			// check if this is an escaped quote
			if (pos < input.size() && input[pos] == '"') {
				// escaped quote
				result += '"';
				continue;
			}
			return result;
		}
		result += input[pos];
	}
	throw InvalidInputException("Failed to parse quoted value - unterminated quote");
}

string DuckLakeUtil::ToQuotedList(const vector<string> &input, char list_separator) {
	string result;
	for (auto &str : input) {
		if (!result.empty()) {
			result += list_separator;
		}
		result += SQLQuotedIdentifier::ToString(str);
	}
	return result;
}

vector<string> DuckLakeUtil::ParseQuotedList(const string &input, char list_separator) {
	vector<string> result;
	if (input.empty()) {
		return result;
	}
	idx_t pos = 0;
	while (true) {
		result.push_back(ParseQuotedValue(input, pos));
		if (pos >= input.size()) {
			break;
		}
		if (input[pos] != list_separator) {
			throw InvalidInputException("Failed to parse list - expected a %s", string(1, list_separator));
		}
		pos++;
	}
	return result;
}

ParsedCatalogEntry DuckLakeUtil::ParseCatalogEntry(const string &input) {
	ParsedCatalogEntry result_data;
	idx_t pos = 0;
	result_data.schema = DuckLakeUtil::ParseQuotedValue(input, pos);
	if (pos >= input.size() || input[pos] != '.') {
		throw InvalidInputException("Failed to parse catalog entry - expected a dot");
	}
	pos++;
	result_data.name = DuckLakeUtil::ParseQuotedValue(input, pos);
	if (pos < input.size()) {
		throw InvalidInputException("Failed to parse catalog entry - trailing data after quoted value");
	}
	return result_data;
}

string DuckLakeUtil::SQLIdentifierToString(const string &text) {
	return "\"" + StringUtil::Replace(text, "\"", "\"\"") + "\"";
}

string DuckLakeUtil::SQLIdentifierToString(const Identifier &identifier) {
	return SQLQuotedIdentifier::ToString(identifier.GetIdentifierName());
}

string DuckLakeUtil::SQLLiteralToString(const string &text) {
	return "'" + StringUtil::Replace(text, "'", "''") + "'";
}

string DuckLakeUtil::StatsToString(const string &text) {
	for (auto c : text) {
		if (c == '\0') {
			return "NULL";
		}
	}
	return DuckLakeUtil::SQLLiteralToString(text);
}

static string EscapeVarcharForSQL(const string &str_val) {
	string ret;
	bool concat = false;
	for (auto c : str_val) {
		switch (c) {
		case '\0':
			concat = true;
			ret += "', chr(0), '";
			break;
		case '\'':
			ret += "''";
			break;
		default:
			ret += c;
			break;
		}
	}
	if (concat) {
		return "CONCAT('" + ret + "')";
	}
	return "'" + ret + "'";
}

string ToSQLString(DuckLakeMetadataManager &metadata_manager, const Value &value) {
	if (value.IsNull()) {
		return value.ToString();
	}
	string value_type = value.type().ToString();
	bool use_native_type = metadata_manager.TypeIsNativelySupported(value.type());
	if (!use_native_type) {
		value_type = "VARCHAR";
	} else {
		value_type = metadata_manager.GetColumnTypeInternal(value.type());
	}
	switch (value.type().id()) {
	case LogicalTypeId::UUID:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIME:
	case LogicalTypeId::TIME_NS:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIME_TZ:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::GEOMETRY:
		// ANSI CAST(value AS type) instead of the PostgreSQL-flavored
		// `'value'::type` operator: SQLite's parser rejects `::` outright,
		// which breaks SQLite-backed metadata backends that ship these
		// inlined-INSERT batches directly to SQLite.
		return StringUtil::Format("CAST('%s' AS %s)", value.ToString(), value_type);
	case LogicalTypeId::INTERVAL: {
		auto interval = IntervalValue::Get(value);
		return StringUtil::Format("CAST('%d months %d days %lld microseconds' AS %s)", interval.months, interval.days,
		                          interval.micros, value_type);
	}
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::ENUM:
		return EscapeVarcharForSQL(value.ToString());
	case LogicalTypeId::VARIANT: {
		Vector tmp(value, count_t(1));
		RecursiveUnifiedVectorFormat format;
		Vector::RecursiveToUnifiedFormat(tmp, format);
		UnifiedVariantVectorData vector_data(format);
		if (!use_native_type) {
			// DuckLakeInlinedChunkEncoder turns these into BLOB columns before the rows are formatted
			throw InternalException("VARIANT values must be encoded as Parquet Variant blobs before being "
			                        "inlined in this catalog type");
		}
		auto val = VariantUtils::ConvertVariantToValue(vector_data, 0, 0);
		// store the variant's value as a typed literal - the explicit cast keeps e.g. a VARIANT[] holding
		// [1, 'x'] from being bound as an INT32 list
		return "CAST(" + ToSQLString(metadata_manager, val) + " AS VARIANT)";
	}
	case LogicalTypeId::STRUCT: {
		if (!metadata_manager.TypeIsNativelySupported(value.type())) {
			// Stored as VARCHAR text - use ToString() which produces parseable format
			return value.ToString();
		}
		auto &child_types = StructType::GetChildTypes(value.type());
		auto &struct_values = StructValue::GetChildren(value);
		if (struct_values.empty()) {
			return "NULL";
		}
		bool is_unnamed = StructType::IsUnnamed(value.type());
		string ret = is_unnamed ? "(" : "{";
		for (idx_t i = 0; i < struct_values.size(); i++) {
			auto &name = child_types[i].first;
			auto &child = struct_values[i];
			if (is_unnamed) {
				ret += ToSQLString(metadata_manager, child);
			} else {
				ret += "'" + StringUtil::Replace(name.GetIdentifierName(), "'", "''") +
				       "': " + ToSQLString(metadata_manager, child);
			}
			if (i < struct_values.size() - 1) {
				ret += ", ";
			}
		}
		ret += is_unnamed ? ")" : "}";
		return ret;
	}
	case LogicalTypeId::FLOAT: {
		float fval = FloatValue::Get(value);
		if (!Value::FloatIsFinite(fval) || (fval == 0.0f && std::signbit(fval))) {
			return StringUtil::Format("CAST('%s' AS %s)", value.ToString(), value_type);
		}
		return value.ToString();
	}
	case LogicalTypeId::DOUBLE: {
		double val = DoubleValue::Get(value);
		if (!Value::DoubleIsFinite(val) || (val == 0.0 && std::signbit(val))) {
			return StringUtil::Format("CAST('%s' AS %s)", value.ToString(), value_type);
		}
		return value.ToString();
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::ARRAY: {
		if (!metadata_manager.TypeIsNativelySupported(value.type())) {
			// Stored as VARCHAR text - use ToString() which produces parseable format
			return value.ToString();
		}
		auto &children =
		    value.type().id() == LogicalTypeId::LIST ? ListValue::GetChildren(value) : ArrayValue::GetChildren(value);
		string ret = "[";
		for (idx_t i = 0; i < children.size(); i++) {
			ret += ToSQLString(metadata_manager, children[i]);
			if (i < children.size() - 1) {
				ret += ", ";
			}
		}
		ret += "]";
		return ret;
	}
	case LogicalTypeId::MAP: {
		if (!metadata_manager.TypeIsNativelySupported(value.type())) {
			return value.ToString();
		}
		string ret = "MAP(";
		auto &map_values = MapValue::GetChildren(value);
		ret += "[";
		for (idx_t i = 0; i < map_values.size(); i++) {
			if (i > 0) {
				ret += ", ";
			}
			auto &map_children = StructValue::GetChildren(map_values[i]);
			ret += ToSQLString(metadata_manager, map_children[0]);
		}
		ret += "], [";
		for (idx_t i = 0; i < map_values.size(); i++) {
			if (i > 0) {
				ret += ", ";
			}
			auto &map_children = StructValue::GetChildren(map_values[i]);
			ret += ToSQLString(metadata_manager, map_children[1]);
		}
		ret += "])";
		return ret;
	}
	case LogicalTypeId::UNION: {
		string ret = "union_value(";
		auto union_tag = UnionValue::GetTag(value);
		auto &tag_name = UnionType::GetMemberName(value.type(), union_tag);
		ret += tag_name + " := ";
		ret += UnionValue::GetValue(value).ToSQLString();
		ret += ")";
		return ret;
	}
	default:
		return value.ToString();
	}
}

string ToByteaHexLiteral(const string &raw_bytes) {
	string hex;
	for (unsigned char c : raw_bytes) {
		hex += StringUtil::Format("%02x", static_cast<int>(c));
	}
	return "'\\x" + hex + "'";
}

string DuckLakeUtil::ValueToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, const Value &val) {
	// FIXME: this should be upstreamed
	if (val.IsNull()) {
		return val.ToString();
	}
	if (val.type().HasAlias()) {
		// extension type: cast to string
		auto str_val = val.CastAs(context, LogicalType::VARCHAR);
		return ValueToSQL(metadata_manager, context, str_val);
	}
	string result;
	switch (val.type().id()) {
	case LogicalTypeId::VARCHAR: {
		auto &str_val = StringValue::Get(val);
		if (!metadata_manager.TypeIsNativelySupported(LogicalType::VARCHAR)) {
			return ToByteaHexLiteral(str_val);
		}
		return EscapeVarcharForSQL(str_val);
	}
	case LogicalTypeId::BLOB: {
		if (!metadata_manager.TypeIsNativelySupported(LogicalType::BLOB)) {
			return ToByteaHexLiteral(StringValue::Get(val));
		}
		result = ToSQLString(metadata_manager, val);
		break;
	}
	default:
		result = ToSQLString(metadata_manager, val);
	}
	if (metadata_manager.TypeIsNativelySupported(val.type()) || !val.type().IsNested()) {
		return result;
	}
	return StringUtil::Format("%s", SQLString(result));
}

void DuckLakeUtil::EnsureDirectoryExists(FileSystem &fs, const string &data_path) {
	if (!fs.IsRemoteFile(data_path)) {
		try {
			fs.CreateDirectoriesRecursive(data_path);
		} catch (...) {
		}
	}
}

string DuckLakeUtil::JoinPath(FileSystem &fs, const string &a, const string &b) {
	auto sep = fs.PathSeparator(a);
	if (StringUtil::EndsWith(a, sep)) {
		return a + b;
	} else {
		return a + sep + b;
	}
}

shared_ptr<DynamicFilterData> DuckLakeUtil::GetOptionalDynamicFilterData(const TableFilter &filter) {
	auto dynamic_filter_data = ExpressionFilter::GetRootOptionalDynamicFilterData(filter);
	if (dynamic_filter_data) {
		return dynamic_filter_data;
	}

	auto &expression_filter =
	    ExpressionFilter::GetExpressionFilter(filter, "DuckLakeUtil::GetOptionalDynamicFilterData");
	if (expression_filter.expr->GetExpressionClass() != ExpressionClass::BOUND_CONJUNCTION) {
		return nullptr;
	}
	auto &conjunction = expression_filter.expr->Cast<BoundConjunctionExpression>();
	if (conjunction.GetExpressionType() != ExpressionType::CONJUNCTION_AND) {
		return nullptr;
	}
	for (auto &child : conjunction.GetChildren()) {
		ExpressionFilter child_filter(child->Copy());
		dynamic_filter_data = GetOptionalDynamicFilterData(child_filter);
		if (dynamic_filter_data) {
			return dynamic_filter_data;
		}
	}
	return nullptr;
}

unique_ptr<Expression> DuckLakeUtil::MergeFilterExpressions(unique_ptr<Expression> left, unique_ptr<Expression> right) {
	vector<unique_ptr<Expression>> conjuncts;
	conjuncts.push_back(std::move(left));
	conjuncts.push_back(std::move(right));
	LogicalFilter::SplitPredicates(conjuncts);

	vector<unique_ptr<Expression>> merged;
	for (auto &conjunct : conjuncts) {
		bool is_duplicate = false;
		for (auto &existing : merged) {
			if (existing->Equals(*conjunct)) {
				is_duplicate = true;
				break;
			}
		}
		if (!is_duplicate) {
			merged.push_back(std::move(conjunct));
		}
	}
	if (merged.size() == 1) {
		return std::move(merged[0]);
	}
	auto result = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND);
	for (auto &conjunct : merged) {
		result->GetChildrenMutable().push_back(std::move(conjunct));
	}
	return std::move(result);
}

//! Resolve which child of the input struct a struct_extract reads, rejecting a position the type cannot hold
static bool TryResolveStructExtractChild(const Expression &expr, idx_t &position) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) {
		return false;
	}
	auto &func = expr.Cast<BoundFunctionExpression>();
	if (func.GetChildren().empty()) {
		return false;
	}
	// stats are stored against a named field, so an unnamed struct (TUPLE) has nothing to resolve against
	auto &input_type = func.GetChildren()[0]->GetReturnType();
	if (input_type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	if (!TryGetStructExtractChildIndex(func, position)) {
		return false;
	}
	return position < StructType::GetChildCount(input_type);
}

bool DuckLakeUtil::IsStructExtract(const Expression &expr) {
	idx_t position;
	return TryResolveStructExtractChild(expr, position);
}

//! Walk to the sub-expressions a filter reads a column through, without descending into them
static void FindFilterSubject(const Expression &expr, optional_ptr<const Expression> &subject, bool &conflict) {
	if (conflict) {
		return;
	}
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF ||
	    expr.GetExpressionClass() == ExpressionClass::BOUND_REF || DuckLakeUtil::IsStructExtract(expr)) {
		if (subject && !subject->Equals(expr)) {
			conflict = true;
		} else {
			subject = expr;
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(
	    expr, [&](const Expression &child) { FindFilterSubject(child, subject, conflict); });
}

optional_ptr<const Expression> DuckLakeUtil::GetFilterSubject(const Expression &expr) {
	optional_ptr<const Expression> subject;
	bool conflict = false;
	FindFilterSubject(expr, subject, conflict);
	return conflict ? nullptr : subject;
}

const Expression &DuckLakeUtil::GetFilterSubjectPath(const Expression &subject, vector<string> &path) {
	reference<const Expression> current = subject;
	idx_t position;
	while (TryResolveStructExtractChild(current.get(), position)) {
		auto &func = current.get().Cast<BoundFunctionExpression>();
		auto &input_type = func.GetChildren()[0]->GetReturnType();
		// the key is matched case-insensitively at bind time, so take the name from the struct type
		path.push_back(StructType::GetChildName(input_type, position).GetIdentifierName());
		current = *func.GetChildren()[0];
	}
	return current.get();
}

//! Rewrite the subject to the column placeholder an ExpressionFilter is evaluated against
unique_ptr<Expression> DuckLakeUtil::ReplaceFilterSubject(const Expression &expr, const Expression &subject,
                                                          const LogicalType &type) {
	if (expr.Equals(subject)) {
		return make_uniq<BoundReferenceExpression>(type, 0U);
	}
	auto result = expr.Copy();
	ExpressionIterator::EnumerateChildren(*result, [&](unique_ptr<Expression> &child) {
		child = DuckLakeUtil::ReplaceFilterSubject(*child, subject, type);
	});
	return result;
}

bool DuckLakeUtil::IsInlinedSystemColumn(const string &name, bool prefixed_inlined_columns) {
	if (prefixed_inlined_columns) {
		return StringUtil::CIStartsWith(name, DuckLakeInlinedColNames::PREFIX);
	}
	return DuckLakeInlinedColNames(false).ConflictsWith(name);
}

static void ThrowReservedInlinedColumn(const string &name, bool prefixed_inlined_columns) {
	if (prefixed_inlined_columns) {
		throw BinderException("Column name \"%s\" is reserved by DuckLake for internal use: column names starting "
		                      "with \"%s\" are not allowed.",
		                      name, DuckLakeInlinedColNames::PREFIX);
	}
	throw BinderException(
	    "Column name \"%s\" is reserved by DuckLake for internal use when data inlining is enabled. If "
	    "you must use this column name, disable inlining by calling "
	    "ducklake_set_option('data_inlining_row_limit', 0).",
	    name);
}

void DuckLakeUtil::ValidateInlinedSystemColumn(DuckLakeCatalog &catalog, ClientContext &context, SchemaIndex schema_id,
                                               TableIndex table_id, const string &name) {
	bool prefixed_inlined_columns = catalog.SupportsV1_1Metadata();
	if (!prefixed_inlined_columns && catalog.DataInliningRowLimit(context, schema_id, table_id) == 0) {
		return;
	}
	if (IsInlinedSystemColumn(name, prefixed_inlined_columns)) {
		ThrowReservedInlinedColumn(name, prefixed_inlined_columns);
	}
}

void DuckLakeUtil::ValidateNoInlinedSystemColumns(DuckLakeCatalog &catalog, ClientContext &context,
                                                  SchemaIndex schema_id, const ColumnList &columns) {
	bool prefixed_inlined_columns = catalog.SupportsV1_1Metadata();
	if (!prefixed_inlined_columns && catalog.DataInliningRowLimit(context, schema_id, TableIndex()) == 0) {
		return;
	}
	for (auto &col : columns.Logical()) {
		if (IsInlinedSystemColumn(col.Name().GetIdentifierName(), prefixed_inlined_columns)) {
			ThrowReservedInlinedColumn(col.Name().GetIdentifierName(), prefixed_inlined_columns);
		}
	}
}

void DuckLakeUtil::ValidateCanEnableInlining(const ColumnList &columns, bool prefixed_inlined_columns,
                                             const string &table_name) {
	DuckLakeInlinedColNames col_names(prefixed_inlined_columns);
	for (auto &col : columns.Logical()) {
		if (col_names.ConflictsWith(col.Name().GetIdentifierName())) {
			throw BinderException(
			    "Cannot enable data inlining for table \"%s\". Column \"%s\" conflicts with a reserved DuckLake "
			    "internal column name used for inlining. To enable inlining for this table, rename or drop column "
			    "\"%s\".",
			    table_name, col.Name().GetIdentifierName(), col.Name().GetIdentifierName());
		}
	}
}

string DuckLakeUtil::ReplaceSkippingQuotes(const string &sql, const string &from, const string &to) {
	if (from.empty()) {
		return sql;
	}

	auto tokens = Parser::Tokenize(sql);

	// Collect quoted ranges (string constants and double-quoted identifiers) where replacement doesn't happen
	vector<pair<idx_t, idx_t>> no_replace_ranges;
	for (idx_t i = 0; i < tokens.size(); i++) {
		bool is_quoted = tokens[i].type == SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT;
		if (!is_quoted && tokens[i].type == SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER &&
		    tokens[i].start < sql.size() && sql[tokens[i].start] == '"') {
			is_quoted = true;
		}
		if (is_quoted) {
			const idx_t start = tokens[i].start;
			const idx_t end = (i + 1 < tokens.size()) ? tokens[i + 1].start : sql.size();
			no_replace_ranges.push_back({start, end});
		}
	}

	string result;
	result.reserve(sql.size());
	idx_t pos = 0;
	idx_t range_idx = 0;

	while (pos < sql.size()) {
		while (range_idx < no_replace_ranges.size() && pos >= no_replace_ranges[range_idx].second) {
			range_idx++;
		}

		// If inside a quoted range, copy verbatim to its end
		if (range_idx < no_replace_ranges.size() && pos >= no_replace_ranges[range_idx].first) {
			idx_t end = no_replace_ranges[range_idx].second;
			result += sql.substr(pos, end - pos);
			pos = end;
			range_idx++;
			continue;
		}

		// If not inside a quoted range, check for a match of `from`
		if (sql.compare(pos, from.size(), from) == 0) {
			result += to;
			pos += from.size();
			continue;
		}

		// Otherwise, just copy the character at the current position
		result += sql[pos];
		pos++;
	}

	return result;
}

string DuckLakeUtil::OptionalIdxOrNull(const optional_idx &v) {
	return v.IsValid() ? std::to_string(v.GetIndex()) : "NULL";
}

string DuckLakeUtil::MappingIdOrNull(const MappingIndex &m) {
	return m.IsValid() ? std::to_string(m.index) : "NULL";
}

string DuckLakeUtil::EncryptionKeyLiteral(const string &key) {
	if (key.empty()) {
		return "NULL";
	}
	return "'" + Blob::ToBase64(string_t(key)) + "'";
}

const char *DuckLakeUtil::BoolLiteral(bool v) {
	return v ? "true" : "false";
}

string DuckLakeUtil::PartitionValueLiteral(const Value &v) {
	return v.IsNull() ? string("NULL") : SQLLiteralToString(v.ToString());
}

string DuckLakeUtil::ChunkRowToSQL(DuckLakeMetadataManager &metadata_manager, ClientContext &context, DataChunk &chunk,
                                   idx_t row) {
	string result;
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		if (c > 0) {
			result += ", ";
		}
		result += ValueToSQL(metadata_manager, context, chunk.GetValue(c, row));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Inlined VARIANT encoding
//===--------------------------------------------------------------------===//
bool DuckLakeUtil::ContainsVariant(const LogicalType &type) {
	return TypeVisitor::Contains(type, [](const LogicalType &t) { return t.id() == LogicalTypeId::VARIANT; });
}

LogicalType DuckLakeUtil::VariantToBlobType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARIANT:
		return LogicalType::BLOB;
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, VariantToBlobType(child.second));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(VariantToBlobType(ListType::GetChildType(type)));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(VariantToBlobType(MapType::KeyType(type)), VariantToBlobType(MapType::ValueType(type)));
	default:
		if (ContainsVariant(type)) {
			throw NotImplementedException("Cannot inline a VARIANT nested inside a %s column in this catalog type",
			                              type.ToString());
		}
		return type;
	}
}

string DuckLakeUtil::DecodeInlinedVariantExpression(const string &expr, const LogicalType &type, idx_t depth) {
	if (!ContainsVariant(type)) {
		return expr;
	}
	switch (type.id()) {
	case LogicalTypeId::VARIANT:
		return "variant_bytes_to_variant(" + expr + ")";
	case LogicalTypeId::STRUCT: {
		// rebuild the struct with the VARIANT fields decoded; a NULL struct has to stay NULL
		auto &children = StructType::GetChildTypes(type);
		bool unnamed = StructType::IsUnnamed(type);
		string fields;
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				fields += ", ";
			}
			auto &child = children[i];
			string child_expr = unnamed ? StringUtil::Format("struct_extract(%s, %d)", expr, i + 1)
			                            : StringUtil::Format("struct_extract(%s, %s)", expr,
			                                                 SQLLiteralToString(child.first.GetIdentifierName()));
			child_expr = DecodeInlinedVariantExpression(child_expr, child.second, depth);
			if (unnamed) {
				fields += child_expr;
			} else {
				fields += SQLLiteralToString(child.first.GetIdentifierName()) + ": " + child_expr;
			}
		}
		string rebuilt = unnamed ? "row(" + fields + ")" : "{" + fields + "}";
		return StringUtil::Format("CASE WHEN (%s) IS NULL THEN NULL ELSE %s END", expr, rebuilt);
	}
	case LogicalTypeId::LIST: {
		auto element = StringUtil::Format("__ducklake_variant_%d", depth);
		auto element_expr = DecodeInlinedVariantExpression(element, ListType::GetChildType(type), depth + 1);
		return StringUtil::Format("list_transform(%s, lambda %s: %s)", expr, element, element_expr);
	}
	case LogicalTypeId::MAP: {
		auto keys =
		    DecodeInlinedVariantExpression("map_keys(" + expr + ")", LogicalType::LIST(MapType::KeyType(type)), depth);
		auto values = DecodeInlinedVariantExpression("map_values(" + expr + ")",
		                                             LogicalType::LIST(MapType::ValueType(type)), depth);
		return StringUtil::Format("CASE WHEN (%s) IS NULL THEN NULL ELSE map(%s, %s) END", expr, keys, values);
	}
	default:
		throw NotImplementedException("Cannot read an inlined VARIANT nested inside a %s column in this catalog type",
		                              type.ToString());
	}
}

//===--------------------------------------------------------------------===//
// DuckLakeInlinedChunkEncoder
//===--------------------------------------------------------------------===//
static ScalarFunctionCatalogEntry &GetFunctionEntry(ClientContext &context, const string &name) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto system_transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(system_transaction, Identifier::DefaultSchema());
	auto entry = schema.GetEntry(system_transaction, CatalogType::SCALAR_FUNCTION_ENTRY, Identifier(name));
	if (!entry) {
		throw MissingExtensionException("Inlining VARIANT data requires the function %s, which is provided by the "
		                                "parquet extension. Try explicitly loading the parquet extension",
		                                name);
	}
	return entry->Cast<ScalarFunctionCatalogEntry>();
}

DuckLakeInlinedChunkEncoder::DuckLakeInlinedChunkEncoder(DuckLakeMetadataManager &metadata_manager,
                                                         ClientContext &context, const vector<LogicalType> &types) {
	if (metadata_manager.TypeIsNativelySupported(LogicalType::VARIANT())) {
		return;
	}
	vector<LogicalType> encoded_types;
	for (idx_t c = 0; c < types.size(); c++) {
		if (!DuckLakeUtil::ContainsVariant(types[c])) {
			encoded_types.push_back(types[c]);
			continue;
		}
		encoded_columns.push_back(c);
		encoded_types.push_back(DuckLakeUtil::VariantToBlobType(types[c]));
	}
	if (encoded_columns.empty()) {
		return;
	}
	// bind variant_to_parquet_variant(#0) - provided by the parquet extension. This runs at commit time, outside of
	// a client transaction, so look the function up through the system catalog directly.
	auto &function_entry = GetFunctionEntry(context, "variant_to_parquet_variant");
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::VARIANT(), 0));
	ErrorData error;
	FunctionBinder binder(context);
	auto function = binder.BindScalarFunction(function_entry, std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	vector<LogicalType> parquet_variant_types {function->GetReturnType()};
	expressions.push_back(std::move(function));
	executor = make_uniq<ExpressionExecutor>(context, expressions);
	variant_chunk.InitializeEmpty({LogicalType::VARIANT()});
	parquet_variant_chunk.Initialize(context, parquet_variant_types);
	encoded_chunk.Initialize(context, encoded_types);
}

DuckLakeInlinedChunkEncoder::~DuckLakeInlinedChunkEncoder() {
}

//! Concatenate the metadata and value blobs of each row into a single BLOB at result[result_offset + row]; rows that
//! are NULL stay NULL
static void ConcatenateParquetVariant(Vector &input, Vector &parquet_variant, idx_t count, Vector &result,
                                      idx_t result_offset) {
	UnifiedVectorFormat input_format;
	input.ToUnifiedFormat(input_format);

	parquet_variant.Flatten();
	auto &struct_validity = FlatVector::Validity(parquet_variant);
	auto &entries = StructVector::GetEntries(parquet_variant);
	D_ASSERT(entries.size() >= 2);
	UnifiedVectorFormat metadata_format;
	UnifiedVectorFormat value_format;
	entries[0].ToUnifiedFormat(metadata_format);
	entries[1].ToUnifiedFormat(value_format);
	auto metadata_data = UnifiedVectorFormat::GetData<string_t>(metadata_format);
	auto value_data = UnifiedVectorFormat::GetData<string_t>(value_format);

	auto result_data = FlatVector::GetDataMutable<string_t>(result);
	auto &result_validity = FlatVector::ValidityMutable(result);
	for (idx_t row = 0; row < count; row++) {
		auto result_idx = result_offset + row;
		auto input_idx = input_format.sel->get_index(row);
		auto metadata_idx = metadata_format.sel->get_index(row);
		auto value_idx = value_format.sel->get_index(row);
		if (!input_format.validity.RowIsValid(input_idx) || !struct_validity.RowIsValid(row) ||
		    !metadata_format.validity.RowIsValid(metadata_idx) || !value_format.validity.RowIsValid(value_idx)) {
			result_validity.SetInvalid(result_idx);
			continue;
		}
		auto &metadata = metadata_data[metadata_idx];
		auto &value = value_data[value_idx];
		auto target = StringVector::EmptyString(result, metadata.GetSize() + value.GetSize());
		auto target_data = target.GetDataWriteable();
		memcpy(target_data, metadata.GetData(), metadata.GetSize());
		memcpy(target_data + metadata.GetSize(), value.GetData(), value.GetSize());
		target.Finalize();
		result_data[result_idx] = target;
	}
}

void DuckLakeInlinedChunkEncoder::EncodeVariant(Vector &input, idx_t count, Vector &result) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	// nested VARIANT vectors (list children) can hold more rows than the executor handles at once - slice them
	for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
		auto batch = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - offset);
		Vector batch_input(input.GetType());
		batch_input.Slice(input, offset, offset + batch);
		variant_chunk.Reset();
		variant_chunk.data[0].Reference(batch_input);
		variant_chunk.SetChildCardinality(batch);
		parquet_variant_chunk.Reset();
		executor->Execute(variant_chunk, parquet_variant_chunk);
		ConcatenateParquetVariant(batch_input, parquet_variant_chunk.data[0], batch, result, offset);
	}
}

void DuckLakeInlinedChunkEncoder::EncodeVector(Vector &input, idx_t count, Vector &result) {
	auto &type = input.GetType();
	if (type.id() == LogicalTypeId::VARIANT) {
		EncodeVariant(input, count, result);
		return;
	}
	if (!DuckLakeUtil::ContainsVariant(type)) {
		result.Reference(input);
		return;
	}
	input.Flatten();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetValidity(result, FlatVector::Validity(input));
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		auto &input_entries = StructVector::GetEntries(input);
		auto &result_entries = StructVector::GetEntries(result);
		D_ASSERT(input_entries.size() == result_entries.size());
		for (idx_t i = 0; i < input_entries.size(); i++) {
			EncodeVector(input_entries[i], count, result_entries[i]);
		}
		break;
	}
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP: {
		// a MAP is physically a LIST of STRUCT(key, value)
		auto input_entries = FlatVector::GetData<list_entry_t>(input);
		auto result_entries = FlatVector::GetDataMutable<list_entry_t>(result);
		memcpy(result_entries, input_entries, count * sizeof(list_entry_t));
		auto list_size = ListVector::GetListSize(input);
		ListVector::Reserve(result, list_size);
		EncodeVector(ListVector::GetChildMutable(input), list_size, ListVector::GetChildMutable(result));
		ListVector::SetListSize(result, list_size);
		break;
	}
	default:
		throw InternalException("DuckLakeInlinedChunkEncoder: unsupported nested type %s", type.ToString());
	}
}

DataChunk &DuckLakeInlinedChunkEncoder::Encode(DataChunk &chunk) {
	if (!executor) {
		return chunk;
	}
	auto count = chunk.size();
	encoded_chunk.Reset();
	idx_t encoded_idx = 0;
	for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
		if (encoded_idx < encoded_columns.size() && encoded_columns[encoded_idx] == c) {
			EncodeVector(chunk.data[c], count, encoded_chunk.data[c]);
			encoded_idx++;
		} else {
			encoded_chunk.data[c].Reference(chunk.data[c]);
		}
	}
	encoded_chunk.SetChildCardinality(count);
	return encoded_chunk;
}

void DuckLakeUtil::CopyExtensionSettings(ClientContext &from, ClientContext &to) {
	auto &db_config = DBConfig::GetConfig(from);
	for (auto &entry : db_config.GetExtensionSettings()) {
		auto &option = entry.second;
		if (!option.setting_index.IsValid()) {
			continue;
		}
		auto setting_index = option.setting_index.GetIndex();
		if (!from.config.user_settings.IsSet(setting_index)) {
			continue;
		}
		Value value;
		if (!from.TryGetCurrentSetting(entry.first, value)) {
			continue;
		}
		to.config.user_settings.SetUserSetting(setting_index, value);
	}
}

bool DuckLakeUtil::TryGetLiteralValue(const ParsedExpression &expr, Value &result) {
	if (expr.GetExpressionType() == ExpressionType::VALUE_CONSTANT) {
		result = expr.Cast<ConstantExpression>().GetLiteral().ToValue();
		return true;
	}
	if (expr.GetExpressionType() != ExpressionType::OPERATOR_CAST) {
		return false;
	}
	auto &cast = expr.Cast<CastExpression>();
	if (cast.IsTryCast() || cast.Child().GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
		return false;
	}
	auto target_type = UnboundType::TryDefaultBind(cast.TargetType());
	if (target_type.id() == LogicalTypeId::INVALID || target_type.id() == LogicalTypeId::UNBOUND) {
		return false;
	}
	auto value = cast.Child().Cast<ConstantExpression>().GetLiteral().ToValue().DefaultTryCastAs(target_type);
	if (!value) {
		return false;
	}
	result = std::move(*value);
	return true;
}

} // namespace duckdb
