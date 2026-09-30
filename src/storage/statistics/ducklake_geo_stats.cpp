#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_geo_stats.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/json_document.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

DuckLakeColumnGeoStats::DuckLakeColumnGeoStats() : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::GEOMETRY) {
	xmin = NumericLimits<double>::Maximum();
	xmax = NumericLimits<double>::Minimum();
	ymin = NumericLimits<double>::Maximum();
	ymax = NumericLimits<double>::Minimum();
	zmin = NumericLimits<double>::Maximum();
	zmax = NumericLimits<double>::Minimum();
	mmin = NumericLimits<double>::Maximum();
	mmax = NumericLimits<double>::Minimum();
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnGeoStats::Copy() const {
	return make_uniq<DuckLakeColumnGeoStats>(*this);
}

void DuckLakeColumnGeoStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &geo_stats = new_stats.Cast<DuckLakeColumnGeoStats>();

	xmin = MinValue(xmin, geo_stats.xmin);
	xmax = MaxValue(xmax, geo_stats.xmax);
	ymin = MinValue(ymin, geo_stats.ymin);
	ymax = MaxValue(ymax, geo_stats.ymax);
	zmin = MinValue(zmin, geo_stats.zmin);
	zmax = MaxValue(zmax, geo_stats.zmax);
	mmin = MinValue(mmin, geo_stats.mmin);
	mmax = MaxValue(mmax, geo_stats.mmax);

	geo_types.insert(geo_stats.geo_types.begin(), geo_stats.geo_types.end());
}

bool DuckLakeColumnGeoStats::TrySerialize(string &result) const {
	// Format as JSON
	auto xmin_val = xmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(xmin);
	auto xmax_val = xmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(xmax);
	auto ymin_val = ymin == NumericLimits<double>::Maximum() ? "null" : std::to_string(ymin);
	auto ymax_val = ymax == NumericLimits<double>::Minimum() ? "null" : std::to_string(ymax);
	auto zmin_val = zmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(zmin);
	auto zmax_val = zmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(zmax);
	auto mmin_val = mmin == NumericLimits<double>::Maximum() ? "null" : std::to_string(mmin);
	auto mmax_val = mmax == NumericLimits<double>::Minimum() ? "null" : std::to_string(mmax);

	auto bbox = StringUtil::Format(
	    R"({"xmin": %s, "xmax": %s, "ymin": %s, "ymax": %s, "zmin": %s, "zmax": %s, "mmin": %s, "mmax": %s})", xmin_val,
	    xmax_val, ymin_val, ymax_val, zmin_val, zmax_val, mmin_val, mmax_val);

	string types = "[";
	for (auto &type : geo_types) {
		if (types.size() > 1) {
			types += ", ";
		}
		types += StringUtil::Format("\"%s\"", type);
	}
	types += "]";

	result = StringUtil::Format(R"('{"bbox": %s, "types": %s}')", bbox, types);
	return true;
}

void DuckLakeColumnGeoStats::Serialize(DuckLakeColumnStatsInfo &column_stats) const {
	TrySerialize(column_stats.extra_stats);
}

void DuckLakeColumnGeoStats::Deserialize(const string &stats) {
	JSONParseError error;
	auto doc = JSONDocument::TryParse(stats.c_str(), stats.size(), error);
	if (!doc) {
		throw InvalidInputException("Failed to parse geo stats JSON");
	}
	auto root = doc->GetRoot();
	if (!root.IsObject()) {
		throw InvalidInputException("Invalid geo stats JSON");
	}

	auto bbox = root.GetMember("bbox");
	const pair<const char *, double *> bounds[] = {{"xmin", &xmin}, {"xmax", &xmax}, {"ymin", &ymin}, {"ymax", &ymax},
	                                               {"zmin", &zmin}, {"zmax", &zmax}, {"mmin", &mmin}, {"mmax", &mmax}};
	for (auto &bound : bounds) {
		auto bound_val = bbox.GetMember(bound.first);
		if (bound_val.IsNumber()) {
			*bound.second = bound_val.GetNumber();
		}
	}

	root.GetMember("types").IterateArray([&](JSONValue type_val) {
		if (type_val.IsString()) {
			geo_types.insert(type_val.GetString());
		}
	});
}

bool DuckLakeColumnGeoStats::ParseStats(const string &stats_name, const vector<Value> &stats_children) {
	if (stats_name == "bbox_xmax") {
		xmax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_xmin") {
		xmin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_ymax") {
		ymax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_ymin") {
		ymin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_zmax") {
		zmax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_zmin") {
		zmin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_mmax") {
		mmax = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "bbox_mmin") {
		mmin = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
	} else if (stats_name == "geo_types") {
		auto list_value = stats_children[1].DefaultCastAs(LogicalType::LIST(LogicalType::VARCHAR));
		for (const auto &child : ListValue::GetChildren(list_value)) {
			geo_types.insert(StringValue::Get(child));
		}
	} else {
		return false;
	}
	return true;
}

unique_ptr<BaseStatistics> DuckLakeColumnGeoStats::ToStats() const {
	auto stats = GeometryStats::CreateEmpty(LogicalType::GEOMETRY());

	auto &extent = GeometryStats::GetExtent(stats);
	extent.x_min = xmin;
	extent.x_max = xmax;
	extent.y_min = ymin;
	extent.y_max = ymax;
	extent.z_min = zmin;
	extent.z_max = zmax;
	extent.m_min = mmin;
	extent.m_max = mmax;

	static const case_insensitive_map_t<pair<GeometryType, VertexType>> type_mapping = {
	    // XY TYPES
	    {"unknown", {GeometryType::INVALID, VertexType::XY}},
	    {"point", {GeometryType::POINT, VertexType::XY}},
	    {"linestring", {GeometryType::LINESTRING, VertexType::XY}},
	    {"polygon", {GeometryType::POLYGON, VertexType::XY}},
	    {"multipoint", {GeometryType::MULTIPOINT, VertexType::XY}},
	    {"multilinestring", {GeometryType::MULTILINESTRING, VertexType::XY}},
	    {"multipolygon", {GeometryType::MULTIPOLYGON, VertexType::XY}},
	    {"geometrycollection", {GeometryType::GEOMETRYCOLLECTION, VertexType::XY}},
	    // Z TYPES
	    {"unknown_z", {GeometryType::INVALID, VertexType::XYZ}},
	    {"point_z", {GeometryType::POINT, VertexType::XYZ}},
	    {"linestring_z", {GeometryType::LINESTRING, VertexType::XYZ}},
	    {"polygon_z", {GeometryType::POLYGON, VertexType::XYZ}},
	    {"multipoint_z", {GeometryType::MULTIPOINT, VertexType::XYZ}},
	    {"multilinestring_z", {GeometryType::MULTILINESTRING, VertexType::XYZ}},
	    {"multipolygon_z", {GeometryType::MULTIPOLYGON, VertexType::XYZ}},
	    {"geometrycollection_z", {GeometryType::GEOMETRYCOLLECTION, VertexType::XYZ}},
	    // M TYPES
	    {"unknown_m", {GeometryType::INVALID, VertexType::XYM}},
	    {"point_m", {GeometryType::POINT, VertexType::XYM}},
	    {"linestring_m", {GeometryType::LINESTRING, VertexType::XYM}},
	    {"polygon_m", {GeometryType::POLYGON, VertexType::XYM}},
	    {"multipoint_m", {GeometryType::MULTIPOINT, VertexType::XYM}},
	    {"multilinestring_m", {GeometryType::MULTILINESTRING, VertexType::XYM}},
	    {"multipolygon_m", {GeometryType::MULTIPOLYGON, VertexType::XYM}},
	    {"geometrycollection_m", {GeometryType::GEOMETRYCOLLECTION, VertexType::XYM}},
	    // ZM TYPES
	    {"unknown_zm", {GeometryType::INVALID, VertexType::XYZM}},
	    {"point_zm", {GeometryType::POINT, VertexType::XYZM}},
	    {"linestring_zm", {GeometryType::LINESTRING, VertexType::XYZM}},
	    {"polygon_zm", {GeometryType::POLYGON, VertexType::XYZM}},
	    {"multipoint_zm", {GeometryType::MULTIPOINT, VertexType::XYZM}},
	    {"multilinestring_zm", {GeometryType::MULTILINESTRING, VertexType::XYZM}},
	    {"multipolygon_zm", {GeometryType::MULTIPOLYGON, VertexType::XYZM}},
	    {"geometrycollection_zm", {GeometryType::GEOMETRYCOLLECTION, VertexType::XYZM}}};

	auto &types = GeometryStats::GetTypes(stats);
	for (auto &type : geo_types) {
		auto entry = type_mapping.find(type);
		if (entry != type_mapping.end()) {
			types.Add(entry->second.first, entry->second.second);
		}
	}

	// DuckLake doesn't store flags for empty/non-empty geometry/parts, so assume the worst.
	auto &flags = GeometryStats::GetFlags(stats);
	flags.SetHasEmptyGeometry();
	flags.SetHasEmptyPart();
	flags.SetHasNonEmptyGeometry();
	flags.SetHasNonEmptyPart();

	return stats.ToUnique();
}

} // namespace duckdb
