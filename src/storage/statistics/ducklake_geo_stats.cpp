#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_geo_stats.hpp"

#include "duckdb/common/types/value.hpp"
#include "duckdb/common/json_document.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "duckdb/storage/statistics/geometry_stats.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"

namespace duckdb {

DuckLakeColumnGeoStats::DuckLakeColumnGeoStats() : DuckLakeColumnExtraStats(DuckLakeExtraStatsType::GEOMETRY) {
}

unique_ptr<DuckLakeColumnExtraStats> DuckLakeColumnGeoStats::Copy() const {
	return make_uniq<DuckLakeColumnGeoStats>(*this);
}

void DuckLakeColumnGeoStats::Merge(const DuckLakeColumnExtraStats &new_stats) {
	auto &geo_stats = new_stats.Cast<DuckLakeColumnGeoStats>();

	extent.Merge(geo_stats.extent);
	geo_types.insert(geo_stats.geo_types.begin(), geo_stats.geo_types.end());
}

struct GeoStatsBound {
	const char *name;
	double GeometryExtent::*field;
	double empty_value;
};

static constexpr GeoStatsBound GEO_STATS_BOUNDS[] = {{"xmin", &GeometryExtent::x_min, GeometryExtent::EMPTY_MIN},
                                                     {"xmax", &GeometryExtent::x_max, GeometryExtent::EMPTY_MAX},
                                                     {"ymin", &GeometryExtent::y_min, GeometryExtent::EMPTY_MIN},
                                                     {"ymax", &GeometryExtent::y_max, GeometryExtent::EMPTY_MAX},
                                                     {"zmin", &GeometryExtent::z_min, GeometryExtent::EMPTY_MIN},
                                                     {"zmax", &GeometryExtent::z_max, GeometryExtent::EMPTY_MAX},
                                                     {"mmin", &GeometryExtent::m_min, GeometryExtent::EMPTY_MIN},
                                                     {"mmax", &GeometryExtent::m_max, GeometryExtent::EMPTY_MAX}};

//! Stands for an unknown bound, since JSON has no infinite numbers
static constexpr double WIDEST_FINITE_BOUND = std::numeric_limits<double>::max();

bool DuckLakeColumnGeoStats::TrySerialize(string &result) const {
	JSONWriter writer;
	auto bbox = writer.CreateObject();
	for (auto &bound : GEO_STATS_BOUNDS) {
		auto value = extent.*bound.field;
		if (value == bound.empty_value) {
			bbox.Add(bound.name, writer.CreateNull());
		} else if (std::isfinite(value)) {
			bbox.Add(bound.name, writer.CreateDouble(value));
		} else {
			bbox.Add(bound.name, writer.CreateDouble(std::copysign(WIDEST_FINITE_BOUND, -bound.empty_value)));
		}
	}
	auto types = writer.CreateArray();
	for (auto &type : geo_types) {
		types.AppendString(type);
	}
	auto root = writer.CreateObject();
	root.Add("bbox", bbox);
	root.Add("types", types);
	writer.SetRoot(root);
	result = SQLString::ToString(writer.ToString());
	return true;
}

void DuckLakeColumnGeoStats::Serialize(DuckLakeColumnStatsInfo &column_stats) const {
	TrySerialize(column_stats.extra_stats);
}

void DuckLakeColumnGeoStats::Deserialize(const string &stats) {
	JSONParseError error;
	auto doc = JSONDocument::TryParse(stats.c_str(), stats.size(), error, JSONReadFlags::ALLOW_INF_AND_NAN);
	if (!doc) {
		throw InvalidInputException("Failed to parse geo stats JSON");
	}
	auto root = doc->GetRoot();
	if (!root.IsObject()) {
		throw InvalidInputException("Invalid geo stats JSON");
	}

	auto bbox = root.GetMember("bbox");
	for (auto &bound : GEO_STATS_BOUNDS) {
		auto bound_val = bbox.GetMember(bound.name);
		if (bound_val.IsNumber()) {
			extent.*bound.field = bound_val.GetNumber();
		}
	}

	root.GetMember("types").IterateArray([&](JSONValue type_val) {
		if (type_val.IsString()) {
			geo_types.insert(type_val.GetString());
		}
	});
}

bool DuckLakeColumnGeoStats::ParseStats(const string &stats_name, const vector<Value> &stats_children) {
	if (stats_name == "geo_types") {
		auto list_value = stats_children[1].DefaultCastAs(LogicalType::LIST(LogicalType::VARCHAR));
		for (const auto &child : ListValue::GetChildren(list_value)) {
			geo_types.insert(StringValue::Get(child));
		}
		return true;
	}
	for (auto &bound : GEO_STATS_BOUNDS) {
		if (stats_name == string("bbox_") + bound.name) {
			extent.*bound.field = stats_children[1].DefaultCastAs(LogicalType::DOUBLE).GetValue<double>();
			return true;
		}
	}
	return false;
}

static GeometryTypeSet ToTypeSet(const set<string> &geo_types) {
	auto types = GeometryTypeSet::Empty();
	for (auto &type : geo_types) {
		types.TryAdd(type);
	}
	return types;
}

static void SetBoundsUnknownIfEmpty(double &min, double &max) {
	if (min == GeometryExtent::EMPTY_MIN && max == GeometryExtent::EMPTY_MAX) {
		min = -WIDEST_FINITE_BOUND;
		max = WIDEST_FINITE_BOUND;
	}
}

void DuckLakeColumnGeoStats::SetOmittedBoundsUnknown() {
	auto types = ToTypeSet(geo_types);
	if (types.IsEmpty()) {
		return;
	}
	SetBoundsUnknownIfEmpty(extent.x_min, extent.x_max);
	SetBoundsUnknownIfEmpty(extent.y_min, extent.y_max);
	if (types.Has(VertexType::XYZ) || types.Has(VertexType::XYZM)) {
		SetBoundsUnknownIfEmpty(extent.z_min, extent.z_max);
	}
	if (types.Has(VertexType::XYM) || types.Has(VertexType::XYZM)) {
		SetBoundsUnknownIfEmpty(extent.m_min, extent.m_max);
	}
}

unique_ptr<BaseStatistics> DuckLakeColumnGeoStats::ToStats() const {
	auto stats = GeometryStats::CreateEmpty(LogicalType::GEOMETRY());

	auto &stats_extent = GeometryStats::GetExtent(stats);
	stats_extent = extent;
	for (auto &bound : GEO_STATS_BOUNDS) {
		// the largest finite bounds of an unknown extent also cover infinite coordinates
		auto &value = stats_extent.*bound.field;
		if (std::fabs(value) == WIDEST_FINITE_BOUND) {
			value = -bound.empty_value;
		}
	}

	GeometryStats::GetTypes(stats) = ToTypeSet(geo_types);

	// DuckLake doesn't store flags for empty/non-empty geometry/parts, so assume the worst.
	auto &flags = GeometryStats::GetFlags(stats);
	flags.SetHasEmptyGeometry();
	flags.SetHasEmptyPart();
	flags.SetHasNonEmptyGeometry();
	flags.SetHasNonEmptyPart();

	return stats.ToUnique();
}

} // namespace duckdb
