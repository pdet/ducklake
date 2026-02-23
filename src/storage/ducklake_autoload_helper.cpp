#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"

#include "storage/ducklake_initializer.hpp"

namespace duckdb {

static string LookupExtensionForPattern(const string &pattern) {
	for (const auto &entry : EXTENSION_FILE_PREFIXES) {
		if (StringUtil::StartsWith(pattern, entry.name)) {
			return entry.extension;
		}
	}
	return "";
}

void DuckLakeInitializer::CheckAndAutoloadedRequiredExtension(const string &pattern) {
	if (pattern.empty()) {
		return;
	}
	const string required_extension = LookupExtensionForPattern(pattern);
	if (!required_extension.empty() && !ExtensionHelper::TryAutoLoadExtension(*context.db, required_extension)) {
		auto error_message =
		    "Ducklake data path " + pattern + " requires the extension " + required_extension + " to be loaded";
		error_message = ExtensionHelper::AddExtensionInstallHintToErrorMsg(context, error_message, required_extension);
		throw MissingExtensionException(error_message);
	}
}

} // namespace duckdb
