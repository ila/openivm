#ifndef IVM_COLUMN_HIDER_HPP
#define IVM_COLUMN_HIDER_HPP

#include "duckdb/common/string_util.hpp"

#include <string>
#include <vector>

namespace duckdb {

/// Naming conventions for IVM internal vs user-facing tables.
///
/// The MV data lives in `_ivm_data_<name>` (physical table with all columns
/// including _ivm_left_key, _ivm_distinct_count, etc.). The user sees `<name>`
/// (a VIEW that excludes internal columns via SELECT * EXCLUDE).
///
/// All IVM-internal operations (upsert, delta, rewrite) use the data table.
/// The view is created/dropped alongside the data table by the parser.
struct IVMTableNames {
	/// Returns the internal data table name for a given user-facing MV name.
	static std::string DataTableName(const std::string &view_name) {
		return "_ivm_data_" + view_name;
	}

	/// Returns true if a column name is an internal IVM column that should be
	/// hidden from users (excluded from the view).
	static bool IsInternalColumn(const std::string &name) {
		return StringUtil::StartsWith(name, "_ivm_");
	}
};

} // namespace duckdb

#endif // IVM_COLUMN_HIDER_HPP
