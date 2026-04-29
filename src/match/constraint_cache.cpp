#include "match/constraint_cache.hpp"

namespace duckdb {
namespace ivm {

vector<CachedConstraint> ConstraintCache::GetConstraints(const string &table_name) {
	std::lock_guard<std::mutex> lock(cache_mutex_);
	auto it = cache_.find(table_name);
	if (it != cache_.end()) {
		return it->second;
	}
	// TODO: query DuckDB catalog (TableCatalogEntry::GetConstraints) and
	// `_duckdb_ivm_constraints_cache` to populate.
	return {};
}

bool ConstraintCache::IsUniqueKey(const string &table_name, const vector<string> &columns) {
	auto cs = GetConstraints(table_name);
	for (const auto &c : cs) {
		if ((c.kind == "PK" || c.kind == "UNIQUE") && c.columns == columns) {
			return true;
		}
	}
	return false;
}

bool ConstraintCache::HasFKToParent(const string &child_table, const vector<string> &child_columns,
                                    const string &parent_table, const vector<string> &parent_columns) {
	auto cs = GetConstraints(child_table);
	for (const auto &c : cs) {
		if ((c.kind == "FK" || c.kind == "RELY_FK") && c.columns == child_columns &&
		    c.referenced_table == parent_table && c.referenced_columns == parent_columns) {
			return true;
		}
	}
	return false;
}

void ConstraintCache::DeclareRelyFK(const CachedConstraint &c) {
	std::lock_guard<std::mutex> lock(cache_mutex_);
	cache_[c.table_name].push_back(c);
	// TODO: persist into _duckdb_ivm_constraints_cache.
}

void ConstraintCache::InvalidateTable(const string &table_name) {
	std::lock_guard<std::mutex> lock(cache_mutex_);
	cache_.erase(table_name);
}

} // namespace ivm
} // namespace duckdb
