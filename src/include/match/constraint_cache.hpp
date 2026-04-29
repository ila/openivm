// Constraint cache for view matching.
//
// Lazy cache of `TableCatalogEntry::GetConstraints()` per table, persisted in
// `_duckdb_ivm_constraints_cache`. Adds RELY-style trusted-but-not-enforced
// FK declarations (Oracle/DB2 pattern) via `PRAGMA ivm_declare_rely_fk(...)`,
// used by the matcher for join elimination.

#ifndef OPENIVM_MATCH_CONSTRAINT_CACHE_HPP
#define OPENIVM_MATCH_CONSTRAINT_CACHE_HPP

#include "duckdb.hpp"

#include <mutex>
#include <unordered_map>

namespace duckdb {
namespace ivm {

struct CachedConstraint {
	string table_name;
	string kind; // 'PK' | 'UNIQUE' | 'FK' | 'NOT_NULL' | 'RELY_FK' | 'CHECK'
	vector<string> columns;
	string referenced_table;           // FK / RELY_FK only
	vector<string> referenced_columns; // FK / RELY_FK only
	bool is_trusted = true;            // false only for un-enforced
};

class ConstraintCache {
public:
	ConstraintCache() = default;

	vector<CachedConstraint> GetConstraints(const string &table_name);

	bool IsUniqueKey(const string &table_name, const vector<string> &columns);

	// Returns true iff a (RELY_)FK from child→parent matching the given
	// columns exists in the cache.
	bool HasFKToParent(const string &child_table, const vector<string> &child_columns, const string &parent_table,
	                   const vector<string> &parent_columns);

	void DeclareRelyFK(const CachedConstraint &c);

	void InvalidateTable(const string &table_name);

private:
	std::mutex cache_mutex_;
	std::unordered_map<string, vector<CachedConstraint>> cache_;
};

} // namespace ivm
} // namespace duckdb

#endif
