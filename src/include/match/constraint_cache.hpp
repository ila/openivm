// Constraint cache for view matching.
//
// Reads qualified, trusted constraint declarations persisted in
// `openivm_constraints_cache`. Adds RELY-style trusted-but-not-enforced
// FK declarations (Oracle/DB2 pattern) via `PRAGMA openivm_declare_rely_fk(...)`,
// used by the matcher for join elimination.

#ifndef OPENIVM_MATCH_CONSTRAINT_CACHE_HPP
#define OPENIVM_MATCH_CONSTRAINT_CACHE_HPP

#include "duckdb.hpp"

namespace duckdb {
namespace openivm {

struct CachedConstraint {
	string table_name;
	string kind; // 'PK' | 'UNIQUE' | 'FK' | 'NOT_NULL' | 'RELY_FK' | 'CHECK'
	vector<string> columns;
	string referenced_table;           // FK / RELY_FK only
	vector<string> referenced_columns; // FK / RELY_FK only
	bool is_trusted = true;            // false only for un-enforced
};

vector<CachedConstraint> ReadConstraintDeclarations(ClientContext &context, const case_insensitive_set_t &sources);
TableFunction DeclareRelyFKFunction();

} // namespace openivm
} // namespace duckdb

#endif
