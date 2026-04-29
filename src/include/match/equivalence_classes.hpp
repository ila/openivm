// Equivalence classes for view matching.
//
// Builds union-find groups over `ColumnBinding` from `=`-predicates. Used to
// rewrite column references against a canonical representative and to check
// compatibility of query vs view ECs (Goldstein-Larson filter-tree level 5).
//
// All consumers gated by `ivm_enable_view_matching`.

#ifndef OPENIVM_MATCH_EQUIVALENCE_CLASSES_HPP
#define OPENIVM_MATCH_EQUIVALENCE_CLASSES_HPP

#include "duckdb.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {
namespace ivm {

class EquivalenceClassMap {
public:
	EquivalenceClassMap() = default;

	// Build from a list of equality predicates. Each predicate must resolve
	// either to (col = col) or (col = literal). Other shapes are ignored.
	static EquivalenceClassMap FromEqualities(const vector<reference<Expression>> &equalities);

	optional_idx FindClass(const ColumnBinding &binding) const;

	// True iff every class in `*this` is a subset of (or equal to) some class
	// in `other`, after constant-attachment compatibility.
	bool IsCompatibleWith(const EquivalenceClassMap &other) const;

	// Constant attached to a class (e.g., col = 5 → class containing col gets
	// the value 5). Returns nullptr if no constant.
	optional_ptr<const Value> GetConstantForClass(idx_t class_id) const;

	idx_t ClassCount() const {
		return classes_.size();
	}

	const vector<ColumnBinding> &GetClassMembers(idx_t class_id) const {
		return classes_[class_id];
	}

private:
	column_binding_map_t<idx_t> class_id_;
	vector<vector<ColumnBinding>> classes_;
	vector<unique_ptr<Value>> constants_;
};

} // namespace ivm
} // namespace duckdb

#endif
