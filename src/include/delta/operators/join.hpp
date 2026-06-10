#ifndef OPENIVM_DELTA_JOIN_HPP
#define OPENIVM_DELTA_JOIN_HPP

#include "delta/delta_helpers.hpp"
#include "delta/delta_operator.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

struct JoinLeafInfo {
	vector<size_t> path;
	LogicalGet *get;            // non-null for simple table scans
	LogicalOperator *node;      // always set; for non-GET leaves, rewrite the subtree
	bool is_right_of_left_join; // true if this leaf is on the RIGHT side of a LEFT JOIN
};

void CollectJoinLeaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves,
                       bool is_right_of_left = false);

LogicalGet *FindGetInSubtree(LogicalOperator *node);

unique_ptr<LogicalOperator> &GetNodeAtPath(unique_ptr<LogicalOperator> &root, const vector<size_t> &path);

void DemoteLeftJoins(LogicalOperator *node);

// Append the multiplicity column to the parent join's projection map for the leaf at `path`, locating
// it by searching the delta-replaced child's bindings for `mul_binding` (arity can change vs the
// original leaf). include_delim_parents=false matches only plain comparison joins (the
// inclusion-exclusion path); =true also matches delim/dependent joins (the delim-join path).
void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const vector<size_t> &path,
                               const ColumnBinding &mul_binding, bool include_delim_parents);

unique_ptr<LogicalOperator> AssembleJoinUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                 const vector<LogicalType> &types, Binder &binder);

ColumnBinding ReplaceJoinOutputBindings(const vector<ColumnBinding> &original_bindings,
                                        unique_ptr<LogicalOperator> &result, LogicalOperator &root);

// Build the combined multiplicity expression for one inclusion-exclusion term:
//   (-1)^(k-1) * ∏ wᵢ   where k = mul_bindings.size()
// i.e. the Z-set bilinear product of the per-leaf multiplicities times the Möbius
// inclusion-exclusion sign (flip only when k is even). Shared by the 2^N-1 join rule
// and the delim-join rule. See the call site in CompileJoinDelta for why the sign is
// required by OpenIVM's "current base = R_old + ΔR" data layout.
unique_ptr<Expression> BuildMultiplicityProduct(Binder &binder, const LogicalType &mul_type,
                                                const vector<ColumnBinding> &mul_bindings);

} // namespace duckdb

#endif // OPENIVM_DELTA_JOIN_HPP
