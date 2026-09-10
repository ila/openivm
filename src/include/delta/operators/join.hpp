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
	bool is_right_of_left_join; // true if this leaf is on the nullable side of an outer join
};

struct Scd2RangeJoinInfo {
	size_t dimension_leaf;
	size_t probe_leaf;
	ColumnBinding effective_binding;
	ColumnBinding end_binding;
	ColumnBinding probe_binding;
	bool upper_inclusive;
};

LogicalGet *FindGetInSubtree(LogicalOperator *node);

unique_ptr<LogicalOperator> &GetNodeAtPath(unique_ptr<LogicalOperator> &root, const vector<size_t> &path);

void DemoteLeftJoins(LogicalOperator *node);

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf,
                               const ColumnBinding &mul_binding);

void ApplyScd2RangeFilters(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &term,
                           const vector<JoinLeafInfo> &leaves, const vector<Scd2RangeJoinInfo> &ranges,
                           uint64_t delta_mask, const unordered_map<idx_t, idx_t> &table_mapping);

void AppendMultiplicityToAncestorProjectionMaps(unique_ptr<LogicalOperator> &term, const vector<size_t> &leaf_path,
                                                const ColumnBinding &mul_binding, const char *context_label,
                                                bool preserve_constant_sibling_child_outputs = false,
                                                idx_t fallback_mul_idx = DConstants::INVALID_INDEX);

unique_ptr<LogicalOperator> AssembleJoinUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                 const vector<LogicalType> &types, Binder &binder);

ColumnBinding ReplaceJoinOutputBindings(const vector<ColumnBinding> &original_bindings,
                                        unique_ptr<LogicalOperator> &result, LogicalOperator &root);

} // namespace duckdb

#endif // OPENIVM_DELTA_JOIN_HPP
