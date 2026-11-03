#ifndef OPENIVM_DELTA_JOIN_HPP
#define OPENIVM_DELTA_JOIN_HPP

#include "delta/delta_helpers.hpp"
#include "delta/delta_operator.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

struct JoinLeafInfo {
	vector<size_t> path;
	LogicalGet *get;            // non-null for simple table scans
	LogicalOperator *node;      // always set; for non-GET leaves, rewrite the subtree
	bool is_right_of_left_join; // true if this leaf is on the nullable side of an outer join
};

struct DeltaJoinKeyProbe {
	size_t other_leaf;
	string delta_column;
	string other_column;
};

// The scan at the bottom of node's unary chain, or nullptr when the chain branches first. When chain is given, the
// operators above the scan are appended to it, outermost first.
LogicalGet *FindGetInSubtree(LogicalOperator *node, vector<LogicalOperator *> *chain = nullptr);
// The leaf's own scan, or the scan at the bottom of its unary chain.
LogicalGet *GetLeafScan(const JoinLeafInfo &leaf);

// The delta of one join leaf inside a term: a delta scan for a bare GET, otherwise the compiled copied subtree.
DeltaPlanFragment CompileRegularLeafDelta(const DeltaOperatorInput &input, ClientContext &context, Binder &binder,
                                          unique_ptr<LogicalOperator> &leaf_node, LogicalOperator *&term_root,
                                          DeltaSourceCache &source_cache);

// For each leaf, the INNER-join equalities between one of its scan columns and another leaf's scan column. Leaves
// wrapped above their scan are traced only when trace_wrapped_leaves is set.
vector<vector<DeltaJoinKeyProbe>> BuildJoinKeyProbes(LogicalOperator *root, const vector<JoinLeafInfo> &leaves,
                                                     bool trace_wrapped_leaves);

// SELECT EXISTS over relation_sql. A failed or NULL probe answers true, so callers keep the term (fail open).
bool DeltaJoinProbeExists(Connection &con, const string &relation_sql, const char *label);

unique_ptr<LogicalOperator> &GetNodeAtPath(unique_ptr<LogicalOperator> &root, const vector<size_t> &path);

void DemoteLeftJoins(LogicalOperator *node);

// Demote each outer join to INNER when its NULL-supplying side contains a leaf whose bit is set in mask.
void DemoteOuterJoinsForMask(LogicalOperator *node, const vector<JoinLeafInfo> &leaves, uint64_t mask);

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf,
                               const ColumnBinding &mul_binding, idx_t original_leaf_width);

void ShiftAncestorProjectionMap(const vector<LogicalOperator *> &ancestors, const vector<size_t> &leaf_path,
                                size_t depth, idx_t insertion_idx, idx_t added);

void AppendMultiplicityToAncestorProjectionMaps(unique_ptr<LogicalOperator> &term, const vector<size_t> &leaf_path,
                                                const ColumnBinding &mul_binding, const char *context_label,
                                                bool preserve_constant_sibling_child_outputs = false,
                                                idx_t fallback_mul_idx = DConstants::INVALID_INDEX,
                                                idx_t original_leaf_width = DConstants::INVALID_INDEX);

// lhs * rhs, bound as DuckDB's multiplication operator.
unique_ptr<Expression> MultiplyIntegers(Binder &binder, unique_ptr<Expression> lhs, unique_ptr<Expression> rhs);

// The product of a term's leaf multiplicities, optionally times the Möbius sign (-1)^(k-1).
unique_ptr<Expression> BuildJoinMultiplicityProduct(Binder &binder, const LogicalType &mul_type,
                                                    const vector<ColumnBinding> &mul_bindings, bool mobius_sign);

// Project a join term to its outputs, minus `hidden` and the leaf multiplicities, plus the combined multiplicity.
unique_ptr<LogicalOperator> FinalizeJoinTerm(Binder &binder, unique_ptr<LogicalOperator> term,
                                             column_binding_set_t hidden, const vector<ColumnBinding> &mul_bindings,
                                             const LogicalType &mul_type, bool mobius_sign);

unique_ptr<LogicalOperator> AssembleJoinUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                 const vector<LogicalType> &types, Binder &binder);

ColumnBinding ReplaceJoinOutputBindings(const vector<ColumnBinding> &original_bindings,
                                        unique_ptr<LogicalOperator> &result, LogicalOperator &root);

} // namespace duckdb

#endif // OPENIVM_DELTA_JOIN_HPP
