#include "core/ivm_view_classifier.hpp"

#include "rules/column_hider.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

namespace {

static void AddStrategyReason(vector<DeltaStrategyReason> &strategy_reasons, DeltaStrategyReason reason) {
	for (auto existing : strategy_reasons) {
		if (existing == reason) {
			return;
		}
	}
	strategy_reasons.push_back(reason);
}

static LogicalProjection *FindFirstProjection(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		return &op->Cast<LogicalProjection>();
	}
	for (auto &child : op->children) {
		auto *projection = FindFirstProjection(child.get());
		if (projection) {
			return projection;
		}
	}
	return nullptr;
}

static LogicalComparisonJoin *FindFirstComparisonJoin(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return &op->Cast<LogicalComparisonJoin>();
	}
	for (auto &child : op->children) {
		auto *join = FindFirstComparisonJoin(child.get());
		if (join) {
			return join;
		}
	}
	return nullptr;
}

static void AddJoinKeyColumn(const unique_ptr<Expression> &expr,
                             unordered_map<idx_t, unordered_set<idx_t>> &join_key_cols) {
	if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return;
	}
	auto &bcr = expr->Cast<BoundColumnRefExpression>();
	join_key_cols[bcr.binding.table_index].insert(bcr.binding.column_index);
}

static string BindingKey(const BoundColumnRefExpression &bcr) {
	return to_string(bcr.binding.table_index) + ":" + to_string(bcr.binding.column_index);
}

static string FindBindingParent(unordered_map<string, string> &parents, const string &key) {
	auto it = parents.find(key);
	if (it == parents.end()) {
		parents[key] = key;
		return key;
	}
	if (it->second == key) {
		return key;
	}
	it->second = FindBindingParent(parents, it->second);
	return it->second;
}

static void UnionBindings(unordered_map<string, string> &parents, const string &left, const string &right) {
	string left_parent = FindBindingParent(parents, left);
	string right_parent = FindBindingParent(parents, right);
	if (left_parent != right_parent) {
		parents[right_parent] = left_parent;
	}
}

static void CollectJoinEquivalences(LogicalOperator *op, unordered_map<string, string> &parents,
                                    vector<BoundColumnRefExpression *> &join_refs) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			if (cond.left->type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &left = cond.left->Cast<BoundColumnRefExpression>();
			auto &right = cond.right->Cast<BoundColumnRefExpression>();
			string left_key = BindingKey(left);
			string right_key = BindingKey(right);
			UnionBindings(parents, left_key, right_key);
			join_refs.push_back(&left);
			join_refs.push_back(&right);
		}
	}
	for (auto &child : op->children) {
		CollectJoinEquivalences(child.get(), parents, join_refs);
	}
}

static void CollectProjectionIndex(LogicalOperator *op,
                                   unordered_map<idx_t, LogicalProjection *> &projections_by_index) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		projections_by_index[proj.table_index] = &proj;
	}
	for (auto &child : op->children) {
		CollectProjectionIndex(child.get(), projections_by_index);
	}
}

static bool ResolvesToGroupBinding(idx_t table_index, idx_t column_index, idx_t group_index, size_t group_count,
                                   const unordered_map<idx_t, LogicalProjection *> &projections_by_index,
                                   int depth = 0) {
	if (depth > 16) {
		return false;
	}
	if (table_index == group_index) {
		return column_index < static_cast<idx_t>(group_count);
	}
	auto it = projections_by_index.find(table_index);
	if (it == projections_by_index.end()) {
		return false;
	}
	auto &proj = *it->second;
	if (column_index >= proj.expressions.size()) {
		return false;
	}
	auto &expr = proj.expressions[column_index];
	if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &bcr = expr->Cast<BoundColumnRefExpression>();
	return ResolvesToGroupBinding(bcr.binding.table_index, bcr.binding.column_index, group_index, group_count,
	                              projections_by_index, depth + 1);
}

static string ProjectionOutputName(const unique_ptr<Expression> &expr, idx_t expr_index,
                                   const vector<string> &output_names, const BoundColumnRefExpression &bcr) {
	if (!expr->alias.empty()) {
		return expr->alias;
	}
	if (expr_index < output_names.size() && !output_names[expr_index].empty() &&
	    !IVMTableNames::IsInternalColumn(output_names[expr_index])) {
		return output_names[expr_index];
	}
	return bcr.GetName();
}

static BoundColumnRefExpression *GetColumnRefThroughCasts(Expression *expr) {
	while (expr && expr->expression_class == ExpressionClass::BOUND_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		expr = cast.child.get();
	}
	if (!expr || expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	return &expr->Cast<BoundColumnRefExpression>();
}

static bool AddGroupColumnsFromProjection(LogicalProjection &proj,
                                          const unordered_map<idx_t, LogicalProjection *> &projections_by_index,
                                          idx_t group_index, size_t group_count, const vector<string> &output_names,
                                          vector<string> &group_names) {
	bool matched = false;
	for (idx_t expr_i = 0; expr_i < proj.expressions.size(); expr_i++) {
		auto &expr = proj.expressions[expr_i];
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		if (!ResolvesToGroupBinding(bcr.binding.table_index, bcr.binding.column_index, group_index, group_count,
		                            projections_by_index)) {
			continue;
		}
		string col_name = ProjectionOutputName(expr, expr_i, output_names, bcr);
		if (!IVMTableNames::IsInternalColumn(col_name)) {
			group_names.push_back(col_name);
			matched = true;
		}
	}
	return matched;
}

static bool FindGroupColumns(LogicalOperator *op, const unordered_map<idx_t, LogicalProjection *> &projections_by_index,
                             idx_t group_index, size_t group_count, const vector<string> &output_names,
                             vector<string> &group_names) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		return AddGroupColumnsFromProjection(proj, projections_by_index, group_index, group_count, output_names,
		                                     group_names);
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		if (op->children.size() >= 2) {
			if (FindGroupColumns(op->children[1].get(), projections_by_index, group_index, group_count, output_names,
			                     group_names)) {
				return true;
			}
			return FindGroupColumns(op->children[0].get(), projections_by_index, group_index, group_count, output_names,
			                        group_names);
		}
		return false;
	}
	for (auto &child : op->children) {
		if (FindGroupColumns(child.get(), projections_by_index, group_index, group_count, output_names, group_names)) {
			return true;
		}
	}
	return false;
}

static vector<string> DeriveGroupColumnNames(LogicalOperator *plan, idx_t group_index, size_t group_count,
                                             const vector<string> &output_names, bool has_union_over_agg) {
	vector<string> group_names;
	if (!plan) {
		return group_names;
	}
	if (has_union_over_agg) {
		for (size_t i = 0; i < group_count && i < output_names.size(); i++) {
			if (!IVMTableNames::IsInternalColumn(output_names[i])) {
				group_names.push_back(output_names[i]);
			}
		}
		return group_names;
	}
	unordered_map<idx_t, LogicalProjection *> projections_by_index;
	CollectProjectionIndex(plan, projections_by_index);
	FindGroupColumns(plan, projections_by_index, group_index, group_count, output_names, group_names);
	return group_names;
}

static void AddUniqueGroupNames(vector<string> &group_names, const vector<string> &new_names) {
	for (auto &name : new_names) {
		bool exists = false;
		for (auto &existing : group_names) {
			if (StringUtil::CIEquals(existing, name)) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			group_names.push_back(name);
		}
	}
}

static void CollectSingleDelimKeyBindings(LogicalOperator *op, unordered_set<string> &key_bindings) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::SINGLE) {
			for (auto &expr : join.duplicate_eliminated_columns) {
				if (expr && expr->type == ExpressionType::BOUND_COLUMN_REF) {
					key_bindings.insert(BindingKey(expr->Cast<BoundColumnRefExpression>()));
				}
			}
			for (auto &cond : join.conditions) {
				if (cond.left && cond.left->type == ExpressionType::BOUND_COLUMN_REF) {
					key_bindings.insert(BindingKey(cond.left->Cast<BoundColumnRefExpression>()));
				}
				if (cond.right && cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
					key_bindings.insert(BindingKey(cond.right->Cast<BoundColumnRefExpression>()));
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectSingleDelimKeyBindings(child.get(), key_bindings);
	}
}

static vector<string> DeriveScalarDelimKeyColumnNames(LogicalOperator *plan, const vector<string> &output_names) {
	vector<string> group_names;
	if (!plan) {
		return group_names;
	}
	unordered_set<string> key_bindings;
	CollectSingleDelimKeyBindings(plan, key_bindings);
	auto *top_projection = FindFirstProjection(plan);
	if (top_projection) {
		for (idx_t expr_idx = 0; expr_idx < top_projection->expressions.size() && expr_idx < output_names.size();
		     expr_idx++) {
			auto &expr = top_projection->expressions[expr_idx];
			if (!expr || expr->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			if (!key_bindings.count(BindingKey(expr->Cast<BoundColumnRefExpression>()))) {
				continue;
			}
			if (!output_names[expr_idx].empty() && !IVMTableNames::IsInternalColumn(output_names[expr_idx])) {
				AddUniqueGroupNames(group_names, vector<string> {output_names[expr_idx]});
			}
		}
	}
	if (group_names.empty() && !output_names.empty() && !IVMTableNames::IsInternalColumn(output_names[0])) {
		group_names.push_back(output_names[0]);
	}
	return group_names;
}

static void CollectAggregateGroupColumns(LogicalOperator *root, LogicalOperator *op,
                                         const unordered_map<idx_t, LogicalProjection *> &projections_by_index,
                                         const vector<string> &output_names, vector<string> &group_names,
                                         bool include_first_aggregate, bool &seen_aggregate) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
		bool include_this = include_first_aggregate || seen_aggregate;
		seen_aggregate = true;
		if (include_this && !agg.groups.empty()) {
			vector<string> nested_names;
			if (FindGroupColumns(root, projections_by_index, agg.group_index, agg.groups.size(), output_names,
			                     nested_names)) {
				AddUniqueGroupNames(group_names, nested_names);
			}
		}
	}
	for (auto &child : op->children) {
		CollectAggregateGroupColumns(root, child.get(), projections_by_index, output_names, group_names,
		                             include_first_aggregate, seen_aggregate);
	}
}

static vector<string> DeriveAggregateGroupColumnNames(LogicalOperator *plan, const vector<string> &output_names,
                                                      bool include_first_aggregate) {
	vector<string> group_names;
	if (!plan) {
		return group_names;
	}
	unordered_map<idx_t, LogicalProjection *> projections_by_index;
	CollectProjectionIndex(plan, projections_by_index);
	bool seen_aggregate = false;
	CollectAggregateGroupColumns(plan, plan, projections_by_index, output_names, group_names, include_first_aggregate,
	                             seen_aggregate);
	return group_names;
}

static string FindFirstTableName(LogicalOperator *node) {
	if (!node) {
		return "";
	}
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		auto *get = dynamic_cast<LogicalGet *>(node);
		if (get && get->GetTable().get()) {
			return get->GetTable().get()->name;
		}
	}
	for (auto &child : node->children) {
		string name = FindFirstTableName(child.get());
		if (!name.empty()) {
			return name;
		}
	}
	return "";
}

static void IndexProjectionsAndGets(LogicalOperator *op, unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                    unordered_map<idx_t, LogicalGet *> &get_by_index) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		proj_by_index[proj.table_index] = &proj;
	} else if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		get_by_index[get.table_index] = &get;
	}
	for (auto &child : op->children) {
		IndexProjectionsAndGets(child.get(), proj_by_index, get_by_index);
	}
}

static string ResolveBindingToBaseColumn(BoundColumnRefExpression *bcr,
                                         const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                         const unordered_map<idx_t, LogicalGet *> &get_by_index) {
	idx_t table_index = bcr->binding.table_index;
	idx_t column_index = bcr->binding.column_index;
	for (int depth = 0; depth < 16; depth++) {
		auto get_it = get_by_index.find(table_index);
		if (get_it != get_by_index.end()) {
			auto *get = get_it->second;
			auto &ids = get->GetColumnIds();
			if (column_index < ids.size() && get->GetTable().get()) {
				auto base_idx = ids[column_index].GetPrimaryIndex();
				auto &cols = get->GetTable().get()->GetColumns();
				if (base_idx < cols.LogicalColumnCount()) {
					return cols.GetColumn(LogicalIndex(base_idx)).Name();
				}
			}
			if (column_index < get->names.size()) {
				return get->names[column_index];
			}
			return "";
		}
		auto proj_it = proj_by_index.find(table_index);
		if (proj_it == proj_by_index.end()) {
			return "";
		}
		auto *proj = proj_it->second;
		if (column_index >= proj->expressions.size()) {
			return "";
		}
		auto &expr = proj->expressions[column_index];
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			return expr->alias.empty() ? expr->GetName() : expr->alias;
		}
		auto &next = expr->Cast<BoundColumnRefExpression>();
		table_index = next.binding.table_index;
		column_index = next.binding.column_index;
	}
	return "";
}

static void ResolveWindowPartitionOutputNames(LogicalOperator *plan, vector<string> &partition_columns,
                                              const vector<string> &output_names) {
	if (partition_columns.empty()) {
		return;
	}
	auto *top_projection = FindFirstProjection(plan);
	if (!top_projection) {
		return;
	}

	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);

	for (auto &partition_column : partition_columns) {
		if (partition_column.find('=') != string::npos) {
			continue;
		}
		for (idx_t expr_i = 0; expr_i < top_projection->expressions.size(); expr_i++) {
			auto &expr = top_projection->expressions[expr_i];
			auto *bcr = GetColumnRefThroughCasts(expr.get());
			if (!bcr) {
				continue;
			}
			string base_col = ResolveBindingToBaseColumn(bcr, proj_by_index, get_by_index);
			if (!StringUtil::CIEquals(base_col, partition_column) &&
			    !StringUtil::CIEquals(bcr->GetName(), partition_column)) {
				continue;
			}
			string output_col = ProjectionOutputName(expr, expr_i, output_names, *bcr);
			if (!output_col.empty() && !IVMTableNames::IsInternalColumn(output_col)) {
				partition_column = output_col + "=" + partition_column;
			}
			break;
		}
	}
}

static void ResolveAggregateGroupColumnsThroughJoinKeys(LogicalOperator *plan, vector<string> &aggregate_columns,
                                                        const vector<string> &output_names) {
	if (aggregate_columns.empty()) {
		return;
	}
	auto *top_projection = FindFirstProjection(plan);
	if (!top_projection) {
		return;
	}

	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);

	unordered_map<string, string> parents;
	vector<BoundColumnRefExpression *> join_refs;
	CollectJoinEquivalences(plan, parents, join_refs);
	if (join_refs.empty()) {
		return;
	}

	unordered_map<string, string> output_by_parent;
	for (idx_t expr_i = 0; expr_i < top_projection->expressions.size(); expr_i++) {
		auto &expr = top_projection->expressions[expr_i];
		auto *bcr = GetColumnRefThroughCasts(expr.get());
		if (!bcr) {
			continue;
		}
		string output_col = ProjectionOutputName(expr, expr_i, output_names, *bcr);
		if (output_col.empty() || IVMTableNames::IsInternalColumn(output_col)) {
			continue;
		}
		string parent = FindBindingParent(parents, BindingKey(*bcr));
		if (!output_by_parent.count(parent)) {
			output_by_parent[parent] = output_col;
		}
	}

	for (auto &group_col : aggregate_columns) {
		bool already_visible = false;
		for (auto &output_col : output_names) {
			if (StringUtil::CIEquals(group_col, output_col)) {
				already_visible = true;
				break;
			}
		}
		if (already_visible) {
			continue;
		}
		for (auto *ref : join_refs) {
			string ref_col = ResolveBindingToBaseColumn(ref, proj_by_index, get_by_index);
			if (ref_col.empty()) {
				ref_col = ref->GetName();
			}
			if (!StringUtil::CIEquals(ref_col, group_col) && !StringUtil::CIEquals(ref->GetName(), group_col)) {
				continue;
			}
			string parent = FindBindingParent(parents, BindingKey(*ref));
			auto out_it = output_by_parent.find(parent);
			if (out_it != output_by_parent.end()) {
				group_col = out_it->second;
				break;
			}
		}
		bool resolved = false;
		for (auto &output_col : output_names) {
			if (StringUtil::CIEquals(group_col, output_col)) {
				resolved = true;
				break;
			}
		}
		if (resolved) {
			continue;
		}
		string suffix = group_col;
		while (true) {
			auto underscore = suffix.find('_');
			if (underscore == string::npos || underscore + 1 >= suffix.size()) {
				break;
			}
			suffix = suffix.substr(underscore + 1);
			string matched_output;
			for (auto &output_col : output_names) {
				if (StringUtil::CIEquals(output_col, suffix)) {
					if (!matched_output.empty()) {
						matched_output.clear();
						break;
					}
					matched_output = output_col;
				}
			}
			if (!matched_output.empty()) {
				group_col = matched_output;
				break;
			}
		}
	}
}

static string FullOuterJoinColumnName(const unique_ptr<Expression> &expr,
                                      const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                      const unordered_map<idx_t, LogicalGet *> &get_by_index) {
	if (expr->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return "";
	}
	auto *ref = dynamic_cast<BoundColumnRefExpression *>(expr.get());
	if (!ref) {
		return "";
	}
	string col_name = ResolveBindingToBaseColumn(ref, proj_by_index, get_by_index);
	return col_name.empty() ? ref->GetName() : col_name;
}

static bool ExtractFullOuterJoinCols(LogicalOperator *node,
                                     const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                     const unordered_map<idx_t, LogicalGet *> &get_by_index, string &result) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join && join->join_type == JoinType::OUTER && !join->conditions.empty()) {
			auto &condition = join->conditions[0];
			string left_col_name = FullOuterJoinColumnName(condition.left, proj_by_index, get_by_index);
			string right_col_name = FullOuterJoinColumnName(condition.right, proj_by_index, get_by_index);
			string left_table = !join->children.empty() ? FindFirstTableName(join->children[0].get()) : "";
			string right_table = join->children.size() > 1 ? FindFirstTableName(join->children[1].get()) : "";
			if (!left_col_name.empty() && !right_col_name.empty() && !left_table.empty() && !right_table.empty()) {
				result = left_table + ":" + left_col_name + "," + right_table + ":" + right_col_name;
				return true;
			}
		}
	}
	for (auto &child : node->children) {
		if (ExtractFullOuterJoinCols(child.get(), proj_by_index, get_by_index, result)) {
			return true;
		}
	}
	return false;
}

static string ExtractFullOuterJoinMetadata(LogicalOperator *plan) {
	if (!plan) {
		return "";
	}
	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);
	string result;
	ExtractFullOuterJoinCols(plan, proj_by_index, get_by_index, result);
	return result;
}

static void DeduplicateGroupColumns(vector<string> &group_columns) {
	unordered_set<string> seen_group;
	for (auto &name : group_columns) {
		if (IVMTableNames::IsInternalColumn(name)) {
			continue;
		}
		string candidate = name;
		idx_t suffix = 1;
		while (seen_group.count(candidate)) {
			candidate = name + "_" + to_string(suffix++);
		}
		seen_group.insert(candidate);
		name = candidate;
	}
}

static void AddJoinKeyGroupColumns(LogicalOperator *plan, const vector<string> &output_names,
                                   vector<string> &aggregate_columns, vector<DeltaStrategyReason> &strategy_reasons) {
	if (!plan) {
		return;
	}
	auto *top_proj_ptr = FindFirstProjection(plan);
	auto *cjoin = FindFirstComparisonJoin(plan);
	if (!top_proj_ptr || !cjoin) {
		return;
	}
	unordered_map<idx_t, unordered_set<idx_t>> join_key_cols;
	for (auto &cond : cjoin->conditions) {
		AddJoinKeyColumn(cond.left, join_key_cols);
		AddJoinKeyColumn(cond.right, join_key_cols);
	}
	auto &top_proj = *top_proj_ptr;
	for (idx_t expr_i = 0; expr_i < top_proj.expressions.size(); expr_i++) {
		auto &expr = top_proj.expressions[expr_i];
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		auto it = join_key_cols.find(bcr.binding.table_index);
		if (it == join_key_cols.end() || !it->second.count(bcr.binding.column_index)) {
			continue;
		}
		string col_name;
		if (!expr->alias.empty()) {
			col_name = expr->alias;
		} else if (expr_i < output_names.size() && !output_names[expr_i].empty() &&
		           !IVMTableNames::IsInternalColumn(output_names[expr_i])) {
			col_name = output_names[expr_i];
		} else {
			col_name = bcr.GetName();
		}
		if (IVMTableNames::IsInternalColumn(col_name)) {
			continue;
		}
		bool exists = false;
		for (auto &existing : aggregate_columns) {
			if (StringUtil::CIEquals(existing, col_name)) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			aggregate_columns.push_back(col_name);
			AddStrategyReason(strategy_reasons, DeltaStrategyReason::JOIN_KEY_GROUP_FALLBACK);
		}
	}
}

} // namespace

const char *DeltaStrategyReasonName(DeltaStrategyReason reason) {
	switch (reason) {
	case DeltaStrategyReason::UNION_OVER_AGGREGATE:
		return "UNION_OVER_AGGREGATE";
	case DeltaStrategyReason::JOIN_KEY_GROUP_FALLBACK:
		return "JOIN_KEY_GROUP_FALLBACK";
	case DeltaStrategyReason::DELIM_AGGREGATE_GROUP_FALLBACK:
		return "DELIM_AGGREGATE_GROUP_FALLBACK";
	case DeltaStrategyReason::SCALAR_DELIM_PROJECTION_GROUP_FALLBACK:
		return "SCALAR_DELIM_PROJECTION_GROUP_FALLBACK";
	case DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK:
		return "JOIN_AGGREGATE_PROJECTION_FALLBACK";
	case DeltaStrategyReason::NESTED_AGGREGATE_GROUP_FALLBACK:
		return "NESTED_AGGREGATE_GROUP_FALLBACK";
	case DeltaStrategyReason::REPEATED_CTE_AGGREGATE_GROUP_FALLBACK:
		return "REPEATED_CTE_AGGREGATE_GROUP_FALLBACK";
	default:
		return "UNKNOWN";
	}
}

const char *IVMTypeName(IVMType type) {
	switch (type) {
	case IVMType::AGGREGATE_GROUP:
		return "AGGREGATE_GROUP";
	case IVMType::SIMPLE_AGGREGATE:
		return "SIMPLE_AGGREGATE";
	case IVMType::SIMPLE_PROJECTION:
		return "SIMPLE_PROJECTION";
	case IVMType::FULL_REFRESH:
		return "FULL_REFRESH";
	case IVMType::AGGREGATE_HAVING:
		return "AGGREGATE_HAVING";
	case IVMType::WINDOW_PARTITION:
		return "WINDOW_PARTITION";
	case IVMType::GROUP_RECOMPUTE:
		return "GROUP_RECOMPUTE";
	case IVMType::TOP_K:
		return "TOP_K";
	case IVMType::DISTINCT_INCREMENTAL:
		return "DISTINCT_INCREMENTAL";
	case IVMType::SEMI_ANTI_RECOMPUTE:
		return "SEMI_ANTI_RECOMPUTE";
	default:
		return "UNKNOWN";
	}
}

bool IsDecomposedAggregateType(const string &aggregate_type) {
	return aggregate_type == "avg" || aggregate_type == "stddev" || aggregate_type == "stddev_samp" ||
	       aggregate_type == "stddev_pop" || aggregate_type == "variance" || aggregate_type == "var_samp" ||
	       aggregate_type == "var_pop";
}

bool IsArgMinMaxAggregateType(const string &aggregate_type) {
	return aggregate_type == "arg_min" || aggregate_type == "arg_max";
}

bool IsExtremumAggregateType(const string &aggregate_type) {
	return aggregate_type == "min" || aggregate_type == "max" || IsArgMinMaxAggregateType(aggregate_type);
}

bool HasGroupRecomputeReason(const vector<DeltaStrategyReason> &reasons) {
	return !reasons.empty();
}

bool IsDistinctAtTop(const PlanAnalysis &analysis, const vector<string> &output_names) {
	if (!analysis.found_distinct || analysis.aggregate_columns.empty() || output_names.empty()) {
		return false;
	}
	unordered_set<string> output_lc;
	for (auto &name : output_names) {
		string lc = name;
		std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
		output_lc.insert(lc);
	}
	for (auto &target : analysis.aggregate_columns) {
		string lc = target;
		std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
		if (!output_lc.count(lc)) {
			return false;
		}
	}
	return true;
}

DeltaViewClassification ClassifyDeltaView(const DeltaViewClassificationInput &input) {
	const auto &analysis = input.analysis;
	DeltaViewClassification result;

	if (input.has_unsupported_set_operation || input.has_unsupported_incremental_construct) {
		result.type = IVMType::FULL_REFRESH;
	} else if (analysis.found_window) {
		result.type = IVMType::WINDOW_PARTITION;
	} else if (analysis.found_grouping_sets) {
		result.type = input.has_aggregate_columns ? IVMType::GROUP_RECOMPUTE : IVMType::FULL_REFRESH;
	} else if (analysis.found_semi_anti_join && analysis.found_aggregation) {
		result.type = IVMType::FULL_REFRESH;
	} else if (analysis.found_semi_anti_join && !analysis.found_aggregation) {
		result.type = input.semi_anti_recompute_supported ? IVMType::SEMI_ANTI_RECOMPUTE : IVMType::FULL_REFRESH;
	} else if (!analysis.ivm_compatible) {
		result.type = IVMType::FULL_REFRESH;
		result.warn_unsupported_incremental = true;
	} else if (analysis.found_filtered_list && input.has_aggregate_columns) {
		result.type = IVMType::GROUP_RECOMPUTE;
	} else if (analysis.found_filtered_list) {
		result.type = IVMType::FULL_REFRESH;
	} else if (analysis.found_count_distinct && input.has_aggregate_columns) {
		result.type = IVMType::GROUP_RECOMPUTE;
	} else if (analysis.found_distinct && !input.distinct_at_top && analysis.found_aggregation) {
		result.type = input.distinct_incremental_supported ? IVMType::DISTINCT_INCREMENTAL : IVMType::GROUP_RECOMPUTE;
	} else if (input.union_distinct_over_agg && input.has_aggregate_columns) {
		result.type = IVMType::GROUP_RECOMPUTE;
	} else if (analysis.found_distinct && input.distinct_at_top && input.has_aggregate_columns) {
		result.type = IVMType::AGGREGATE_GROUP;
	} else if (analysis.found_having && analysis.found_aggregation && input.has_aggregate_columns) {
		result.type = IVMType::AGGREGATE_HAVING;
	} else if (HasGroupRecomputeReason(input.strategy_reasons) && input.has_aggregate_columns) {
		result.type = IVMType::GROUP_RECOMPUTE;
	} else if (analysis.found_aggregation && input.has_aggregate_columns) {
		result.type = IVMType::AGGREGATE_GROUP;
	} else if (analysis.found_aggregation && !input.has_aggregate_columns) {
		result.type = IVMType::SIMPLE_AGGREGATE;
	} else if (analysis.found_projection && !analysis.found_aggregation) {
		result.type = IVMType::SIMPLE_PROJECTION;
	} else {
		result.type = IVMType::FULL_REFRESH;
		result.warn_unrecognized_pattern = true;
	}

	return result;
}

DeltaViewModel BuildDeltaViewModel(const DeltaViewClassificationInput &input) {
	auto classification = ClassifyDeltaView(input);
	DeltaViewModel model;
	model.type = classification.type;
	model.strategy_reasons = input.strategy_reasons;
	model.warn_unsupported_incremental = classification.warn_unsupported_incremental;
	model.warn_unrecognized_pattern = classification.warn_unrecognized_pattern;
	return model;
}

DeltaViewModel BuildDeltaViewModel(DeltaViewModelInput input) {
	auto &analysis = input.analysis;
	DeltaViewModel model;

	model.has_minmax_metadata = analysis.found_minmax || analysis.found_count_distinct || analysis.found_list;
	model.distinct_at_top = IsDistinctAtTop(analysis, input.output_names);
	model.union_distinct_over_agg =
	    analysis.found_distinct && model.distinct_at_top && analysis.found_union_before_aggregate;

	if (analysis.found_union_before_aggregate) {
		AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::UNION_OVER_AGGREGATE);
	}
	if (analysis.found_nested_aggregate) {
		AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::NESTED_AGGREGATE_GROUP_FALLBACK);
	}

	size_t group_count = analysis.group_count;
	idx_t group_index = analysis.group_index;
	if (analysis.found_union_before_aggregate) {
		model.group_columns = DeriveGroupColumnNames(input.plan, group_index, group_count, input.output_names, true);
	} else if (model.distinct_at_top) {
		model.group_columns = analysis.aggregate_columns;
	} else if (analysis.found_distinct && analysis.aggregate_columns.empty()) {
		model.group_columns = analysis.aggregate_columns;
	} else if (group_count > 0 && group_index != DConstants::INVALID_INDEX) {
		model.group_columns = DeriveGroupColumnNames(input.plan, group_index, group_count, input.output_names, false);
	}

	if (analysis.found_delim_join && analysis.found_aggregation && model.group_columns.empty() &&
	    input.output_names.size() > analysis.aggregate_types.size()) {
		idx_t key_count = input.output_names.size() - analysis.aggregate_types.size();
		for (idx_t i = 0; i < key_count; i++) {
			if (!input.output_names[i].empty() && !IVMTableNames::IsInternalColumn(input.output_names[i])) {
				model.group_columns.push_back(input.output_names[i]);
			}
		}
		if (!model.group_columns.empty()) {
			AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::DELIM_AGGREGATE_GROUP_FALLBACK);
		}
	}

	if (analysis.found_delim_join && analysis.found_single_join && !analysis.found_aggregation &&
	    model.group_columns.empty()) {
		auto delim_key_names = DeriveScalarDelimKeyColumnNames(input.plan, input.output_names);
		for (auto &name : delim_key_names) {
			model.group_columns.push_back(name);
		}
		if (!model.group_columns.empty()) {
			AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::SCALAR_DELIM_PROJECTION_GROUP_FALLBACK);
		}
	}

	if (analysis.found_nested_aggregate && model.group_columns.empty()) {
		auto nested_group_names = DeriveAggregateGroupColumnNames(input.plan, input.output_names, false);
		for (auto &name : nested_group_names) {
			if (!IVMTableNames::IsInternalColumn(name)) {
				model.group_columns.push_back(name);
			}
		}
		if (!model.group_columns.empty()) {
			AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::NESTED_AGGREGATE_GROUP_FALLBACK);
		}
	}

	if (analysis.found_aggregation && model.group_columns.empty()) {
		auto cte_group_names = DeriveAggregateGroupColumnNames(input.plan, input.output_names, true);
		for (auto &name : cte_group_names) {
			if (!IVMTableNames::IsInternalColumn(name)) {
				model.group_columns.push_back(name);
			}
		}
		if (!model.group_columns.empty()) {
			AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::REPEATED_CTE_AGGREGATE_GROUP_FALLBACK);
		}
	}

	DeduplicateGroupColumns(model.group_columns);

	model.aggregate_types = analysis.aggregate_types;
	model.window_partition_columns = analysis.window_partition_columns;
	ResolveWindowPartitionOutputNames(input.plan, model.window_partition_columns, input.output_names);

	if (analysis.found_join && group_count > 0 && !analysis.found_union_before_aggregate) {
		AddJoinKeyGroupColumns(input.plan, input.output_names, model.group_columns, model.strategy_reasons);
	}

	if (analysis.found_join && analysis.found_aggregation && !model.group_columns.empty()) {
		ResolveAggregateGroupColumnsThroughJoinKeys(input.plan, model.group_columns, input.output_names);
		idx_t expected_linear_outputs = model.group_columns.size() + model.aggregate_types.size();
		if (input.output_names.size() > expected_linear_outputs) {
			AddStrategyReason(model.strategy_reasons, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK);
		}
	}

	if (analysis.found_window && analysis.found_join && !input.keep_window_join_partitions) {
		model.window_partition_columns.clear();
	}
	if (analysis.outer_join_aggregate_needs_recompute) {
		model.has_minmax_metadata = true;
	}
	if (analysis.found_full_outer) {
		model.full_outer_join_cols = ExtractFullOuterJoinMetadata(input.plan);
	}

	DeltaViewClassificationInput classify_input;
	classify_input.analysis = analysis;
	classify_input.has_unsupported_set_operation = analysis.found_unsupported_set_operation;
	classify_input.has_unsupported_incremental_construct = input.has_unsupported_incremental_construct;
	classify_input.has_aggregate_columns = !model.group_columns.empty();
	classify_input.distinct_at_top = model.distinct_at_top;
	classify_input.union_distinct_over_agg = model.union_distinct_over_agg;
	classify_input.strategy_reasons = model.strategy_reasons;
	classify_input.distinct_incremental_supported = input.distinct_incremental_supported;
	classify_input.semi_anti_recompute_supported = input.semi_anti_recompute_supported;

	auto classification = ClassifyDeltaView(classify_input);
	model.type = classification.type;
	model.warn_unsupported_incremental = classification.warn_unsupported_incremental;
	model.warn_unrecognized_pattern = classification.warn_unrecognized_pattern;
	return model;
}

} // namespace duckdb
