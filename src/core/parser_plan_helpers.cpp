#include "core/parser_plan_helpers.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"

#include <set>

namespace duckdb {

/// Build "ORDER BY col1 ASC, col2 DESC LIMIT k [OFFSET n]".
/// Works for both LOGICAL_TOP_N (fused) and separate LOGICAL_ORDER_BY + LOGICAL_LIMIT nodes.
/// output_col_names is the sanitized output column list; BoundColumnRefs are resolved via
/// their column_index into that list.
string BuildTopKSuffix(const vector<BoundOrderByNode> &orders, idx_t limit_val, idx_t offset_val,
                       const vector<string> &output_col_names, bool include_limit) {
	string sql = "ORDER BY ";
	for (size_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		auto &ord = orders[i];
		bool resolved = false;
		if (ord.expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = ord.expression->Cast<BoundColumnRefExpression>();
			idx_t cidx = col_ref.binding.column_index;
			if (cidx < output_col_names.size() && !output_col_names[cidx].empty()) {
				sql += KeywordHelper::WriteOptionallyQuoted(output_col_names[cidx]);
				resolved = true;
			}
		}
		if (!resolved) {
			const string &alias = ord.expression->alias;
			if (!alias.empty()) {
				sql += KeywordHelper::WriteOptionallyQuoted(alias);
			} else {
				sql += ord.expression->ToString();
			}
		}
		sql += " " + ord.GetOrderModifier();
	}
	if (include_limit && limit_val > 0) {
		sql += " LIMIT " + to_string(limit_val);
		if (offset_val > 0) {
			sql += " OFFSET " + to_string(offset_val);
		}
	}
	return sql;
}

static bool PlanContainsCte(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF || op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return true;
	}
	for (auto &child : op->children) {
		if (PlanContainsCte(child.get())) {
			return true;
		}
	}
	return false;
}

static void RelaxMaterializedCtes(LogicalOperator *op) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			cte.materialize = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
		}
	}
	for (auto &child : op->children) {
		RelaxMaterializedCtes(child.get());
	}
}

void InlineCtesIfPresent(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	if (!PlanContainsCte(plan.get())) {
		return;
	}
	RelaxMaterializedCtes(plan.get());
	Optimizer cte_opt(binder, context);
	CTEInlining cte_inlining(cte_opt);
	plan = cte_inlining.Optimize(std::move(plan));
}

string QualifyCreateSourceTable(const string &table_name, const string &current_catalog, const string &current_schema,
                                const string &default_db) {
	if (current_catalog.empty() || current_catalog == default_db || table_name.find('.') != string::npos ||
	    table_name.find('(') != string::npos) {
		return table_name;
	}
	return current_catalog + "." + current_schema + "." + table_name;
}

static string StripTrailingSemicolon(string sql) {
	StringUtil::Trim(sql);
	if (!sql.empty() && sql.back() == ';') {
		sql.pop_back();
		StringUtil::Trim(sql);
	}
	return sql;
}

string ExplainInitialLoadQuery(Connection &con, const string &label, const string &query) {
	string sql = StripTrailingSemicolon(query);
	if (sql.empty()) {
		return label + "\n<empty query>\n";
	}
	auto old_explain = con.Query("SELECT current_setting('explain_output')");
	con.Query("SET explain_output='all'");
	auto explain = con.Query("EXPLAIN " + sql);
	if (old_explain && !old_explain->HasError() && old_explain->RowCount() > 0 &&
	    !old_explain->GetValue(0, 0).IsNull()) {
		con.Query("SET explain_output='" + SqlUtils::EscapeSingleQuotes(old_explain->GetValue(0, 0).ToString()) + "'");
	}
	string result = label + "\n";
	if (!explain || explain->HasError()) {
		result += "<EXPLAIN failed: " + (explain ? explain->GetError() : string("no result")) + ">\n";
	} else {
		result += explain->ToString() + "\n";
	}
	return result;
}

void AddJoinKeyColumn(const unique_ptr<Expression> &expr, unordered_map<idx_t, unordered_set<idx_t>> &join_key_cols) {
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

static bool IsPurePassThroughExpression(const Expression *expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		return true;
	}
	if (expr->expression_class == ExpressionClass::BOUND_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		return IsPurePassThroughExpression(cast.child.get());
	}
	return false;
}

static bool ExpressionReferencesNonGroupBinding(Expression *expr, idx_t group_index) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		return bcr.binding.table_index != group_index;
	}
	bool refs_non_group = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		if (!refs_non_group && ExpressionReferencesNonGroupBinding(child.get(), group_index)) {
			refs_non_group = true;
		}
	});
	return refs_non_group;
}

bool OuterJoinAggregateNeedsRecompute(const CreateMVPlanFacts &facts, idx_t group_index) {
	for (auto *agg_ptr : facts.aggregates) {
		auto &agg = *agg_ptr;
		for (auto &expr : agg.expressions) {
			if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			auto &bound = expr->Cast<BoundAggregateExpression>();
			for (auto &child : bound.children) {
				if (!IsPurePassThroughExpression(child.get())) {
					return true;
				}
			}
		}
	}
	for (auto *proj_ptr : facts.projections) {
		auto &proj = *proj_ptr;
		for (auto &expr : proj.expressions) {
			if (!IsPurePassThroughExpression(expr.get()) &&
			    ExpressionReferencesNonGroupBinding(expr.get(), group_index)) {
				return true;
			}
		}
	}
	return false;
}

static bool ContainsTableFunction(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET && !op.Cast<LogicalGet>().GetTable().get()) {
		return true;
	}
	for (auto &child : op.children) {
		if (ContainsTableFunction(*child)) {
			return true;
		}
	}
	return false;
}

bool OuterJoinPreservedSideHasTableFunction(const CreateMVPlanFacts &facts) {
	for (auto *join : facts.comparison_joins) {
		idx_t preserved_child;
		if (join->join_type == JoinType::LEFT) {
			preserved_child = 0;
		} else if (join->join_type == JoinType::RIGHT) {
			preserved_child = 1;
		} else {
			continue;
		}
		if (join->children.size() > preserved_child && ContainsTableFunction(*join->children[preserved_child])) {
			return true;
		}
	}
	return false;
}

static void AddGetFacts(LogicalGet &get, const string &current_catalog, CreateMVPlanFacts &facts,
                        unordered_map<string, idx_t> &next_occurrence) {
	facts.gets_by_index[get.table_index] = &get;
	auto table_ref = get.GetTable();
	if (table_ref.get()) {
		auto &table = *table_ref.get();
		string table_name = table.name;
		if (!table_name.empty() && !SqlUtils::IsDelta(table_name)) {
			facts.source_table_info[table_name] = {table_name, table.ParentCatalog().GetName(), table.schema.name};
		}
		string table_lc = StringUtil::Lower(table_name);
		ProjectionSourceOccurrence source;
		source.table = table_name;
		source.occurrence = next_occurrence[table_lc]++;
		source.table_index = get.table_index;
		facts.occurrence_by_index[get.table_index] = source;
		facts.source_occurrences.push_back(std::move(source));
	}
	if (get.function.name != "ducklake_scan" || !get.function.function_info) {
		return;
	}
	auto &info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
	string lc = StringUtil::Lower(info.table_name);
	if (facts.ducklake_table_info.find(lc) != facts.ducklake_table_info.end()) {
		return;
	}
	string cat = info.table.ParentCatalog().GetName();
	if (cat.empty()) {
		if (current_catalog.empty()) {
			throw CatalogException("Could not resolve DuckLake catalog for table '" + info.table_name + "'");
		}
		cat = current_catalog;
	}
	DuckLakeSourceTableInfo source_info;
	source_info.table_name = info.table_name;
	source_info.catalog_name = cat;
	source_info.schema_name = info.table.schema.name;
	source_info.table_id = static_cast<int64_t>(info.table_id.index);
	facts.ducklake_table_info[lc] = source_info;
}

static string CollectCreateMVPlanFacts(LogicalOperator *op, const string &current_catalog, CreateMVPlanFacts &facts,
                                       unordered_map<string, idx_t> &next_occurrence, bool seen_agg_above,
                                       bool under_join) {
	if (!op) {
		return "";
	}
	string first_table;
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		seen_agg_above = true;
		auto &agg = op->Cast<LogicalAggregate>();
		facts.aggregates.push_back(&agg);
		for (auto &expr : agg.expressions) {
			if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE &&
			    expr->Cast<BoundAggregateExpression>().filter) {
				facts.has_bound_aggregate_filter = true;
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		if (!seen_agg_above) {
			facts.has_union_before_aggregate = true;
		}
		auto &setop = op->Cast<LogicalSetOperation>();
		facts.setops_by_index[setop.table_index] = &setop;
	} else if (op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_EXCEPT) {
		facts.has_unsupported_set_operation = true;
	} else if (op->type == LogicalOperatorType::LOGICAL_PIVOT) {
		facts.has_pivot = true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_FILTER && !op->children.empty() &&
	    op->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		facts.has_filter_above_aggregate = true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		under_join = true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		if (!facts.first_projection) {
			facts.first_projection = &proj;
		}
		facts.projections.push_back(&proj);
		facts.projections_by_index[proj.table_index] = &proj;
	} else if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	           op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN ||
	           op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if ((op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		     op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) &&
		    !facts.first_comparison_join) {
			facts.first_comparison_join = &join;
		}
		if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
			facts.comparison_joins.push_back(&join);
			for (auto &cond : join.conditions) {
				auto *left = cond.left.get();
				auto *right = cond.right.get();
				if (left->type != ExpressionType::BOUND_COLUMN_REF || right->type != ExpressionType::BOUND_COLUMN_REF) {
					continue;
				}
				auto &left_ref = left->Cast<BoundColumnRefExpression>();
				auto &right_ref = right->Cast<BoundColumnRefExpression>();
				UnionBindings(facts.join_parents, BindingKey(left_ref), BindingKey(right_ref));
				facts.join_refs.push_back(&left_ref);
				facts.join_refs.push_back(&right_ref);
			}
		}
		if (op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN && join.join_type == JoinType::SINGLE) {
			for (auto &expr : join.duplicate_eliminated_columns) {
				if (expr && expr->type == ExpressionType::BOUND_COLUMN_REF) {
					facts.single_delim_key_bindings.insert(BindingKey(expr->Cast<BoundColumnRefExpression>()));
				}
			}
			for (auto &cond : join.conditions) {
				if (cond.left && cond.left->type == ExpressionType::BOUND_COLUMN_REF) {
					facts.single_delim_key_bindings.insert(BindingKey(cond.left->Cast<BoundColumnRefExpression>()));
				}
				if (cond.right && cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
					facts.single_delim_key_bindings.insert(BindingKey(cond.right->Cast<BoundColumnRefExpression>()));
				}
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		AddGetFacts(get, current_catalog, facts, next_occurrence);
		auto table_ref = get.GetTable();
		if (table_ref.get()) {
			first_table = table_ref.get()->name;
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte_ref = op->Cast<LogicalCTERef>();
		facts.cte_refs_by_table_index[cte_ref.table_index] = &cte_ref;
		if (under_join) {
			if (++facts.cte_refs_under_join_count[cte_ref.cte_index] >= 2) {
				facts.has_repeated_cte_ref_under_join = true;
			}
		}
	} else if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op->children.size() >= 2) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		facts.cte_defs_by_index[cte.table_index] = op->children[0].get();
	} else if (op->type == LogicalOperatorType::LOGICAL_WINDOW) {
		facts.windows.push_back(&op->Cast<LogicalWindow>());
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		facts.first_table_name[op] = first_table;
		return first_table;
	}
	for (auto &child : op->children) {
		string child_first =
		    CollectCreateMVPlanFacts(child.get(), current_catalog, facts, next_occurrence, seen_agg_above, under_join);
		if (first_table.empty()) {
			first_table = std::move(child_first);
		}
	}
	if (!first_table.empty()) {
		facts.first_table_name[op] = first_table;
	}
	return first_table;
}

static bool IsHiddenHavingColumn(const string &name) {
	return StringUtil::StartsWith(name, "openivm_having_");
}

static bool IsMinMaxAggregateColumn(const BoundColumnRefExpression &column_ref,
                                    const unordered_map<idx_t, LogicalAggregate *> &aggregates) {
	auto aggregate_it = aggregates.find(column_ref.binding.table_index);
	if (aggregate_it == aggregates.end()) {
		return false;
	}
	auto &aggregate = *aggregate_it->second;
	if (column_ref.binding.column_index >= aggregate.expressions.size()) {
		return false;
	}
	auto &aggregate_expr = aggregate.expressions[column_ref.binding.column_index];
	if (aggregate_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	auto &bound_aggregate = aggregate_expr->Cast<BoundAggregateExpression>();
	return bound_aggregate.function.name == "min" || bound_aggregate.function.name == "max";
}

static bool ExpressionReferencesMinMaxAggregate(Expression &expr,
                                                const unordered_map<idx_t, LogicalAggregate *> &aggregates) {
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column_ref = expr.Cast<BoundColumnRefExpression>();
		return IsMinMaxAggregateColumn(column_ref, aggregates);
	}

	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (!found && ExpressionReferencesMinMaxAggregate(child, aggregates)) {
			found = true;
		}
	});
	return found;
}

static bool ExpressionReferencesUserSumAggregate(Expression &expr, const CreateMVPlanFacts &facts,
                                                 const unordered_map<idx_t, LogicalAggregate *> &aggregates,
                                                 idx_t depth = 0) {
	if (depth > 16) {
		return false;
	}
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column_ref = expr.Cast<BoundColumnRefExpression>();
		auto aggregate_it = aggregates.find(column_ref.binding.table_index);
		if (aggregate_it != aggregates.end()) {
			auto &aggregate = *aggregate_it->second;
			if (column_ref.binding.column_index >= aggregate.expressions.size()) {
				return false;
			}
			auto &aggregate_expr = aggregate.expressions[column_ref.binding.column_index];
			if (aggregate_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &bound_aggregate = aggregate_expr->Cast<BoundAggregateExpression>();
			return bound_aggregate.function.name == "sum" &&
			       !IncrementalTableNames::IsInternalColumn(bound_aggregate.alias);
		}
		auto projection_it = facts.projections_by_index.find(column_ref.binding.table_index);
		if (projection_it == facts.projections_by_index.end()) {
			return false;
		}
		auto &projection = *projection_it->second;
		if (column_ref.binding.column_index >= projection.expressions.size()) {
			return false;
		}
		return ExpressionReferencesUserSumAggregate(*projection.expressions[column_ref.binding.column_index], facts,
		                                            aggregates, depth + 1);
	}

	bool found = false;
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) {
		if (!found && ExpressionReferencesUserSumAggregate(child, facts, aggregates, depth + 1)) {
			found = true;
		}
	});
	return found;
}

static void FinalizeCreateMVPlanFacts(CreateMVPlanFacts &facts) {
	unordered_map<idx_t, LogicalAggregate *> aggregates;
	for (auto *aggregate : facts.aggregates) {
		aggregates[aggregate->aggregate_index] = aggregate;
	}
	if (aggregates.empty()) {
		return;
	}
	for (auto *projection : facts.projections) {
		for (auto &expr : projection->expressions) {
			Expression *unwrapped = expr.get();
			while (unwrapped->expression_class == ExpressionClass::BOUND_CAST) {
				unwrapped = unwrapped->Cast<BoundCastExpression>().child.get();
			}
			if (unwrapped->expression_class != ExpressionClass::BOUND_COLUMN_REF &&
			    ExpressionReferencesUserSumAggregate(*unwrapped, facts, aggregates)) {
				facts.has_computed_sum_aggregate_projection = true;
			}
			if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF && IsHiddenHavingColumn(expr->alias)) {
				auto &column_ref = expr->Cast<BoundColumnRefExpression>();
				if (IsMinMaxAggregateColumn(column_ref, aggregates)) {
					facts.has_hidden_minmax_having_column = true;
				}
			} else if (expr->expression_class != ExpressionClass::BOUND_COLUMN_REF &&
			           ExpressionReferencesMinMaxAggregate(*expr, aggregates)) {
				facts.has_computed_minmax_aggregate_projection = true;
			}
		}
	}
}

static bool ResolvesToGroupBinding(idx_t table_index, idx_t column_index, idx_t group_index, size_t group_count,
                                   const CreateMVPlanFacts &facts, int depth = 0) {
	if (depth > 16) {
		return false;
	}
	if (table_index == group_index) {
		return column_index < static_cast<idx_t>(group_count);
	}
	auto projection_it = facts.projections_by_index.find(table_index);
	if (projection_it != facts.projections_by_index.end()) {
		auto &proj = *projection_it->second;
		if (column_index >= proj.expressions.size()) {
			return false;
		}
		auto &expr = proj.expressions[column_index];
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		return ResolvesToGroupBinding(bcr.binding.table_index, bcr.binding.column_index, group_index, group_count,
		                              facts, depth + 1);
	}
	auto setop_it = facts.setops_by_index.find(table_index);
	if (setop_it != facts.setops_by_index.end()) {
		auto &setop = *setop_it->second;
		if (column_index >= setop.column_count) {
			return false;
		}
		for (auto &child : setop.children) {
			auto bindings = child->GetColumnBindings();
			if (column_index >= bindings.size()) {
				continue;
			}
			auto &binding = bindings[column_index];
			if (ResolvesToGroupBinding(binding.table_index, binding.column_index, group_index, group_count, facts,
			                           depth + 1)) {
				return true;
			}
		}
		return false;
	}
	auto cte_ref_it = facts.cte_refs_by_table_index.find(table_index);
	if (cte_ref_it != facts.cte_refs_by_table_index.end()) {
		auto &cte_ref = *cte_ref_it->second;
		auto cte_def_it = facts.cte_defs_by_index.find(cte_ref.cte_index);
		if (cte_def_it == facts.cte_defs_by_index.end()) {
			return false;
		}
		auto bindings = cte_def_it->second->GetColumnBindings();
		if (column_index >= bindings.size()) {
			return false;
		}
		auto &binding = bindings[column_index];
		return ResolvesToGroupBinding(binding.table_index, binding.column_index, group_index, group_count, facts,
		                              depth + 1);
	}
	return false;
}

bool RelationExists(Connection &con, const string &qualified_name) {
	auto result = con.Query("SELECT * FROM " + qualified_name + " LIMIT 0");
	return !result->HasError();
}

static string ProjectionOutputName(const unique_ptr<Expression> &expr, idx_t expr_index,
                                   const vector<string> &output_names, const BoundColumnRefExpression &bcr) {
	if (!expr->alias.empty()) {
		return expr->alias;
	}
	if (expr_index < output_names.size() && !output_names[expr_index].empty() &&
	    !IncrementalTableNames::IsInternalColumn(output_names[expr_index])) {
		return output_names[expr_index];
	}
	return bcr.GetName();
}

static BoundColumnRefExpression *GetColumnRefThroughCasts(Expression *expr, string *cast_type = nullptr) {
	if (cast_type) {
		cast_type->clear();
	}
	vector<string> cast_specs;
	while (expr && expr->expression_class == ExpressionClass::BOUND_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		if (cast_type) {
			cast_specs.push_back(SqlUtils::BuildCastSpec(expr->return_type.ToString(), cast.try_cast));
		}
		expr = cast.child.get();
	}
	if (!expr || expr->type != ExpressionType::BOUND_COLUMN_REF) {
		return nullptr;
	}
	if (cast_type) {
		for (auto it = cast_specs.rbegin(); it != cast_specs.rend(); it++) {
			*cast_type = SqlUtils::ComposeCastSpecs(*it, *cast_type);
		}
	}
	return &expr->Cast<BoundColumnRefExpression>();
}

static bool AddGroupColumnsFromProjection(LogicalProjection &proj, const CreateMVPlanFacts &facts, idx_t group_index,
                                          size_t group_count, const vector<string> &output_names,
                                          vector<string> &group_names) {
	bool matched = false;
	for (idx_t expr_i = 0; expr_i < proj.expressions.size(); expr_i++) {
		auto &expr = proj.expressions[expr_i];
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		if (!ResolvesToGroupBinding(bcr.binding.table_index, bcr.binding.column_index, group_index, group_count,
		                            facts)) {
			continue;
		}
		string col_name = ProjectionOutputName(expr, expr_i, output_names, bcr);
		if (!IncrementalTableNames::IsInternalColumn(col_name)) {
			group_names.push_back(col_name);
			matched = true;
		}
	}
	return matched;
}

static bool AddGroupColumnsFromBindings(LogicalOperator &op, const CreateMVPlanFacts &facts, idx_t group_index,
                                        size_t group_count, const vector<string> &output_names,
                                        vector<string> &group_names) {
	bool matched = false;
	auto bindings = op.GetColumnBindings();
	for (idx_t col_idx = 0; col_idx < bindings.size() && col_idx < output_names.size(); col_idx++) {
		auto &binding = bindings[col_idx];
		if (!ResolvesToGroupBinding(binding.table_index, binding.column_index, group_index, group_count, facts)) {
			continue;
		}
		if (!output_names[col_idx].empty() && !IncrementalTableNames::IsInternalColumn(output_names[col_idx])) {
			group_names.push_back(output_names[col_idx]);
			matched = true;
		}
	}
	return matched;
}

static bool FindGroupColumns(LogicalOperator *op, const CreateMVPlanFacts &facts, idx_t group_index, size_t group_count,
                             const vector<string> &output_names, vector<string> &group_names) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		return AddGroupColumnsFromProjection(proj, facts, group_index, group_count, output_names, group_names);
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		if (AddGroupColumnsFromBindings(*op, facts, group_index, group_count, output_names, group_names)) {
			return true;
		}
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		if (op->children.size() >= 2) {
			if (FindGroupColumns(op->children[1].get(), facts, group_index, group_count, output_names, group_names)) {
				return true;
			}
			return FindGroupColumns(op->children[0].get(), facts, group_index, group_count, output_names, group_names);
		}
		return false;
	}
	for (auto &child : op->children) {
		if (FindGroupColumns(child.get(), facts, group_index, group_count, output_names, group_names)) {
			return true;
		}
	}
	return false;
}

vector<string> DeriveGroupColumnNames(const CreateMVPlanFacts &facts, idx_t group_index, size_t group_count,
                                      const vector<string> &output_names) {
	vector<string> group_names;
	if (AddGroupColumnsFromBindings(*facts.root, facts, group_index, group_count, output_names, group_names)) {
		return group_names;
	}
	FindGroupColumns(facts.root, facts, group_index, group_count, output_names, group_names);
	return group_names;
}

static void AddUniqueGroupName(vector<string> &group_names, const string &new_name) {
	for (auto &existing : group_names) {
		if (StringUtil::CIEquals(existing, new_name)) {
			return;
		}
	}
	group_names.push_back(new_name);
}

static void AddUniqueGroupNames(vector<string> &group_names, const vector<string> &new_names) {
	for (auto &name : new_names) {
		AddUniqueGroupName(group_names, name);
	}
}

vector<string> DeriveScalarDelimKeyColumnNames(const CreateMVPlanFacts &facts, const vector<string> &output_names) {
	vector<string> group_names;
	auto *top_projection = facts.first_projection;
	if (top_projection) {
		for (idx_t expr_idx = 0; expr_idx < top_projection->expressions.size() && expr_idx < output_names.size();
		     expr_idx++) {
			auto &expr = top_projection->expressions[expr_idx];
			if (!expr || expr->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			if (!facts.single_delim_key_bindings.count(BindingKey(expr->Cast<BoundColumnRefExpression>()))) {
				continue;
			}
			if (!output_names[expr_idx].empty() && !IncrementalTableNames::IsInternalColumn(output_names[expr_idx])) {
				AddUniqueGroupName(group_names, output_names[expr_idx]);
			}
		}
	}
	if (group_names.empty() && !output_names.empty() && !IncrementalTableNames::IsInternalColumn(output_names[0])) {
		// Scalar correlated subqueries normally project the duplicate-eliminated outer keys
		// first. Use that shape as a fallback if binding aliases were hidden by projection
		// rewrites.
		group_names.push_back(output_names[0]);
	}
	return group_names;
}

vector<string> DeriveAggregateGroupColumnNames(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                                               bool include_first_aggregate) {
	vector<string> group_names;
	bool seen_aggregate = false;
	for (auto *agg_ptr : facts.aggregates) {
		auto &agg = *agg_ptr;
		bool include_this = include_first_aggregate || seen_aggregate;
		seen_aggregate = true;
		if (include_this && !agg.groups.empty()) {
			vector<string> nested_names;
			if (FindGroupColumns(facts.root, facts, agg.group_index, agg.groups.size(), output_names, nested_names)) {
				AddUniqueGroupNames(group_names, nested_names);
			}
		}
	}
	return group_names;
}

static bool GetLogicalGetColumnName(LogicalGet &get, idx_t column_index, string &column);

static string ResolveBindingToBaseColumn(BoundColumnRefExpression *bcr, const CreateMVPlanFacts &facts) {
	idx_t table_index = bcr->binding.table_index;
	idx_t column_index = bcr->binding.column_index;
	for (int depth = 0; depth < 16; depth++) {
		auto get_it = facts.gets_by_index.find(table_index);
		if (get_it != facts.gets_by_index.end()) {
			auto *get = get_it->second;
			string column;
			return GetLogicalGetColumnName(*get, column_index, column) ? column : "";
		}
		auto proj_it = facts.projections_by_index.find(table_index);
		if (proj_it == facts.projections_by_index.end()) {
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

using ProjectionLineageStep = RefreshMetadata::ProjectionKeyLineageStep;
using ProjectionLineageArm = RefreshMetadata::ProjectionKeyLineageArm;

static string OccurrenceKey(const string &table, idx_t occurrence) {
	return StringUtil::Lower(table) + "#" + to_string(occurrence);
}

static string OccurrenceKey(const OccurrenceColumnRef &ref) {
	return OccurrenceKey(ref.table, ref.occurrence);
}

static bool GetLogicalGetColumnName(LogicalGet &get, idx_t column_index, string &column) {
	if (get.GetTable().get()) {
		auto &ids = get.GetColumnIds();
		if (column_index < ids.size()) {
			auto base_idx = ids[column_index].GetPrimaryIndex();
			auto &cols = get.GetTable().get()->GetColumns();
			if (base_idx < cols.LogicalColumnCount()) {
				column = cols.GetColumn(LogicalIndex(base_idx)).Name();
				return true;
			}
		}
	}
	if (column_index < get.names.size()) {
		column = get.names[column_index];
		return true;
	}
	return false;
}

static bool GetLogicalGetColumnType(LogicalGet &get, idx_t column_index, LogicalType &type) {
	if (column_index < get.returned_types.size()) {
		type = get.returned_types[column_index];
		return true;
	}
	if (get.GetTable().get()) {
		auto &ids = get.GetColumnIds();
		if (column_index < ids.size()) {
			auto base_idx = ids[column_index].GetPrimaryIndex();
			auto &cols = get.GetTable().get()->GetColumns();
			if (base_idx < cols.LogicalColumnCount()) {
				type = cols.GetColumn(LogicalIndex(base_idx)).Type();
				return true;
			}
		}
	}
	return false;
}

static bool ResolveBindingToGetColumn(ColumnBinding binding, const CreateMVPlanFacts &facts, LogicalGet *&get,
                                      string &column) {
	idx_t table_index = binding.table_index;
	idx_t column_index = binding.column_index;
	for (int depth = 0; depth < 16; depth++) {
		auto get_it = facts.gets_by_index.find(table_index);
		if (get_it != facts.gets_by_index.end()) {
			get = get_it->second;
			return GetLogicalGetColumnName(*get, column_index, column);
		}
		auto proj_it = facts.projections_by_index.find(table_index);
		if (proj_it == facts.projections_by_index.end()) {
			return false;
		}
		auto *proj = proj_it->second;
		if (column_index >= proj->expressions.size()) {
			return false;
		}
		auto *next = GetColumnRefThroughCasts(proj->expressions[column_index].get());
		if (!next) {
			return false;
		}
		table_index = next->binding.table_index;
		column_index = next->binding.column_index;
	}
	return false;
}

static bool ResolveBindingToBaseRef(ColumnBinding binding, const CreateMVPlanFacts &facts, BaseColumnRef &out) {
	LogicalGet *get = nullptr;
	if (!ResolveBindingToGetColumn(binding, facts, get, out.column) || !get->GetTable().get()) {
		return false;
	}
	out.table = get->GetTable().get()->name;
	return true;
}

static bool ResolveBindingToOccurrenceRef(ColumnBinding binding, const CreateMVPlanFacts &facts,
                                          OccurrenceColumnRef &out) {
	LogicalGet *get = nullptr;
	if (!ResolveBindingToGetColumn(binding, facts, get, out.column)) {
		return false;
	}
	auto occurrence_it = facts.occurrence_by_index.find(get->table_index);
	if (occurrence_it == facts.occurrence_by_index.end()) {
		return false;
	}
	out.table = occurrence_it->second.table;
	out.occurrence = occurrence_it->second.occurrence;
	return true;
}

static bool ResolveBindingToOccurrenceRefWithCast(ColumnBinding binding, const CreateMVPlanFacts &facts,
                                                  OccurrenceColumnRef &out, string &cast_type,
                                                  const LogicalType &expression_type) {
	idx_t table_index = binding.table_index;
	idx_t column_index = binding.column_index;
	for (int depth = 0; depth < 16; depth++) {
		auto get_it = facts.gets_by_index.find(table_index);
		if (get_it != facts.gets_by_index.end()) {
			auto *get = get_it->second;
			if (!GetLogicalGetColumnName(*get, column_index, out.column)) {
				return false;
			}
			auto occurrence_it = facts.occurrence_by_index.find(get->table_index);
			if (occurrence_it == facts.occurrence_by_index.end()) {
				return false;
			}
			out.table = occurrence_it->second.table;
			out.occurrence = occurrence_it->second.occurrence;
			LogicalType base_type;
			if (cast_type.empty() && GetLogicalGetColumnType(*get, column_index, base_type) &&
			    !(base_type == expression_type)) {
				cast_type = expression_type.ToString();
			}
			return true;
		}
		auto proj_it = facts.projections_by_index.find(table_index);
		if (proj_it == facts.projections_by_index.end()) {
			return false;
		}
		auto *proj = proj_it->second;
		if (column_index >= proj->expressions.size()) {
			return false;
		}
		string projection_cast;
		auto *next = GetColumnRefThroughCasts(proj->expressions[column_index].get(), &projection_cast);
		if (!next) {
			return false;
		}
		if (!projection_cast.empty()) {
			cast_type = SqlUtils::ComposeCastSpecs(cast_type, projection_cast);
		}
		table_index = next->binding.table_index;
		column_index = next->binding.column_index;
	}
	return false;
}

static void AddJoinEdgesFromFacts(CreateMVPlanFacts &facts) {
	for (auto *join : facts.comparison_joins) {
		if (join->join_type != JoinType::INNER) {
			continue;
		}
		for (auto &cond : join->conditions) {
			auto *left = GetColumnRefThroughCasts(cond.left.get());
			auto *right = GetColumnRefThroughCasts(cond.right.get());
			if (!left || !right) {
				continue;
			}
			BaseColumnRef lbase, rbase;
			if (ResolveBindingToBaseRef(left->binding, facts, lbase) &&
			    ResolveBindingToBaseRef(right->binding, facts, rbase)) {
				facts.inner_join_edges.emplace_back(std::move(lbase), std::move(rbase));
			}
			if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			OccurrenceColumnRef lref, rref;
			if (ResolveBindingToOccurrenceRef(left->binding, facts, lref) &&
			    ResolveBindingToOccurrenceRef(right->binding, facts, rref)) {
				facts.projection_lineage_edges.push_back({lref, rref});
				facts.projection_lineage_edges.push_back({rref, lref});
			}
		}
	}
}

bool ProducesAtMostOneRow(LogicalOperator &node) {
	LogicalOperator *current = &node;
	while ((current->type == LogicalOperatorType::LOGICAL_PROJECTION ||
	        current->type == LogicalOperatorType::LOGICAL_FILTER) &&
	       !current->children.empty()) {
		current = current->children[0].get();
	}
	if (current->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto &aggregate = current->Cast<LogicalAggregate>();
	return aggregate.groups.empty() && aggregate.grouping_sets.size() <= 1;
}

CreateMVPlanFacts BuildCreateMVPlanFacts(LogicalOperator *plan, const string &current_catalog) {
	CreateMVPlanFacts facts;
	facts.root = plan;
	facts.analysis = AnalyzePlan(plan);
	LogicalOperator *top = plan;
	while (top && top->children.size() == 1 &&
	       (top->type == LogicalOperatorType::LOGICAL_CREATE_TABLE ||
	        top->type == LogicalOperatorType::LOGICAL_PROJECTION || top->type == LogicalOperatorType::LOGICAL_FILTER ||
	        top->type == LogicalOperatorType::LOGICAL_ORDER_BY || top->type == LogicalOperatorType::LOGICAL_LIMIT ||
	        top->type == LogicalOperatorType::LOGICAL_TOP_N)) {
		top = top->children[0].get();
	}
	facts.has_top_level_redundant_scalar_distinct = top && top->type == LogicalOperatorType::LOGICAL_DISTINCT &&
	                                                !top->children.empty() && ProducesAtMostOneRow(*top->children[0]);
	unordered_map<string, idx_t> next_occurrence;
	CollectCreateMVPlanFacts(plan, current_catalog, facts, next_occurrence, false, false);
	FinalizeCreateMVPlanFacts(facts);
	AddJoinEdgesFromFacts(facts);
	return facts;
}

static bool FindProjectionPath(const ProjectionSourceOccurrence &source, const OccurrenceColumnRef &key_ref,
                               const unordered_map<string, vector<ProjectionLineageEdge>> &adjacency,
                               vector<ProjectionLineageEdge> &path) {
	string source_key = OccurrenceKey(source.table, source.occurrence);
	string target_key = OccurrenceKey(key_ref);
	if (source_key == target_key) {
		path.clear();
		return true;
	}

	vector<string> queue;
	unordered_map<string, bool> seen;
	unordered_map<string, ProjectionLineageEdge> prev_edge;
	unordered_map<string, string> prev_node;
	queue.push_back(source_key);
	seen[source_key] = true;
	for (idx_t pos = 0; pos < queue.size(); pos++) {
		string node = queue[pos];
		auto adjacency_it = adjacency.find(node);
		if (adjacency_it == adjacency.end()) {
			continue;
		}
		for (auto &edge : adjacency_it->second) {
			string next = OccurrenceKey(edge.to);
			if (seen[next]) {
				continue;
			}
			seen[next] = true;
			prev_edge[next] = edge;
			prev_node[next] = node;
			if (next == target_key) {
				vector<ProjectionLineageEdge> reversed;
				string walk = target_key;
				while (walk != source_key) {
					reversed.push_back(prev_edge[walk]);
					walk = prev_node[walk];
				}
				path.assign(reversed.rbegin(), reversed.rend());
				return true;
			}
			queue.push_back(next);
		}
	}
	return false;
}

static bool BuildProjectionLineageArm(const ProjectionSourceOccurrence &source, const OccurrenceColumnRef &key_ref,
                                      const vector<ProjectionLineageEdge> &path, ProjectionLineageArm &arm) {
	arm.source = source.table;
	arm.occurrence = source.occurrence;
	arm.steps.clear();
	if (path.empty()) {
		arm.source_col = key_ref.column;
		return StringUtil::CIEquals(source.table, key_ref.table) && source.occurrence == key_ref.occurrence;
	}
	arm.source_col = path[0].from.column;
	for (idx_t i = 0; i < path.size(); i++) {
		ProjectionLineageStep step;
		step.table = path[i].to.table;
		step.occurrence = path[i].to.occurrence;
		step.lookup_col = path[i].to.column;
		step.lookup_out = i + 1 < path.size() ? path[i + 1].from.column : key_ref.column;
		arm.steps.push_back(std::move(step));
	}
	return true;
}

string BuildRefreshLineageJson(const vector<string> &entries) {
	if (entries.empty()) {
		return "";
	}
	return "{\"v\":1,\"items\":[" +
	       StringUtil::Join(entries, entries.size(), ",", [](const string &entry) { return entry; }) + "]}";
}

bool BuildProjectionKeyLineage(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                               RefreshMetadata::ProjectionKeyLineage &out) {
	if (!facts.root) {
		return false;
	}
	auto *top_projection = facts.first_projection;
	if (!top_projection) {
		return false;
	}
	if (facts.source_occurrences.size() < 3) {
		return false;
	}
	if (facts.projection_lineage_edges.empty()) {
		return false;
	}

	unordered_map<string, vector<ProjectionLineageEdge>> adjacency;
	for (auto &edge : facts.projection_lineage_edges) {
		adjacency[OccurrenceKey(edge.from)].push_back(edge);
	}

	for (idx_t expr_i = 0; expr_i < top_projection->expressions.size(); expr_i++) {
		auto &expr = top_projection->expressions[expr_i];
		auto *bcr = GetColumnRefThroughCasts(expr.get());
		if (!bcr) {
			continue;
		}
		OccurrenceColumnRef key_ref;
		if (!ResolveBindingToOccurrenceRef(bcr->binding, facts, key_ref)) {
			continue;
		}
		string output_col = ProjectionOutputName(expr, expr_i, output_names, *bcr);
		if (output_col.empty() || IncrementalTableNames::IsInternalColumn(output_col)) {
			continue;
		}

		vector<ProjectionLineageArm> arms;
		bool covered = true;
		for (auto &occurrence : facts.source_occurrences) {
			vector<ProjectionLineageEdge> path;
			if (!FindProjectionPath(occurrence, key_ref, adjacency, path)) {
				covered = false;
				break;
			}
			ProjectionLineageArm arm;
			if (!BuildProjectionLineageArm(occurrence, key_ref, path, arm)) {
				covered = false;
				break;
			}
			arms.push_back(std::move(arm));
		}
		if (!covered || arms.empty()) {
			continue;
		}

		RefreshMetadata::ProjectionKeyLineage lineage;
		lineage.output_col = output_col;
		lineage.key_source = key_ref.table;
		lineage.key_occurrence = key_ref.occurrence;
		lineage.key_col = key_ref.column;
		lineage.arms = std::move(arms);
		out = std::move(lineage);
		return true;
	}
	return false;
}

static string NullableGetTableName(LogicalGet &get) {
	auto table = get.GetTable();
	if (table.get() && !table.get()->name.empty()) {
		return table.get()->name;
	}
	if (get.function.name == "ducklake_scan" && get.function.function_info) {
		return get.function.function_info->Cast<DuckLakeFunctionInfo>().table_name;
	}
	return "";
}

static string NormalizeNullableTableName(const string &table_name) {
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	static const string delta_prefix(openivm::DELTA_PREFIX);
	string last = SqlUtils::LastIdentifierPart(table_name);
	if (last.size() > data_prefix.size() && last.rfind(data_prefix, 0) == 0) {
		last = last.substr(data_prefix.size());
	}
	if (last.size() > delta_prefix.size() && last.rfind(delta_prefix, 0) == 0) {
		last = last.substr(delta_prefix.size());
	}
	return StringUtil::Lower(last);
}

static void CollectNullableBaseTables(LogicalOperator *node, std::set<string> &out, bool &complete, int depth) {
	if (!node || depth > 64) {
		complete = false;
		return;
	}
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		string table_name = NullableGetTableName(node->Cast<LogicalGet>());
		if (table_name.empty()) {
			complete = false;
			return;
		}
		out.insert(NormalizeNullableTableName(table_name));
		return;
	}
	if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		complete = false;
		return;
	}
	for (auto &child : node->children) {
		CollectNullableBaseTables(child.get(), out, complete, depth + 1);
	}
}

bool BuildLeftJoinNullableSources(const CreateMVPlanFacts &facts, RefreshMetadata::LeftJoinNullableSources &out) {
	out.tables.clear();
	out.complete = true;
	std::set<string> tables;
	bool found_any = false;
	vector<LogicalOperator *> stack;
	if (facts.root) {
		stack.push_back(facts.root);
	}
	while (!stack.empty()) {
		auto *node = stack.back();
		stack.pop_back();
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (join && node->children.size() >= 2) {
			if (join->join_type == JoinType::LEFT) {
				found_any = true;
				CollectNullableBaseTables(node->children[1].get(), tables, out.complete, 0);
			} else if (join->join_type == JoinType::RIGHT) {
				found_any = true;
				CollectNullableBaseTables(node->children[0].get(), tables, out.complete, 0);
			} else if (join->join_type == JoinType::OUTER) {
				found_any = true;
				CollectNullableBaseTables(node->children[0].get(), tables, out.complete, 0);
				CollectNullableBaseTables(node->children[1].get(), tables, out.complete, 0);
			}
		}
		for (auto &child : node->children) {
			stack.push_back(child.get());
		}
	}
	out.tables.assign(tables.begin(), tables.end());
	return found_any;
}

struct WindowLineageOp {
	string kind;
	string output_col;
	string source_table;
	idx_t source_occurrence = 0;
	string source_col;
	string source_cast;
	string lookup_table;
	idx_t lookup_occurrence = 0;
	string lookup_col;
	string lookup_cast;
	string lookup_output_col;
	string lookup_output_cast;
};

struct WindowLookupEdge {
	OccurrenceColumnRef lookup_join;
	string lookup_cast;
	OccurrenceColumnRef changed_join;
	string changed_cast;
};

struct WindowEquivalenceEdge {
	OccurrenceColumnRef first;
	string first_cast;
	OccurrenceColumnRef second;
	string second_cast;

	bool HasCast() const {
		return !first_cast.empty() || !second_cast.empty();
	}
};

static void CollectLeafColumnBindings(Expression *expr, vector<ColumnBinding> &bindings) {
	if (!expr) {
		return;
	}
	if (auto *bcr = GetColumnRefThroughCasts(expr)) {
		bindings.push_back(bcr->binding);
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](Expression &child) { CollectLeafColumnBindings(&child, bindings); });
}

static void AddUniqueOccurrenceRef(vector<OccurrenceColumnRef> &refs, const OccurrenceColumnRef &ref) {
	for (auto &existing : refs) {
		if (StringUtil::CIEquals(existing.table, ref.table) && existing.occurrence == ref.occurrence &&
		    StringUtil::CIEquals(existing.column, ref.column)) {
			return;
		}
	}
	refs.push_back(ref);
}

static bool ResolveBindingToOccurrenceRefsInternal(ColumnBinding binding, const CreateMVPlanFacts &facts,
                                                   vector<OccurrenceColumnRef> &out, int depth) {
	if (depth >= 16) {
		return false;
	}
	OccurrenceColumnRef direct_ref;
	if (ResolveBindingToOccurrenceRef(binding, facts, direct_ref)) {
		AddUniqueOccurrenceRef(out, direct_ref);
		return true;
	}
	auto proj_it = facts.projections_by_index.find(binding.table_index);
	if (proj_it == facts.projections_by_index.end()) {
		return false;
	}
	auto *proj = proj_it->second;
	if (binding.column_index >= proj->expressions.size()) {
		return false;
	}
	vector<ColumnBinding> child_bindings;
	CollectLeafColumnBindings(proj->expressions[binding.column_index].get(), child_bindings);
	bool found = false;
	for (auto &child_binding : child_bindings) {
		if (child_binding.table_index == binding.table_index && child_binding.column_index == binding.column_index) {
			continue;
		}
		found |= ResolveBindingToOccurrenceRefsInternal(child_binding, facts, out, depth + 1);
	}
	return found;
}

static bool ResolveBindingToOccurrenceRefs(ColumnBinding binding, const CreateMVPlanFacts &facts,
                                           vector<OccurrenceColumnRef> &out) {
	out.clear();
	return ResolveBindingToOccurrenceRefsInternal(binding, facts, out, 0);
}

static void CollectInnerJoinEdgesOccurrence(LogicalOperator *op, const CreateMVPlanFacts &facts,
                                            vector<WindowEquivalenceEdge> &edges) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			for (auto &cond : join.conditions) {
				if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				string left_cast;
				string right_cast;
				auto *left = GetColumnRefThroughCasts(cond.left.get(), &left_cast);
				auto *right = GetColumnRefThroughCasts(cond.right.get(), &right_cast);
				if (!left || !right) {
					continue;
				}
				OccurrenceColumnRef lref, rref;
				if (ResolveBindingToOccurrenceRefWithCast(left->binding, facts, lref, left_cast, left->return_type) &&
				    ResolveBindingToOccurrenceRefWithCast(right->binding, facts, rref, right_cast,
				                                          right->return_type)) {
					edges.push_back({std::move(lref), std::move(left_cast), std::move(rref), std::move(right_cast)});
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectInnerJoinEdgesOccurrence(child.get(), facts, edges);
	}
}

static void CollectWindowLookupEdges(LogicalOperator *op, const CreateMVPlanFacts &facts,
                                     vector<WindowLookupEdge> &edges) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		auto add_lookup_edge = [&](OccurrenceColumnRef lookup_ref, string lookup_cast, OccurrenceColumnRef changed_ref,
		                           string changed_cast) {
			edges.push_back(
			    {std::move(lookup_ref), std::move(lookup_cast), std::move(changed_ref), std::move(changed_cast)});
		};
		for (auto &cond : join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			string left_cast;
			string right_cast;
			auto *left = GetColumnRefThroughCasts(cond.left.get(), &left_cast);
			auto *right = GetColumnRefThroughCasts(cond.right.get(), &right_cast);
			if (!left || !right) {
				continue;
			}
			OccurrenceColumnRef lref, rref;
			if (!ResolveBindingToOccurrenceRefWithCast(left->binding, facts, lref, left_cast, left->return_type) ||
			    !ResolveBindingToOccurrenceRefWithCast(right->binding, facts, rref, right_cast, right->return_type)) {
				continue;
			}
			switch (join.join_type) {
			case JoinType::INNER:
				add_lookup_edge(lref, left_cast, rref, right_cast);
				add_lookup_edge(rref, right_cast, lref, left_cast);
				break;
			case JoinType::LEFT:
				add_lookup_edge(lref, left_cast, rref, right_cast);
				add_lookup_edge(rref, right_cast, lref, left_cast);
				break;
			case JoinType::RIGHT:
				add_lookup_edge(rref, right_cast, lref, left_cast);
				add_lookup_edge(lref, left_cast, rref, right_cast);
				break;
			default:
				break;
			}
		}
	}
	for (auto &child : op->children) {
		CollectWindowLookupEdges(child.get(), facts, edges);
	}
}

static void CollectWindowPartitionRefs(LogicalOperator *op, const CreateMVPlanFacts &facts,
                                       const vector<string> &partition_columns, vector<WindowLineageOp> &direct_ops) {
	if (op->type == LogicalOperatorType::LOGICAL_WINDOW) {
		auto &window = op->Cast<LogicalWindow>();
		for (auto &expr : window.expressions) {
			if (expr->expression_class != ExpressionClass::BOUND_WINDOW) {
				continue;
			}
			auto &win_expr = expr->Cast<BoundWindowExpression>();
			for (auto &part : win_expr.partitions) {
				string partition_cast;
				auto *bcr = GetColumnRefThroughCasts(part.get(), &partition_cast);
				if (!bcr) {
					continue;
				}
				vector<OccurrenceColumnRef> refs;
				if (!ResolveBindingToOccurrenceRefs(bcr->binding, facts, refs)) {
					continue;
				}
				for (auto &ref : refs) {
					for (auto &stored : partition_columns) {
						auto pos = stored.find('=');
						string output_col = pos == string::npos ? stored : stored.substr(0, pos);
						string source_col = pos == string::npos ? stored : stored.substr(pos + 1);
						if (!StringUtil::CIEquals(source_col, ref.column)) {
							continue;
						}
						WindowLineageOp op;
						op.kind = "direct";
						op.output_col = output_col;
						op.source_table = ref.table;
						op.source_occurrence = ref.occurrence;
						op.source_col = ref.column;
						op.source_cast = partition_cast;
						direct_ops.push_back(std::move(op));
					}
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectWindowPartitionRefs(child.get(), facts, partition_columns, direct_ops);
	}
}

static bool SameRef(const OccurrenceColumnRef &a, const OccurrenceColumnRef &b) {
	return StringUtil::CIEquals(a.table, b.table) && a.occurrence == b.occurrence &&
	       StringUtil::CIEquals(a.column, b.column);
}

static bool SameOp(const WindowLineageOp &a, const WindowLineageOp &b) {
	return a.kind == b.kind && StringUtil::CIEquals(a.output_col, b.output_col) &&
	       StringUtil::CIEquals(a.source_table, b.source_table) && a.source_occurrence == b.source_occurrence &&
	       StringUtil::CIEquals(a.source_col, b.source_col) && StringUtil::CIEquals(a.source_cast, b.source_cast) &&
	       StringUtil::CIEquals(a.lookup_table, b.lookup_table) && a.lookup_occurrence == b.lookup_occurrence &&
	       StringUtil::CIEquals(a.lookup_col, b.lookup_col) && StringUtil::CIEquals(a.lookup_cast, b.lookup_cast) &&
	       StringUtil::CIEquals(a.lookup_output_col, b.lookup_output_col) &&
	       StringUtil::CIEquals(a.lookup_output_cast, b.lookup_output_cast);
}

static void AddUniqueLineageOp(vector<WindowLineageOp> &ops, WindowLineageOp op) {
	for (auto &existing : ops) {
		if (SameOp(existing, op)) {
			return;
		}
	}
	ops.push_back(std::move(op));
}

static bool HasEquivalentPartitionRef(const vector<WindowLineageOp> &ops, const OccurrenceColumnRef &ref,
                                      const string &output_col) {
	for (auto &op : ops) {
		if (op.kind == "direct" && StringUtil::CIEquals(op.output_col, output_col) &&
		    StringUtil::CIEquals(op.source_table, ref.table) && op.source_occurrence == ref.occurrence &&
		    StringUtil::CIEquals(op.source_col, ref.column)) {
			return true;
		}
	}
	return false;
}

static RefreshMetadata::WindowPartitionLineageOp ToMetadataWindowLineageOp(const WindowLineageOp &op) {
	RefreshMetadata::WindowPartitionLineageOp metadata_op;
	metadata_op.kind = op.kind;
	metadata_op.output_col = op.output_col;
	metadata_op.source = op.source_table;
	metadata_op.source_col = op.source_col;
	metadata_op.source_cast = op.source_cast;
	metadata_op.lookup = op.lookup_table;
	metadata_op.lookup_col = op.lookup_col;
	metadata_op.lookup_cast = op.lookup_cast;
	metadata_op.lookup_out = op.lookup_output_col;
	metadata_op.lookup_out_cast = op.lookup_output_cast;
	return metadata_op;
}

bool BuildWindowPartitionLineageOps(const CreateMVPlanFacts &facts, const vector<string> &partition_columns,
                                    vector<RefreshMetadata::WindowPartitionLineageOp> &out,
                                    vector<RefreshMetadata::WindowPartitionLineageOp> *direct_out) {
	out.clear();
	if (direct_out) {
		direct_out->clear();
	}
	if (!facts.root || partition_columns.empty()) {
		return false;
	}
	auto *plan = facts.root;
	if (facts.source_occurrences.empty()) {
		return false;
	}

	vector<WindowLineageOp> direct_ops;
	CollectWindowPartitionRefs(plan, facts, partition_columns, direct_ops);
	if (direct_ops.empty()) {
		return false;
	}
	if (direct_out) {
		for (auto &op : direct_ops) {
			direct_out->push_back(ToMetadataWindowLineageOp(op));
		}
	}

	vector<WindowEquivalenceEdge> edges;
	CollectInnerJoinEdgesOccurrence(plan, facts, edges);
	vector<WindowLookupEdge> lookup_edges;
	CollectWindowLookupEdges(plan, facts, lookup_edges);

	vector<WindowLineageOp> partition_ops;
	for (auto &op : direct_ops) {
		AddUniqueLineageOp(partition_ops, op);
	}

	bool changed = true;
	while (changed) {
		changed = false;
		vector<WindowLineageOp> next_ops = partition_ops;
		for (auto &edge : edges) {
			if (edge.HasCast()) {
				continue;
			}
			for (auto &direct : partition_ops) {
				OccurrenceColumnRef direct_ref;
				direct_ref.table = direct.source_table;
				direct_ref.occurrence = direct.source_occurrence;
				direct_ref.column = direct.source_col;
				OccurrenceColumnRef other;
				bool found = false;
				if (SameRef(edge.first, direct_ref)) {
					other = edge.second;
					found = true;
				} else if (SameRef(edge.second, direct_ref)) {
					other = edge.first;
					found = true;
				}
				if (!found || HasEquivalentPartitionRef(next_ops, other, direct.output_col)) {
					continue;
				}
				WindowLineageOp equivalent;
				equivalent.kind = "direct";
				equivalent.output_col = direct.output_col;
				equivalent.source_table = other.table;
				equivalent.source_occurrence = other.occurrence;
				equivalent.source_col = other.column;
				equivalent.source_cast = direct.source_cast;
				AddUniqueLineageOp(next_ops, std::move(equivalent));
				changed = true;
			}
		}
		partition_ops = std::move(next_ops);
	}

	vector<WindowLineageOp> ops;
	for (auto &op : partition_ops) {
		AddUniqueLineageOp(ops, op);
	}

	for (auto &edge : lookup_edges) {
		for (auto &direct : partition_ops) {
			if (!StringUtil::CIEquals(edge.lookup_join.table, direct.source_table) ||
			    edge.lookup_join.occurrence != direct.source_occurrence) {
				continue;
			}
			if (StringUtil::CIEquals(edge.lookup_join.column, direct.source_col) &&
			    StringUtil::CIEquals(edge.changed_join.table, direct.source_table) &&
			    edge.changed_join.occurrence == direct.source_occurrence) {
				continue;
			}
			WindowLineageOp lookup;
			lookup.kind = "lookup";
			lookup.output_col = direct.output_col;
			lookup.source_table = edge.changed_join.table;
			lookup.source_occurrence = edge.changed_join.occurrence;
			lookup.source_col = edge.changed_join.column;
			lookup.source_cast = edge.changed_cast;
			lookup.lookup_table = direct.source_table;
			lookup.lookup_occurrence = direct.source_occurrence;
			lookup.lookup_col = edge.lookup_join.column;
			lookup.lookup_cast = edge.lookup_cast;
			lookup.lookup_output_col = direct.source_col;
			lookup.lookup_output_cast = direct.source_cast;
			AddUniqueLineageOp(ops, std::move(lookup));
		}
	}

	for (auto &op : ops) {
		out.push_back(ToMetadataWindowLineageOp(op));
	}
	return !out.empty();
}

void ResolveWindowPartitionOutputNames(const CreateMVPlanFacts &facts, vector<string> &partition_columns,
                                       const vector<string> &output_names) {
	if (partition_columns.empty()) {
		return;
	}
	auto *top_projection = facts.first_projection;
	if (!top_projection) {
		return;
	}

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
			string base_col = ResolveBindingToBaseColumn(bcr, facts);
			if (!StringUtil::CIEquals(base_col, partition_column) &&
			    !StringUtil::CIEquals(bcr->GetName(), partition_column)) {
				continue;
			}
			string output_col = ProjectionOutputName(expr, expr_i, output_names, *bcr);
			if (!output_col.empty() && !IncrementalTableNames::IsInternalColumn(output_col)) {
				partition_column = output_col + "=" + partition_column;
			}
			break;
		}
	}
}

void ResolveAggregateGroupColumnsThroughJoinKeys(const CreateMVPlanFacts &facts, vector<string> &aggregate_columns,
                                                 const vector<string> &output_names) {
	if (aggregate_columns.empty()) {
		return;
	}
	auto *top_projection = facts.first_projection;
	if (!top_projection) {
		return;
	}
	auto parents = facts.join_parents;
	if (facts.join_refs.empty()) {
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
		if (output_col.empty() || IncrementalTableNames::IsInternalColumn(output_col)) {
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
		for (auto *ref : facts.join_refs) {
			string ref_col = ResolveBindingToBaseColumn(ref, facts);
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

static string FullOuterJoinColumnName(const unique_ptr<Expression> &expr, const CreateMVPlanFacts &facts) {
	if (expr->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return "";
	}
	auto *ref = dynamic_cast<BoundColumnRefExpression *>(expr.get());
	if (!ref) {
		return "";
	}
	string col_name = ResolveBindingToBaseColumn(ref, facts);
	return col_name.empty() ? ref->GetName() : col_name;
}

string ExtractFullOuterJoinMetadata(const CreateMVPlanFacts &facts) {
	for (auto *join : facts.comparison_joins) {
		if (join->join_type != JoinType::OUTER || join->conditions.empty()) {
			continue;
		}
		auto &condition = join->conditions[0];
		string left_col_name = FullOuterJoinColumnName(condition.left, facts);
		string right_col_name = FullOuterJoinColumnName(condition.right, facts);
		string left_table;
		string right_table;
		if (!join->children.empty()) {
			auto it = facts.first_table_name.find(join->children[0].get());
			left_table = it == facts.first_table_name.end() ? string() : it->second;
		}
		if (join->children.size() > 1) {
			auto it = facts.first_table_name.find(join->children[1].get());
			right_table = it == facts.first_table_name.end() ? string() : it->second;
		}
		if (!left_col_name.empty() && !right_col_name.empty() && !left_table.empty() && !right_table.empty()) {
			return left_table + ":" + left_col_name + "," + right_table + ":" + right_col_name;
		}
	}
	return "";
}

static string SanitizeOutputName(const string &name) {
	if (IncrementalTableNames::IsInternalColumn(name)) {
		return name;
	}
	string clean;
	bool last_was_underscore = false;
	for (auto c : name) {
		if (isalnum(c)) {
			clean += c;
			last_was_underscore = false;
		} else if (!last_was_underscore && !clean.empty()) {
			clean += '_';
			last_was_underscore = true;
		}
	}
	if (!clean.empty() && clean.back() == '_') {
		clean.pop_back();
	}
	return clean.empty() ? name : clean;
}

static LogicalOperator *FindTopNameSource(LogicalOperator *plan) {
	for (auto *walk = plan; walk;) {
		if (walk->type == LogicalOperatorType::LOGICAL_PROJECTION ||
		    walk->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return walk;
		}
		if ((walk->type == LogicalOperatorType::LOGICAL_ORDER_BY || walk->type == LogicalOperatorType::LOGICAL_LIMIT ||
		     walk->type == LogicalOperatorType::LOGICAL_TOP_N || walk->type == LogicalOperatorType::LOGICAL_DISTINCT ||
		     walk->type == LogicalOperatorType::LOGICAL_FILTER) &&
		    !walk->children.empty()) {
			walk = walk->children[0].get();
			continue;
		}
		break;
	}
	return nullptr;
}

static void AppendProjectionOutputNames(LogicalProjection &projection, idx_t output_count,
                                        vector<string> &output_names) {
	while (output_names.size() < output_count) {
		idx_t idx = output_names.size();
		if (idx < projection.expressions.size() && !projection.expressions[idx]->alias.empty()) {
			output_names.push_back(projection.expressions[idx]->alias);
		} else {
			output_names.push_back("openivm_col_" + to_string(idx));
		}
	}
}

static void AppendAggregateOutputNames(LogicalAggregate &aggregate, idx_t output_count, vector<string> &output_names) {
	idx_t group_count = aggregate.groups.size();
	while (output_names.size() < output_count) {
		idx_t idx = output_names.size();
		if (idx >= group_count) {
			idx_t expr_idx = idx - group_count;
			if (expr_idx < aggregate.expressions.size() && !aggregate.expressions[expr_idx]->alias.empty()) {
				output_names.push_back(aggregate.expressions[expr_idx]->alias);
				continue;
			}
		}
		output_names.push_back("openivm_col_" + to_string(idx));
	}
}

static void AppendHiddenOutputNames(LogicalOperator *plan, vector<string> &output_names) {
	auto output_count = plan->GetColumnBindings().size();
	auto *name_source = FindTopNameSource(plan);
	if (!name_source) {
		return;
	}
	if (name_source->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		AppendProjectionOutputNames(name_source->Cast<LogicalProjection>(), output_count, output_names);
	} else {
		AppendAggregateOutputNames(name_source->Cast<LogicalAggregate>(), output_count, output_names);
	}
}

static void DeduplicateOutputNames(vector<string> &output_names) {
	unordered_set<string> seen;
	for (auto &name : output_names) {
		if (IncrementalTableNames::IsInternalColumn(name)) {
			continue;
		}
		string candidate = name;
		idx_t suffix = 1;
		while (seen.count(candidate)) {
			candidate = name + "_" + to_string(suffix++);
		}
		seen.insert(candidate);
		name = candidate;
	}
}

vector<string> PrepareOutputNames(LogicalOperator *select_plan, const vector<string> &planner_names) {
	auto output_names = planner_names;
	for (auto &name : output_names) {
		name = SanitizeOutputName(name);
	}
	AppendHiddenOutputNames(select_plan, output_names);
	DeduplicateOutputNames(output_names);
	return output_names;
}

static bool IsAggregateFunctionUnsupportedByLpts(const string &fn_name) {
	return fn_name == "quantile_cont" || fn_name == "quantile_disc" || fn_name == "percentile_cont" ||
	       fn_name == "percentile_disc" || fn_name == "approx_quantile" || fn_name == "mad" || fn_name == "median" ||
	       fn_name == "mode" ||
	       // Two-argument aggregates whose children LPTS re-aliases to internal
	       // `tN_col` names; the serialized SQL refers to those names against the
	       // original FROM clause and fails binding at CREATE-table time.
	       fn_name == "corr" || fn_name == "covar_pop" || fn_name == "covar_samp" || fn_name == "regr_avgx" ||
	       fn_name == "regr_avgy" || fn_name == "regr_count" || fn_name == "regr_intercept" || fn_name == "regr_r2" ||
	       fn_name == "regr_slope" || fn_name == "regr_sxx" || fn_name == "regr_sxy" || fn_name == "regr_syy" ||
	       fn_name == "arg_min" || fn_name == "arg_max";
}

bool QueryNeedsOriginalSqlForLpts(const string &query) {
	string lower = StringUtil::Lower(query);
	return lower.find("pivot ") != string::npos || lower.find("(pivot ") != string::npos;
}

bool PlanNeedsOriginalSqlForLpts(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PIVOT) {
		return true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto *agg = dynamic_cast<LogicalAggregate *>(op);
		if (agg) {
			for (auto &expr : agg->expressions) {
				if (expr->type != ExpressionType::BOUND_AGGREGATE) {
					continue;
				}
				auto &bound_agg = expr->Cast<BoundAggregateExpression>();
				if (IsAggregateFunctionUnsupportedByLpts(bound_agg.function.name)) {
					return true;
				}
			}
		}
	}
	for (auto &child : op->children) {
		if (PlanNeedsOriginalSqlForLpts(child.get())) {
			return true;
		}
	}
	return false;
}

LogicalAggregate *FindOuterAggregate(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return &op->Cast<LogicalAggregate>();
	}
	for (auto &child : op->children) {
		auto *aggregate = FindOuterAggregate(child.get());
		if (aggregate) {
			return aggregate;
		}
	}
	return nullptr;
}

bool IsPacLoaded(ClientContext &context) {
	Value pac_check_val;
	return context.TryGetCurrentSetting("pac_check", pac_check_val);
}

void ForwardPacSettingsIfLoaded(ClientContext &context, Connection &con) {
	if (!IsPacLoaded(context)) {
		return;
	}
	for (auto &name : {"pac_mi", "pac_seed", "pac_m", "pac_noise", "pac_hash_repair", "pac_check", "pac_rewrite",
	                   "pac_conservative_mode"}) {
		Value val;
		if (context.TryGetCurrentSetting(name, val) && !val.IsNull()) {
			con.Query("SET " + string(name) + " = " + val.ToString());
		}
	}
}

static bool SubtreeContainsComparisonJoin(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return true;
	}
	for (auto &child : op->children) {
		if (SubtreeContainsComparisonJoin(child.get())) {
			return true;
		}
	}
	return false;
}

static void CollectSubtreeTableIndices(LogicalOperator *op, std::set<idx_t> &out) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		out.insert(op->Cast<LogicalGet>().table_index);
	}
	for (auto &child : op->children) {
		CollectSubtreeTableIndices(child.get(), out);
	}
}

// Which base tables read as NULL in a null-padded row of join `oj`.
//
// It is not just oj's own inner side: in a chain c ⟕ o ⟕ l ⟕ s, when a line disappears the supplier
// joined to that line's key disappears too, and anything joined to the supplier after it, and so on.
// So start from oj's inner subtree and close transitively over any join whose condition touches an
// already-NULL table, adding that join's inner side. Aggregates over these tables contribute 0 to the
// null-padded row; aggregates over the preserved side still contribute.
static std::set<idx_t> ComputeNullTablesForLevel(const CreateMVPlanFacts &facts, LogicalComparisonJoin *oj) {
	std::set<idx_t> null_tables;
	if (!oj || oj->children.size() != 2) {
		return null_tables;
	}
	CollectSubtreeTableIndices(oj->children[1].get(), null_tables);
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto *other : facts.comparison_joins) {
			if (other == oj || other->join_type != JoinType::LEFT || other->children.size() != 2) {
				continue;
			}
			std::set<idx_t> other_inner;
			CollectSubtreeTableIndices(other->children[1].get(), other_inner);
			bool already_null = true;
			for (auto idx : other_inner) {
				if (!null_tables.count(idx)) {
					already_null = false;
					break;
				}
			}
			if (already_null || other_inner.empty()) {
				continue;
			}
			bool touches_null = false;
			for (auto &cond : other->conditions) {
				for (auto *side : {cond.left.get(), cond.right.get()}) {
					auto *cref = GetColumnRefThroughCasts(side);
					if (!cref) {
						continue;
					}
					LogicalGet *g = nullptr;
					string c;
					if (ResolveBindingToGetColumn(cref->binding, facts, g, c) && g &&
					    null_tables.count(g->table_index)) {
						touches_null = true;
						break;
					}
				}
				if (touches_null) {
					break;
				}
			}
			if (touches_null) {
				for (auto idx : other_inner) {
					null_tables.insert(idx);
				}
				changed = true;
			}
		}
	}
	return null_tables;
}

// Secondary delta for ONE LEFT JOIN level. `level` indexes the emitted placeholders, and
// `null_tables` is the set of base tables that read NULL in this level's null-padded row (see
// ComputeNullTablesForLevel) which decides each aggregate's contribution.
static string BuildLeftJoinSecondaryForLevel(ClientContext &context, const CreateMVPlanFacts &facts,
                                             const vector<string> &output_names, const string &view_name,
                                             LogicalComparisonJoin *oj, size_t level,
                                             const std::set<idx_t> &null_tables, vector<string> &preserved_cols,
                                             const string &delta_view_catalog_prefix, string *out_inner_table,
                                             string *out_inner_key, string *out_pres_table, string *out_pres_key) {
	preserved_cols.clear();
	if (facts.aggregates.size() != 1) {
		return "";
	}
	if (!oj || oj->join_type != JoinType::LEFT || oj->conditions.empty() || oj->children.size() != 2) {
		return "";
	}
	// Only PIPELINES need the secondary: for a single LEFT JOIN the primary delta already emits the
	// null-padded reappearance. Require the preserved (left) side to itself contain a join.
	if (!SubtreeContainsComparisonJoin(oj->children[0].get())) {
		return "";
	}
	auto &agg = *facts.aggregates[0];
	size_t G = agg.groups.size();
	if (G == 0 || output_names.size() < G + agg.expressions.size()) {
		return "";
	}
	// Deepest-join keys: condition.left = preserved side, condition.right = inner (null) side.
	auto *pres_ref = GetColumnRefThroughCasts(oj->conditions[0].left.get());
	auto *inner_ref = GetColumnRefThroughCasts(oj->conditions[0].right.get());
	if (!pres_ref || !inner_ref) {
		return "";
	}
	ColumnBinding key_pres = pres_ref->binding;
	LogicalGet *inner_get = nullptr;
	string inner_col;
	if (!ResolveBindingToGetColumn(inner_ref->binding, facts, inner_get, inner_col) || !inner_get ||
	    !inner_get->GetTable().get()) {
		return "";
	}
	idx_t inner_tidx = inner_get->table_index;
	string inner_table = inner_get->GetTable().get()->name;
	auto sti = facts.source_table_info.find(inner_table);
	if (sti == facts.source_table_info.end()) {
		return "";
	}
	string cat = sti->second.catalog_name;
	string sch = sti->second.schema_name;
	string qual;
	if (!cat.empty()) {
		qual += KeywordHelper::WriteOptionallyQuoted(cat) + ".";
	}
	if (!sch.empty()) {
		qual += KeywordHelper::WriteOptionallyQuoted(sch) + ".";
	}
	string inner_base = qual + KeywordHelper::WriteOptionallyQuoted(inner_table);
	string delta_inner_bare = SqlUtils::DeltaName(inner_table);
	string delta_inner = qual + KeywordHelper::WriteOptionallyQuoted(delta_inner_bare);
	string qcol = KeywordHelper::WriteOptionallyQuoted(inner_col);

	// The correction below assumes a dangling (null-padded) row for this key is ALREADY materialized
	// in the view -- true only if the preserved-side row (e.g. the order) existed before this refresh
	// batch. If the preserved-side row is itself brand new in this same batch (e.g. a freshly inserted
	// order that immediately gets its first line), no dangling row was ever materialized, and applying
	// this correction would spuriously subtract a row that never existed. Resolve the preserved side's
	// own base table/column so we can gate on "did this key exist before this batch's delta."
	LogicalGet *pres_get = nullptr;
	string pres_col;
	if (!ResolveBindingToGetColumn(key_pres, facts, pres_get, pres_col) || !pres_get || !pres_get->GetTable().get()) {
		return "";
	}
	string pres_table = pres_get->GetTable().get()->name;
	if (pres_get->table_index == inner_tidx || StringUtil::CIEquals(pres_table, inner_table)) {
		// Self-referencing deepest join (preserved and inner sides are the same base table): the
		// old_pres_count/old_count arithmetic below assumes two distinct tables with independent
		// delta tracking. Bail rather than risk generating ambiguous or incorrect SQL for this rare
		// shape.
		return "";
	}
	auto pres_sti = facts.source_table_info.find(pres_table);
	if (pres_sti == facts.source_table_info.end()) {
		return "";
	}
	string pres_qual;
	if (!pres_sti->second.catalog_name.empty()) {
		pres_qual += KeywordHelper::WriteOptionallyQuoted(pres_sti->second.catalog_name) + ".";
	}
	if (!pres_sti->second.schema_name.empty()) {
		pres_qual += KeywordHelper::WriteOptionallyQuoted(pres_sti->second.schema_name) + ".";
	}
	string pres_base = pres_qual + KeywordHelper::WriteOptionallyQuoted(pres_table);
	string delta_pres_bare = SqlUtils::DeltaName(pres_table);
	string delta_pres = pres_qual + KeywordHelper::WriteOptionallyQuoted(delta_pres_bare);
	string pres_qcol = KeywordHelper::WriteOptionallyQuoted(pres_col);

	// Classify each aggregate output column's contribution to a null-padded row of the deepest join.
	// output_names layout: [group cols (G)] [visible aggregates (agg.expressions, in order)] [hidden helpers].
	// Visible: resolve the arg's base table — inner-side count/sum -> 0, preserved-side count -> 1
	// (preserved-side sum needs the value: unsupported for now -> bail). count_star (no arg) -> 1.
	// Hidden: openivm_count_star -> 1, openivm_match_count / openivm_nonnull_sum_count* -> 0 (all inner-derived).
	size_t num_agg = output_names.size() - G;
	vector<string> contribs;
	for (size_t j = 0; j < num_agg; j++) {
		if (j < agg.expressions.size()) {
			auto &e = agg.expressions[j];
			if (e->expression_class != ExpressionClass::BOUND_AGGREGATE) {
				return "";
			}
			auto &ba = e->Cast<BoundAggregateExpression>();
			string fn = StringUtil::Lower(ba.function.name);
			if (ba.children.empty()) {
				contribs.push_back("1"); // count_star: null-padded row still counts
				continue;
			}
			auto *cref = GetColumnRefThroughCasts(ba.children[0].get());
			if (!cref) {
				return "";
			}
			LogicalGet *g = nullptr;
			string c;
			bool resolved = ResolveBindingToGetColumn(cref->binding, facts, g, c);
			bool is_inner = resolved && g && null_tables.count(g->table_index) > 0;
			if (fn == "count") {
				contribs.push_back(is_inner ? "0" : "1");
				if (!is_inner) {
					// Preserved-side COUNT: the MERGE must NOT gate it by the (inner-side) match_count.
					preserved_cols.push_back(output_names[G + j]);
				}
			} else if (fn == "sum") {
				if (is_inner) {
					contribs.push_back("0");
				} else {
					return ""; // preserved-side SUM needs the value; defer to a later generalization
				}
			} else {
				return "";
			}
		} else {
			// Hidden helper columns are internally generated with fixed openivm_* names (never
			// user-chosen), so an exact/prefix match is safe and tighter than a bare substring search.
			const string &col = output_names[G + j];
			if (StringUtil::CIEquals(col, "openivm_count_star")) {
				contribs.push_back("1");
			} else if (StringUtil::CIEquals(col, "openivm_match_count") ||
			           StringUtil::StartsWith(col, "openivm_nonnull_sum_count")) {
				contribs.push_back("0");
			} else {
				return "";
			}
		}
	}

	// Build the preserved-side subquery X via LPTS, naming the join key "__k" and each group col "__g<i>".
	auto x_plan = oj->children[0]->Copy(context);
	x_plan->ResolveOperatorTypes();
	auto x_bindings = x_plan->GetColumnBindings();
	vector<ColumnBinding> group_bindings;
	for (auto &ge : agg.groups) {
		auto *gr = GetColumnRefThroughCasts(ge.get());
		if (!gr) {
			return "";
		}
		group_bindings.push_back(gr->binding);
	}
	vector<string> x_names(x_bindings.size());
	int key_pos = -1;
	vector<int> group_pos(G, -1);
	for (size_t i = 0; i < x_bindings.size(); i++) {
		x_names[i] = "__x" + std::to_string(i);
		if (x_bindings[i] == key_pres) {
			x_names[i] = "__k";
			key_pos = static_cast<int>(i);
		}
		for (size_t gi = 0; gi < G; gi++) {
			if (x_bindings[i] == group_bindings[gi]) {
				x_names[i] = "__g" + std::to_string(gi);
				group_pos[gi] = static_cast<int>(i);
			}
		}
	}
	if (key_pos < 0) {
		return "";
	}
	for (size_t gi = 0; gi < G; gi++) {
		if (group_pos[gi] < 0) {
			return "";
		}
	}
	string x_sql;
	try {
		SqlDialect dialect = SqlDialect::DUCKDB;
		auto ast = LogicalPlanToAst(context, x_plan, dialect);
		auto cte_list = AstToCteList(*ast, dialect);
		x_sql = cte_list->ToQuery(true, x_names);
		if (!x_sql.empty() && x_sql.back() == ';') {
			x_sql.pop_back();
		}
	} catch (...) {
		return "";
	}
	if (x_sql.empty()) {
		return "";
	}

	// delta_mv columns: group cols + aggregate cols + openivm_multiplicity (matches the primary INSERT).
	string col_list;
	string sel;
	for (size_t gi = 0; gi < G; gi++) {
		col_list += KeywordHelper::WriteOptionallyQuoted(output_names[gi]) + ", ";
		sel += "X.\"__g" + std::to_string(gi) + "\", ";
	}
	for (size_t j = 0; j < contribs.size(); j++) {
		col_list += KeywordHelper::WriteOptionallyQuoted(output_names[G + j]) + ", ";
		sel += contribs[j] + ", ";
	}
	col_list += "openivm_multiplicity";
	sel += "CASE WHEN (__newc - __t.dsum) > 0 AND __newc = 0 THEN 1 ELSE -1 END";

	// Must carry the same catalog prefix the delta table was created with. Unqualified works only when
	// the MV's state lives in the default catalog; a DuckLake-backed view's delta table is
	// dl.main.openivm_delta_<view>, and an unqualified INSERT there fails the whole refresh.
	if (out_inner_table) {
		*out_inner_table = inner_table;
	}
	if (out_inner_key) {
		*out_inner_key = inner_col;
	}
	if (out_pres_table) {
		*out_pres_table = pres_table;
	}
	if (out_pres_key) {
		*out_pres_key = pres_col;
	}

	// Must carry the same catalog prefix the delta table was created with. Unqualified works only when
	// the MV's state lives in the default catalog; a DuckLake-backed view's delta table is
	// dl.main.openivm_delta_<view>, and an unqualified INSERT there fails the whole refresh.
	string dmv = delta_view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
	// The two delta row sources are placeholders resolved at refresh: a regular table reads its
	// openivm_delta_<table>, a DuckLake table reads ducklake_table_insertions/deletions between
	// snapshots (whose IDs only exist at refresh time). Both substitutions yield columns (__k, __m).
	string inner_delta_src =
	    string(openivm::LJSEC_INNER_DELTA_PREFIX) + std::to_string(level) + string(openivm::LJSEC_PLACEHOLDER_SUFFIX);
	string pres_delta_src =
	    string(openivm::LJSEC_PRES_DELTA_PREFIX) + std::to_string(level) + string(openivm::LJSEC_PLACEHOLDER_SUFFIX);
	// The correlated LATERAL form is deliberate: rewriting these as pre-aggregated CTEs restricted to
	// the delta keys measured ~40% SLOWER at TPC-H SF50 (DuckDB already decorrelates them well).
	string sql = "INSERT INTO " + dmv + " (" + col_list + ")\nSELECT " + sel +
	             "\nFROM (SELECT __k, SUM(__m) AS dsum FROM " + inner_delta_src + " GROUP BY __k) __t\nJOIN (" + x_sql +
	             ") X ON X.\"__k\" = __t.__k,\nLATERAL (SELECT (SELECT COUNT(*) FROM " + inner_base + " ib WHERE ib." +
	             qcol + " = __t.__k) AS __newc) __n,\nLATERAL (SELECT (SELECT COUNT(*) FROM " + pres_base +
	             " pb WHERE pb." + pres_qcol + " = __t.__k) - COALESCE((SELECT SUM(__m) FROM " + pres_delta_src +
	             " __pd WHERE __pd.__k = __t.__k), 0) AS __old_pres_count) __op"
	             "\nWHERE (__newc > 0) <> ((__newc - __t.dsum) > 0) AND __old_pres_count > 0;";
	return sql;
}

// Emit a secondary delta for EVERY LEFT JOIN level whose preserved side is itself a join.
//
// A chain of N tables has N-1 LEFT JOIN levels, and a disappearing match at ANY of them can strand a
// preserved-side count. Correcting only the outermost level (the previous behaviour) left
// COUNT(intermediate_key) undercounted for 4+ table chains: with c ⟕ o ⟕ l ⟕ s, deleting an order's
// last line dropped that order from COUNT(o.oid) because the `⟕ l` level had no correction.
//
// The per-level SQL fragments are concatenated and the two source identities per level are stored as
// index-aligned CSV lists, so refresh can resolve each level's placeholders independently (a regular
// table reads its delta table, a DuckLake table reads snapshot insertions/deletions).
string BuildLeftJoinSecondaryDeltaSQL(ClientContext &context, const CreateMVPlanFacts &facts,
                                      const vector<string> &output_names, const string &view_name,
                                      vector<string> &preserved_cols, const string &delta_view_catalog_prefix,
                                      string *out_inner_table, string *out_inner_key, string *out_pres_table,
                                      string *out_pres_key) {
	preserved_cols.clear();
	string combined_sql;
	vector<string> inner_tables, inner_keys, pres_tables, pres_keys;
	size_t level = 0;
	for (auto *oj : facts.comparison_joins) {
		if (!oj || oj->join_type != JoinType::LEFT || oj->conditions.empty() || oj->children.size() != 2) {
			continue;
		}
		if (!SubtreeContainsComparisonJoin(oj->children[0].get())) {
			continue; // innermost level: the primary delta already emits its null-padded reappearance
		}
		auto null_tables = ComputeNullTablesForLevel(facts, oj);
		vector<string> level_preserved;
		string it, ik, pt, pk;
		string level_sql =
		    BuildLeftJoinSecondaryForLevel(context, facts, output_names, view_name, oj, level, null_tables,
		                                   level_preserved, delta_view_catalog_prefix, &it, &ik, &pt, &pk);
		if (level_sql.empty()) {
			// An unsupported level makes the whole correction incomplete, and a partial correction is
			// worse than none (it would double-count the levels it does cover). Bail entirely.
			preserved_cols.clear();
			return "";
		}
		combined_sql += (combined_sql.empty() ? "" : "\n") + level_sql;
		inner_tables.push_back(it);
		inner_keys.push_back(ik);
		pres_tables.push_back(pt);
		pres_keys.push_back(pk);
		for (auto &c : level_preserved) {
			if (std::find(preserved_cols.begin(), preserved_cols.end(), c) == preserved_cols.end()) {
				preserved_cols.push_back(c);
			}
		}
		level++;
	}
	if (combined_sql.empty()) {
		return "";
	}
	if (out_inner_table) {
		*out_inner_table = StringUtil::Join(inner_tables, ",");
	}
	if (out_inner_key) {
		*out_inner_key = StringUtil::Join(inner_keys, ",");
	}
	if (out_pres_table) {
		*out_pres_table = StringUtil::Join(pres_tables, ",");
	}
	if (out_pres_key) {
		*out_pres_key = StringUtil::Join(pres_keys, ",");
	}
	return combined_sql;
}

} // namespace duckdb
