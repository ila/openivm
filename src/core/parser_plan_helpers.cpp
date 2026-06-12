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
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"

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

static bool HasSourcePushdownBarrier(const PlanAnalysis &analysis) {
	return analysis.found_aggregation || analysis.found_having || analysis.found_distinct ||
	       analysis.found_union_distinct || analysis.found_outer_join_under_setop || analysis.found_semi_anti_join ||
	       analysis.found_asof_join || analysis.found_positional_join || analysis.found_sample ||
	       analysis.found_window || analysis.found_top_k || analysis.found_count_distinct ||
	       analysis.found_grouping_sets || analysis.found_nested_aggregate || analysis.found_cte_nonlinear_construct ||
	       analysis.found_unsupported_order_by || analysis.found_unsupported_operator;
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

CreateMVPlanFacts BuildCreateMVPlanFacts(LogicalOperator *plan, const string &current_catalog) {
	CreateMVPlanFacts facts;
	facts.root = plan;
	facts.analysis = AnalyzePlan(plan);
	unordered_map<string, idx_t> next_occurrence;
	CollectCreateMVPlanFacts(plan, current_catalog, facts, next_occurrence, false, false);
	AddJoinEdgesFromFacts(facts);
	return facts;
}

bool PlanContainsBoundAggregateFilter(LogicalOperator *plan) {
	if (!plan) {
		return false;
	}
	if (plan->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = plan->Cast<LogicalAggregate>();
		for (auto &expr : aggregate.expressions) {
			if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE &&
			    expr->Cast<BoundAggregateExpression>().filter) {
				return true;
			}
		}
	}
	for (auto &child : plan->children) {
		if (PlanContainsBoundAggregateFilter(child.get())) {
			return true;
		}
	}
	return false;
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

// Single-pass replacement for the former PlanContainsAggregateFilter / PlanHasHiddenMinMaxHavingColumn /
// PlanHasComputedMinMaxAggregateProjection probes, which each walked the plan independently (and built the
// aggregate-index map twice). One traversal collects the aggregates by index, flags FILTER-over-AGGREGATE,
// and buckets the projection expressions the MIN/MAX checks need. Projections sit above their aggregates, so
// the binding lookups run in a cheap post-pass once the aggregate map is complete (no extra tree walk).
CreateMvAggregateProbes AnalyzeCreateMvAggregateProbes(LogicalOperator *plan) {
	CreateMvAggregateProbes probes;
	if (!plan) {
		return probes;
	}

	unordered_map<idx_t, LogicalAggregate *> aggregates;
	vector<BoundColumnRefExpression *> hidden_having_refs;
	vector<Expression *> computed_candidate_exprs;

	std::function<void(LogicalOperator *)> walk;
	walk = [&](LogicalOperator *node) {
		if (!node) {
			return;
		}
		switch (node->type) {
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &aggregate = node->Cast<LogicalAggregate>();
			aggregates[aggregate.aggregate_index] = &aggregate;
			break;
		}
		case LogicalOperatorType::LOGICAL_FILTER:
			if (!node->children.empty() &&
			    node->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				probes.has_aggregate_filter = true;
			}
			break;
		case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto &projection = node->Cast<LogicalProjection>();
			for (auto &expr : projection.expressions) {
				if (expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					if (IsHiddenHavingColumn(expr->alias)) {
						hidden_having_refs.push_back(&expr->Cast<BoundColumnRefExpression>());
					}
				} else {
					computed_candidate_exprs.push_back(expr.get());
				}
			}
			break;
		}
		default:
			break;
		}
		for (auto &child : node->children) {
			walk(child.get());
		}
	};
	walk(plan);

	// MIN/MAX checks require the aggregate map; the FILTER-over-AGGREGATE flag stands on its own.
	if (aggregates.empty()) {
		return probes;
	}

	for (auto *column_ref : hidden_having_refs) {
		if (IsMinMaxAggregateColumn(*column_ref, aggregates)) {
			probes.has_hidden_minmax_having = true;
			break;
		}
	}
	for (auto *expr : computed_candidate_exprs) {
		if (ExpressionReferencesMinMaxAggregate(*expr, aggregates)) {
			probes.has_computed_minmax_aggregate_projection = true;
			break;
		}
	}

	return probes;
}

static bool IsCountStarAggregate(const Expression &expr) {
	if (expr.expression_class != ExpressionClass::BOUND_AGGREGATE) {
		return false;
	}
	auto &agg = expr.Cast<BoundAggregateExpression>();
	return agg.function.name == "count_star" || (agg.function.name == "count" && agg.children.empty());
}

// Last identifier part of a bound expression's rendered form — mirrors how the former string extractor
// derived the source column name (e.g. "se_fgc.wid" / `"wid"` → "wid").
static string PlanExpressionColumnName(const Expression &expr) {
	return SqlUtils::LastIdentifierPart(expr.ToString());
}

// True iff `expr` is a numeric zero constant, looking through CAST wrappers (e.g. CAST(0 AS DOUBLE)).
static bool IsZeroConstantExpression(const Expression &expr) {
	const Expression *e = &expr;
	while (e->expression_class == ExpressionClass::BOUND_CAST) {
		e = e->Cast<BoundCastExpression>().child.get();
	}
	if (e->expression_class != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}
	auto &value = e->Cast<BoundConstantExpression>().value;
	if (value.IsNull() || !value.type().IsNumeric()) {
		return false;
	}
	return value.GetValue<double>() == 0.0;
}

// Follow `binding` down through the projections sitting between the filter and the inner aggregate,
// remapping it whenever it points at a projection slot that is a plain column reference. Returns true
// if it ultimately lands on the inner aggregate's aggregate output (i.e. the SUM, not the group key).
static bool ColumnRefResolvesToAggregateOutput(ColumnBinding binding, const vector<LogicalProjection *> &projections,
                                               idx_t aggregate_index) {
	for (size_t guard = 0; guard <= projections.size(); guard++) {
		if (binding.table_index == aggregate_index) {
			return true;
		}
		bool advanced = false;
		for (auto *projection : projections) {
			if (projection->table_index != binding.table_index) {
				continue;
			}
			if (binding.column_index >= projection->expressions.size()) {
				return false;
			}
			auto &slot = projection->expressions[binding.column_index];
			if (slot->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			binding = slot->Cast<BoundColumnRefExpression>().binding;
			advanced = true;
			break;
		}
		if (!advanced) {
			break;
		}
	}
	return binding.table_index == aggregate_index;
}

bool RecognizeFilteredGroupCount(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                                 FilteredGroupCountExtract &out) {
	if (output_names.size() != 1 || output_names[0].empty()) {
		return false;
	}
	if (facts.aggregates.size() != 2) {
		return false;
	}

	// Identify the outer COUNT(*) aggregate (no groups) and the inner single-group / single-SUM
	// aggregate by role, so we don't depend on traversal order.
	LogicalAggregate *outer = nullptr;
	LogicalAggregate *inner = nullptr;
	for (auto *agg : facts.aggregates) {
		if (agg->groups.empty() && agg->expressions.size() == 1 && IsCountStarAggregate(*agg->expressions[0])) {
			outer = agg;
		} else if (agg->groups.size() == 1 && agg->expressions.size() == 1 &&
		           agg->expressions[0]->expression_class == ExpressionClass::BOUND_AGGREGATE &&
		           agg->expressions[0]->Cast<BoundAggregateExpression>().function.name == "sum") {
			inner = agg;
		}
	}
	if (!outer || !inner) {
		return false;
	}

	auto &sum_agg = inner->expressions[0]->Cast<BoundAggregateExpression>();
	if (sum_agg.children.size() != 1 || sum_agg.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	string group_col = PlanExpressionColumnName(*inner->groups[0]);
	string sum_col = PlanExpressionColumnName(*sum_agg.children[0]);
	if (group_col.empty() || sum_col.empty()) {
		return false;
	}

	// The inner aggregate must read a single base table (no joins / set operations underneath).
	LogicalGet *get = nullptr;
	for (LogicalOperator *node = inner->children.empty() ? nullptr : inner->children[0].get(); node;) {
		if (node->type == LogicalOperatorType::LOGICAL_GET) {
			get = &node->Cast<LogicalGet>();
			break;
		}
		if (node->children.size() != 1) {
			break;
		}
		node = node->children[0].get();
	}
	if (!get || !get->GetTable()) {
		return false;
	}

	// Walk from the outer aggregate down to the inner one: the path must hold exactly one FILTER
	// (the `<sum> </> 0` predicate) and only projections otherwise.
	LogicalFilter *filter = nullptr;
	vector<LogicalProjection *> path_projections;
	bool reached_inner = false;
	for (LogicalOperator *node = outer->children.empty() ? nullptr : outer->children[0].get(); node;) {
		if (node == inner) {
			reached_inner = true;
			break;
		}
		if (node->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (filter) {
				return false;
			}
			filter = &node->Cast<LogicalFilter>();
		} else if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			path_projections.push_back(&node->Cast<LogicalProjection>());
		} else {
			return false;
		}
		if (node->children.size() != 1) {
			return false;
		}
		node = node->children[0].get();
	}
	if (!reached_inner || !filter || filter->expressions.size() != 1) {
		return false;
	}

	auto &condition = *filter->expressions[0];
	if (condition.expression_class != ExpressionClass::BOUND_COMPARISON) {
		return false;
	}
	auto &cmp = condition.Cast<BoundComparisonExpression>();
	string op;
	if (cmp.type == ExpressionType::COMPARE_LESSTHAN) {
		op = "<";
	} else if (cmp.type == ExpressionType::COMPARE_GREATERTHAN) {
		op = ">";
	} else {
		return false;
	}

	// One side is the SUM column reference, the other a zero constant. If the SUM is on the right
	// (0 < sum), flip the operator so the stored form is always `<sum> <op> 0`.
	Expression *col_side = nullptr;
	bool sum_on_left = false;
	if (cmp.left->expression_class == ExpressionClass::BOUND_COLUMN_REF && IsZeroConstantExpression(*cmp.right)) {
		col_side = cmp.left.get();
		sum_on_left = true;
	} else if (cmp.right->expression_class == ExpressionClass::BOUND_COLUMN_REF &&
	           IsZeroConstantExpression(*cmp.left)) {
		col_side = cmp.right.get();
	} else {
		return false;
	}
	if (!ColumnRefResolvesToAggregateOutput(col_side->Cast<BoundColumnRefExpression>().binding, path_projections,
	                                        inner->aggregate_index)) {
		return false;
	}
	if (!sum_on_left) {
		op = (op == "<") ? ">" : "<";
	}

	out = FilteredGroupCountExtract {};
	out.source = get->GetTable()->name;
	out.group_col = group_col;
	out.sum_col = sum_col;
	out.sum_alias = string();
	out.output_col = output_names[0];
	out.comparison_op = op;
	out.threshold_sql = "0";
	return true;
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
		lineage.pushdown_safe = !facts.analysis.found_left_join && !facts.analysis.found_full_outer &&
		                        !HasSourcePushdownBarrier(facts.analysis);
		lineage.arms = std::move(arms);
		out = std::move(lineage);
		return true;
	}
	return false;
}

// First outer join in pre-order — matches ScanOuterJoins, which is the join RewriteLeftJoinKey traces
// openivm_left_key from. Returns null if there is none.
static LogicalComparisonJoin *FindFirstOuterJoin(LogicalOperator *node) {
	if (!node) {
		return nullptr;
	}
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = node->Cast<LogicalComparisonJoin>();
		if ((join.join_type == JoinType::LEFT || join.join_type == JoinType::RIGHT) && !join.conditions.empty()) {
			return &join;
		}
	}
	for (auto &child : node->children) {
		if (auto *found = FindFirstOuterJoin(child.get())) {
			return found;
		}
	}
	return nullptr;
}

static idx_t CountLeftRightOuterJoins(LogicalOperator *node) {
	if (!node) {
		return 0;
	}
	idx_t count = 0;
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = node->Cast<LogicalComparisonJoin>();
		if ((join.join_type == JoinType::LEFT || join.join_type == JoinType::RIGHT) && !join.conditions.empty()) {
			count++;
		}
	}
	for (auto &child : node->children) {
		count += CountLeftRightOuterJoins(child.get());
	}
	return count;
}

static bool IsLeftJoinPushdownSafe(const CreateMVPlanFacts &facts) {
	const auto &analysis = facts.analysis;
	if (CountLeftRightOuterJoins(facts.root) != 1) {
		return false;
	}
	return analysis.found_left_join && !analysis.found_full_outer && !HasSourcePushdownBarrier(analysis);
}

bool BuildLeftJoinKeyLineage(const CreateMVPlanFacts &facts, RefreshMetadata::ProjectionKeyLineage &out) {
	auto *outer = FindFirstOuterJoin(facts.root);
	if (!outer) {
		return false;
	}
	// Preserved side carries the surviving key (LEFT keeps left, RIGHT keeps right).
	auto &condition = outer->conditions[0];
	auto &preserved = outer->join_type == JoinType::RIGHT ? condition.right : condition.left;
	auto *bcr = GetColumnRefThroughCasts(preserved.get());
	if (!bcr) {
		return false; // derived / non-column key — fall back to full recompute
	}
	OccurrenceColumnRef ref;
	if (!ResolveBindingToOccurrenceRef(bcr->binding, facts, ref)) {
		return false; // could not pin the key to a single source binding
	}
	RefreshMetadata::ProjectionKeyLineage lineage;
	lineage.output_col = string(openivm::LEFT_KEY_COL);
	lineage.key_source = ref.table;
	lineage.key_occurrence = ref.occurrence;
	lineage.key_col = ref.column;
	lineage.pushdown_safe = IsLeftJoinPushdownSafe(facts);
	RefreshMetadata::ProjectionKeyLineageArm arm;
	arm.source = ref.table;
	arm.occurrence = ref.occurrence;
	arm.source_col = ref.column;
	lineage.arms.push_back(std::move(arm));
	out = std::move(lineage);
	return true;
}

struct WindowLineageOp {
	string kind;
	string output_col;
	string source_table;
	idx_t source_occurrence = 0;
	string source_col;
	string lookup_table;
	idx_t lookup_occurrence = 0;
	string lookup_col;
	string lookup_output_col;
};

struct WindowLookupEdge {
	OccurrenceColumnRef lookup_join;
	OccurrenceColumnRef changed_join;
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
                                            vector<pair<OccurrenceColumnRef, OccurrenceColumnRef>> &edges) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			for (auto &cond : join.conditions) {
				if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				auto *left = GetColumnRefThroughCasts(cond.left.get());
				auto *right = GetColumnRefThroughCasts(cond.right.get());
				if (!left || !right) {
					continue;
				}
				OccurrenceColumnRef lref, rref;
				if (ResolveBindingToOccurrenceRef(left->binding, facts, lref) &&
				    ResolveBindingToOccurrenceRef(right->binding, facts, rref)) {
					edges.emplace_back(std::move(lref), std::move(rref));
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectInnerJoinEdgesOccurrence(child.get(), facts, edges);
	}
}

static void CollectWindowLookupEdges(LogicalOperator *op, const CreateMVPlanFacts &facts,
                                     vector<WindowLookupEdge> &edges, bool &has_unsafe_lookup_key) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_ASOF_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		auto add_lookup_edge = [&](OccurrenceColumnRef lookup_ref, OccurrenceColumnRef changed_ref) {
			edges.push_back({std::move(lookup_ref), std::move(changed_ref)});
		};
		for (auto &cond : join.conditions) {
			if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			auto *left = GetColumnRefThroughCasts(cond.left.get());
			auto *right = GetColumnRefThroughCasts(cond.right.get());
			if (!left || !right) {
				continue;
			}
			// The affected-keys lookup reproduces this equi-join as a raw `col IN (lookup_col)` probe over the
			// base columns. If the join wrapped the keys in a cast to make incomparable types match (e.g.
			// CAST(cik AS VARCHAR) = company_id, BIGINT vs VARCHAR), that probe would either fail to bind or
			// silently under-count, so mark the lineage unsafe and let the view fall back to a full recompute.
			// Differing-but-numeric types (e.g. INTEGER vs BIGINT) bind and compare by value, so they are safe.
			if (left->return_type != right->return_type &&
			    !(left->return_type.IsNumeric() && right->return_type.IsNumeric())) {
				has_unsafe_lookup_key = true;
			}
			OccurrenceColumnRef lref, rref;
			if (!ResolveBindingToOccurrenceRef(left->binding, facts, lref) ||
			    !ResolveBindingToOccurrenceRef(right->binding, facts, rref)) {
				continue;
			}
			switch (join.join_type) {
			case JoinType::INNER:
				add_lookup_edge(lref, rref);
				add_lookup_edge(rref, lref);
				break;
			case JoinType::LEFT:
				add_lookup_edge(lref, rref);
				add_lookup_edge(rref, lref);
				break;
			case JoinType::RIGHT:
				add_lookup_edge(rref, lref);
				add_lookup_edge(lref, rref);
				break;
			default:
				break;
			}
		}
	}
	for (auto &child : op->children) {
		CollectWindowLookupEdges(child.get(), facts, edges, has_unsafe_lookup_key);
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
				auto *bcr = GetColumnRefThroughCasts(part.get());
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
	       StringUtil::CIEquals(a.source_col, b.source_col) && StringUtil::CIEquals(a.lookup_table, b.lookup_table) &&
	       a.lookup_occurrence == b.lookup_occurrence && StringUtil::CIEquals(a.lookup_col, b.lookup_col) &&
	       StringUtil::CIEquals(a.lookup_output_col, b.lookup_output_col);
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
	metadata_op.lookup = op.lookup_table;
	metadata_op.lookup_col = op.lookup_col;
	metadata_op.lookup_out = op.lookup_output_col;
	return metadata_op;
}

bool BuildWindowPartitionLineageOps(const CreateMVPlanFacts &facts, const vector<string> &partition_columns,
                                    vector<RefreshMetadata::WindowPartitionLineageOp> &out,
                                    vector<RefreshMetadata::WindowPartitionLineageOp> *direct_out,
                                    bool *out_unsafe_lookup) {
	out.clear();
	if (direct_out) {
		direct_out->clear();
	}
	if (out_unsafe_lookup) {
		*out_unsafe_lookup = false;
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

	vector<pair<OccurrenceColumnRef, OccurrenceColumnRef>> edges;
	CollectInnerJoinEdgesOccurrence(plan, facts, edges);
	vector<WindowLookupEdge> lookup_edges;
	bool unsafe_lookup_key = false;
	CollectWindowLookupEdges(plan, facts, lookup_edges, unsafe_lookup_key);
	if (out_unsafe_lookup) {
		*out_unsafe_lookup = unsafe_lookup_key;
	}

	vector<WindowLineageOp> partition_ops;
	for (auto &op : direct_ops) {
		AddUniqueLineageOp(partition_ops, op);
	}

	bool changed = true;
	while (changed) {
		changed = false;
		vector<WindowLineageOp> next_ops = partition_ops;
		for (auto &edge : edges) {
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
			lookup.lookup_table = direct.source_table;
			lookup.lookup_occurrence = direct.source_occurrence;
			lookup.lookup_col = edge.lookup_join.column;
			lookup.lookup_output_col = direct.source_col;
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

} // namespace duckdb
