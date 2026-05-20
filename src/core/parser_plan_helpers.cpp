#include "core/parser_plan_helpers.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
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

namespace duckdb {

/// Build "ORDER BY col1 ASC, col2 DESC LIMIT k [OFFSET n]".
/// Works for both LOGICAL_TOP_N (fused) and separate LOGICAL_ORDER_BY + LOGICAL_LIMIT nodes.
/// output_col_names is the sanitized output column list; BoundColumnRefs are resolved via
/// their column_index into that list.
string BuildTopKSuffix(const vector<BoundOrderByNode> &orders, idx_t limit_val, idx_t offset_val,
                       const vector<string> &output_col_names) {
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
	if (limit_val > 0) {
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

bool BoolSetting(ClientContext &context, const string &name) {
	Value value;
	return context.TryGetCurrentSetting(name, value) && !value.IsNull() && BooleanValue::Get(value);
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

bool HasUnionBeforeAggregate(const LogicalOperator *op, bool seen_agg_above) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		seen_agg_above = true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION && !seen_agg_above) {
		return true;
	}
	for (auto &child : op->children) {
		if (HasUnionBeforeAggregate(child.get(), seen_agg_above)) {
			return true;
		}
	}
	return false;
}

bool HasUnsupportedSetOperation(const LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_EXCEPT) {
		return true;
	}
	for (auto &child : op->children) {
		if (HasUnsupportedSetOperation(child.get())) {
			return true;
		}
	}
	return false;
}

LogicalProjection *FindFirstProjection(LogicalOperator *op) {
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

LogicalComparisonJoin *FindFirstComparisonJoin(LogicalOperator *op) {
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

bool OuterJoinAggregateNeedsRecompute(LogicalOperator *op, idx_t group_index) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
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
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (auto &expr : proj.expressions) {
			if (!IsPurePassThroughExpression(expr.get()) &&
			    ExpressionReferencesNonGroupBinding(expr.get(), group_index)) {
				return true;
			}
		}
	}
	for (auto &child : op->children) {
		if (OuterJoinAggregateNeedsRecompute(child.get(), group_index)) {
			return true;
		}
	}
	return false;
}

static void CountCteRefsUnderJoin(const LogicalOperator *op, bool under_join, std::map<idx_t, int> &cte_ref_count) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op->type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		under_join = true;
	}
	if (under_join && op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte_ref = op->Cast<LogicalCTERef>();
		cte_ref_count[cte_ref.cte_index]++;
	}
	for (auto &child : op->children) {
		CountCteRefsUnderJoin(child.get(), under_join, cte_ref_count);
	}
}

bool HasRepeatedCteRefUnderJoin(const LogicalOperator *plan) {
	std::map<idx_t, int> cte_ref_count;
	CountCteRefsUnderJoin(plan, false, cte_ref_count);
	for (auto &entry : cte_ref_count) {
		if (entry.second >= 2) {
			return true;
		}
	}
	return false;
}

void CollectDuckLakeTables(LogicalOperator *op, const string &current_catalog,
                           unordered_map<string, DuckLakeSourceTableInfo> &dl_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.function.name == "ducklake_scan" && get.function.function_info) {
			auto &info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
			string lc = StringUtil::Lower(info.table_name);
			if (dl_table_info.find(lc) == dl_table_info.end()) {
				// Always pull the catalog from the DuckLakeTableEntry — it's the
				// one the `dl.ORDER_LINE` reference actually resolved to. Falling
				// back to `current_catalog` is wrong for cross-system MVs (native
				// MV reading from dl.*) because `current_catalog` is the physical
				// default, not the DuckLake catalog.
				string cat = info.table.ParentCatalog().GetName();
				if (cat.empty()) {
					if (current_catalog.empty()) {
						throw CatalogException("Could not resolve DuckLake catalog for table '" + info.table_name +
						                       "'");
					}
					cat = current_catalog;
				}
				DuckLakeSourceTableInfo source_info;
				source_info.table_name = info.table_name;
				source_info.catalog_name = cat;
				source_info.schema_name = info.table.schema.name;
				source_info.table_id = static_cast<int64_t>(info.table_id.index);
				dl_table_info[lc] = source_info;
			}
		}
	}
	for (auto &child : op->children) {
		CollectDuckLakeTables(child.get(), current_catalog, dl_table_info);
	}
}

void CollectSourceTables(LogicalOperator *op, unordered_map<string, SourceTableInfo> &source_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto table_ref = get.GetTable();
		if (table_ref.get()) {
			auto &table = *table_ref.get();
			string table_name = table.name;
			if (!table_name.empty() && !SqlUtils::IsDelta(table_name)) {
				source_table_info[table_name] = {table_name, table.ParentCatalog().GetName(), table.schema.name};
			}
		}
	}
	for (auto &child : op->children) {
		CollectSourceTables(child.get(), source_table_info);
	}
}

bool RelationExists(Connection &con, const string &qualified_name) {
	auto result = con.Query("SELECT * FROM " + qualified_name + " LIMIT 0");
	return !result->HasError();
}

struct BindingResolverIndex {
	unordered_map<idx_t, LogicalProjection *> projections_by_index;
	unordered_map<idx_t, LogicalSetOperation *> setops_by_index;
	unordered_map<idx_t, LogicalCTERef *> cte_refs_by_table_index;
	unordered_map<idx_t, LogicalOperator *> cte_defs_by_index;
};

static void CollectBindingResolverIndex(LogicalOperator *op, BindingResolverIndex &index) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		index.projections_by_index[proj.table_index] = &proj;
	} else if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		auto &setop = op->Cast<LogicalSetOperation>();
		index.setops_by_index[setop.table_index] = &setop;
	} else if (op->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &cte_ref = op->Cast<LogicalCTERef>();
		index.cte_refs_by_table_index[cte_ref.table_index] = &cte_ref;
	} else if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && op->children.size() >= 2) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		index.cte_defs_by_index[cte.table_index] = op->children[0].get();
	}
	for (auto &child : op->children) {
		CollectBindingResolverIndex(child.get(), index);
	}
}

static bool ResolvesToGroupBinding(idx_t table_index, idx_t column_index, idx_t group_index, size_t group_count,
                                   const BindingResolverIndex &index, int depth = 0) {
	if (depth > 16) {
		return false;
	}
	if (table_index == group_index) {
		return column_index < static_cast<idx_t>(group_count);
	}
	auto projection_it = index.projections_by_index.find(table_index);
	if (projection_it != index.projections_by_index.end()) {
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
		                              index, depth + 1);
	}
	auto setop_it = index.setops_by_index.find(table_index);
	if (setop_it != index.setops_by_index.end()) {
		// UNION outputs keep branch columns by position; follow that binding instead of
		// guessing that the first N final outputs are the group keys.
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
			if (ResolvesToGroupBinding(binding.table_index, binding.column_index, group_index, group_count, index,
			                           depth + 1)) {
				return true;
			}
		}
		return false;
	}
	auto cte_ref_it = index.cte_refs_by_table_index.find(table_index);
	if (cte_ref_it != index.cte_refs_by_table_index.end()) {
		auto &cte_ref = *cte_ref_it->second;
		auto cte_def_it = index.cte_defs_by_index.find(cte_ref.cte_index);
		if (cte_def_it == index.cte_defs_by_index.end()) {
			return false;
		}
		auto bindings = cte_def_it->second->GetColumnBindings();
		if (column_index >= bindings.size()) {
			return false;
		}
		auto &binding = bindings[column_index];
		return ResolvesToGroupBinding(binding.table_index, binding.column_index, group_index, group_count, index,
		                              depth + 1);
	}
	return false;
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

static bool AddGroupColumnsFromProjection(LogicalProjection &proj, const BindingResolverIndex &index, idx_t group_index,
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
		                            index)) {
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

static bool AddGroupColumnsFromBindings(LogicalOperator &op, const BindingResolverIndex &index, idx_t group_index,
                                        size_t group_count, const vector<string> &output_names,
                                        vector<string> &group_names) {
	bool matched = false;
	auto bindings = op.GetColumnBindings();
	for (idx_t col_idx = 0; col_idx < bindings.size() && col_idx < output_names.size(); col_idx++) {
		auto &binding = bindings[col_idx];
		if (!ResolvesToGroupBinding(binding.table_index, binding.column_index, group_index, group_count, index)) {
			continue;
		}
		if (!output_names[col_idx].empty() && !IncrementalTableNames::IsInternalColumn(output_names[col_idx])) {
			group_names.push_back(output_names[col_idx]);
			matched = true;
		}
	}
	return matched;
}

static bool FindGroupColumns(LogicalOperator *op, const BindingResolverIndex &index, idx_t group_index,
                             size_t group_count, const vector<string> &output_names, vector<string> &group_names) {
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		return AddGroupColumnsFromProjection(proj, index, group_index, group_count, output_names, group_names);
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION) {
		if (AddGroupColumnsFromBindings(*op, index, group_index, group_count, output_names, group_names)) {
			return true;
		}
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		if (op->children.size() >= 2) {
			if (FindGroupColumns(op->children[1].get(), index, group_index, group_count, output_names, group_names)) {
				return true;
			}
			return FindGroupColumns(op->children[0].get(), index, group_index, group_count, output_names, group_names);
		}
		return false;
	}
	for (auto &child : op->children) {
		if (FindGroupColumns(child.get(), index, group_index, group_count, output_names, group_names)) {
			return true;
		}
	}
	return false;
}

vector<string> DeriveGroupColumnNames(LogicalOperator *plan, idx_t group_index, size_t group_count,
                                      const vector<string> &output_names) {
	vector<string> group_names;
	BindingResolverIndex index;
	CollectBindingResolverIndex(plan, index);
	if (AddGroupColumnsFromBindings(*plan, index, group_index, group_count, output_names, group_names)) {
		return group_names;
	}
	FindGroupColumns(plan, index, group_index, group_count, output_names, group_names);
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

vector<string> DeriveScalarDelimKeyColumnNames(LogicalOperator *plan, const vector<string> &output_names) {
	vector<string> group_names;
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
			if (!output_names[expr_idx].empty() && !IncrementalTableNames::IsInternalColumn(output_names[expr_idx])) {
				AddUniqueGroupNames(group_names, vector<string> {output_names[expr_idx]});
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

static void CollectAggregateGroupColumns(LogicalOperator *root, LogicalOperator *op, const BindingResolverIndex &index,
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
			if (FindGroupColumns(root, index, agg.group_index, agg.groups.size(), output_names, nested_names)) {
				AddUniqueGroupNames(group_names, nested_names);
			}
		}
	}
	for (auto &child : op->children) {
		CollectAggregateGroupColumns(root, child.get(), index, output_names, group_names, include_first_aggregate,
		                             seen_aggregate);
	}
}

vector<string> DeriveAggregateGroupColumnNames(LogicalOperator *plan, const vector<string> &output_names,
                                               bool include_first_aggregate) {
	vector<string> group_names;
	BindingResolverIndex index;
	CollectBindingResolverIndex(plan, index);
	bool seen_aggregate = false;
	CollectAggregateGroupColumns(plan, plan, index, output_names, group_names, include_first_aggregate, seen_aggregate);
	return group_names;
}

static string FindFirstTableName(LogicalOperator *node) {
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

struct BaseColumnRef {
	string table;
	string column;
};

struct OccurrenceColumnRef {
	string table;
	idx_t occurrence = 0;
	string column;
};

struct ProjectionSourceOccurrence {
	string table;
	idx_t occurrence = 0;
	idx_t table_index = 0;
};

struct ProjectionLineageEdge {
	OccurrenceColumnRef from;
	OccurrenceColumnRef to;
};

struct ProjectionLineageStep {
	string table;
	idx_t occurrence = 0;
	string lookup_col;
	string lookup_out;
};

struct ProjectionLineageArm {
	string source_table;
	idx_t source_occurrence = 0;
	string source_col;
	vector<ProjectionLineageStep> steps;
};

static string OccurrenceKey(const string &table, idx_t occurrence) {
	return StringUtil::Lower(table) + "#" + to_string(occurrence);
}

static string OccurrenceKey(const OccurrenceColumnRef &ref) {
	return OccurrenceKey(ref.table, ref.occurrence);
}

static bool ResolveBindingToBaseRef(ColumnBinding binding,
                                    const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                    const unordered_map<idx_t, LogicalGet *> &get_by_index, BaseColumnRef &out) {
	idx_t table_index = binding.table_index;
	idx_t column_index = binding.column_index;
	for (int depth = 0; depth < 16; depth++) {
		auto get_it = get_by_index.find(table_index);
		if (get_it != get_by_index.end()) {
			auto *get = get_it->second;
			if (get->GetTable().get()) {
				out.table = get->GetTable().get()->name;
				auto &ids = get->GetColumnIds();
				if (column_index < ids.size()) {
					auto base_idx = ids[column_index].GetPrimaryIndex();
					auto &cols = get->GetTable().get()->GetColumns();
					if (base_idx < cols.LogicalColumnCount()) {
						out.column = cols.GetColumn(LogicalIndex(base_idx)).Name();
						return true;
					}
				}
			}
			if (column_index < get->names.size()) {
				out.column = get->names[column_index];
				return !out.table.empty();
			}
			return false;
		}
		auto proj_it = proj_by_index.find(table_index);
		if (proj_it == proj_by_index.end()) {
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

static void CollectProjectionSourceOccurrences(LogicalOperator *op,
                                               unordered_map<idx_t, ProjectionSourceOccurrence> &occurrence_by_index,
                                               vector<ProjectionSourceOccurrence> &occurrences,
                                               unordered_map<string, idx_t> &next_occurrence) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.GetTable().get()) {
			string table = get.GetTable().get()->name;
			idx_t occurrence = next_occurrence[StringUtil::Lower(table)]++;
			ProjectionSourceOccurrence source;
			source.table = table;
			source.occurrence = occurrence;
			source.table_index = get.table_index;
			occurrence_by_index[get.table_index] = source;
			occurrences.push_back(std::move(source));
		}
	}
	for (auto &child : op->children) {
		CollectProjectionSourceOccurrences(child.get(), occurrence_by_index, occurrences, next_occurrence);
	}
}

static bool ResolveBindingToOccurrenceRef(ColumnBinding binding,
                                          const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                          const unordered_map<idx_t, LogicalGet *> &get_by_index,
                                          const unordered_map<idx_t, ProjectionSourceOccurrence> &occurrence_by_index,
                                          OccurrenceColumnRef &out) {
	idx_t table_index = binding.table_index;
	idx_t column_index = binding.column_index;
	for (int depth = 0; depth < 16; depth++) {
		auto get_it = get_by_index.find(table_index);
		if (get_it != get_by_index.end()) {
			auto *get = get_it->second;
			auto occ_it = occurrence_by_index.find(table_index);
			if (occ_it == occurrence_by_index.end()) {
				return false;
			}
			out.table = occ_it->second.table;
			out.occurrence = occ_it->second.occurrence;
			if (get->GetTable().get()) {
				auto &ids = get->GetColumnIds();
				if (column_index < ids.size()) {
					auto base_idx = ids[column_index].GetPrimaryIndex();
					auto &cols = get->GetTable().get()->GetColumns();
					if (base_idx < cols.LogicalColumnCount()) {
						out.column = cols.GetColumn(LogicalIndex(base_idx)).Name();
						return true;
					}
				}
			}
			if (column_index < get->names.size()) {
				out.column = get->names[column_index];
				return true;
			}
			return false;
		}
		auto proj_it = proj_by_index.find(table_index);
		if (proj_it == proj_by_index.end()) {
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

static void CollectProjectionJoinEdges(LogicalOperator *op,
                                       const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                       const unordered_map<idx_t, LogicalGet *> &get_by_index,
                                       const unordered_map<idx_t, ProjectionSourceOccurrence> &occurrence_by_index,
                                       vector<ProjectionLineageEdge> &edges) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
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
				if (ResolveBindingToOccurrenceRef(left->binding, proj_by_index, get_by_index, occurrence_by_index,
				                                  lref) &&
				    ResolveBindingToOccurrenceRef(right->binding, proj_by_index, get_by_index, occurrence_by_index,
				                                  rref)) {
					edges.push_back({lref, rref});
					edges.push_back({rref, lref});
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectProjectionJoinEdges(child.get(), proj_by_index, get_by_index, occurrence_by_index, edges);
	}
}

static bool FindProjectionPath(const ProjectionSourceOccurrence &source, const OccurrenceColumnRef &key_ref,
                               const vector<ProjectionLineageEdge> &edges, vector<ProjectionLineageEdge> &path) {
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
		for (auto &edge : edges) {
			if (OccurrenceKey(edge.from) != node) {
				continue;
			}
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
	arm.source_table = source.table;
	arm.source_occurrence = source.occurrence;
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

static string ProjectionLineageStepString(const ProjectionLineageStep &step) {
	return step.table + "|" + to_string(step.occurrence) + "|" + step.lookup_col + "|" + step.lookup_out;
}

static string ProjectionLineageArmJson(const ProjectionLineageArm &arm) {
	string json = "{\"source\":" + SqlUtils::JsonQuote(arm.source_table) +
	              ",\"occ\":" + SqlUtils::JsonQuote(to_string(arm.source_occurrence)) +
	              ",\"source_col\":" + SqlUtils::JsonQuote(arm.source_col) + ",\"steps\":[";
	for (idx_t i = 0; i < arm.steps.size(); i++) {
		if (i > 0) {
			json += ",";
		}
		json += SqlUtils::JsonQuote(ProjectionLineageStepString(arm.steps[i]));
	}
	json += "]}";
	return json;
}

string BuildRefreshLineageJson(const vector<string> &entries) {
	if (entries.empty()) {
		return "";
	}
	return "{\"v\":1,\"items\":[" +
	       StringUtil::Join(entries, entries.size(), ",", [](const string &entry) { return entry; }) + "]}";
}

string BuildProjectionKeyLineageEntryJson(LogicalOperator *plan, const vector<string> &output_names) {
	if (!plan) {
		return "";
	}
	auto *top_projection = FindFirstProjection(plan);
	if (!top_projection) {
		return "";
	}

	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);

	unordered_map<idx_t, ProjectionSourceOccurrence> occurrence_by_index;
	vector<ProjectionSourceOccurrence> occurrences;
	unordered_map<string, idx_t> next_occurrence;
	CollectProjectionSourceOccurrences(plan, occurrence_by_index, occurrences, next_occurrence);
	if (occurrences.size() < 3) {
		return "";
	}

	vector<ProjectionLineageEdge> edges;
	CollectProjectionJoinEdges(plan, proj_by_index, get_by_index, occurrence_by_index, edges);
	if (edges.empty()) {
		return "";
	}

	for (idx_t expr_i = 0; expr_i < top_projection->expressions.size(); expr_i++) {
		auto &expr = top_projection->expressions[expr_i];
		auto *bcr = GetColumnRefThroughCasts(expr.get());
		if (!bcr) {
			continue;
		}
		OccurrenceColumnRef key_ref;
		if (!ResolveBindingToOccurrenceRef(bcr->binding, proj_by_index, get_by_index, occurrence_by_index, key_ref)) {
			continue;
		}
		string output_col = ProjectionOutputName(expr, expr_i, output_names, *bcr);
		if (output_col.empty() || IncrementalTableNames::IsInternalColumn(output_col)) {
			continue;
		}

		vector<ProjectionLineageArm> arms;
		bool covered = true;
		for (auto &occurrence : occurrences) {
			vector<ProjectionLineageEdge> path;
			if (!FindProjectionPath(occurrence, key_ref, edges, path)) {
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

		string json = "{\"k\":\"projection_key\",\"out\":" + SqlUtils::JsonQuote(output_col) +
		              ",\"key_source\":" + SqlUtils::JsonQuote(key_ref.table) +
		              ",\"key_occ\":" + SqlUtils::JsonQuote(to_string(key_ref.occurrence)) +
		              ",\"key_col\":" + SqlUtils::JsonQuote(key_ref.column) + ",\"arms\":[";
		for (idx_t i = 0; i < arms.size(); i++) {
			if (i > 0) {
				json += ",";
			}
			json += ProjectionLineageArmJson(arms[i]);
		}
		json += "]}";
		return json;
	}
	return "";
}

struct WindowLineageOp {
	string kind;
	string output_col;
	string source_table;
	string source_col;
	string lookup_table;
	string lookup_col;
	string lookup_output_col;
};

static void CollectInnerJoinEdges(LogicalOperator *op, const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                  const unordered_map<idx_t, LogicalGet *> &get_by_index,
                                  vector<pair<BaseColumnRef, BaseColumnRef>> &edges) {
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		if (join.join_type == JoinType::INNER) {
			for (auto &cond : join.conditions) {
				auto *left = GetColumnRefThroughCasts(cond.left.get());
				auto *right = GetColumnRefThroughCasts(cond.right.get());
				if (!left || !right) {
					continue;
				}
				BaseColumnRef lref, rref;
				if (ResolveBindingToBaseRef(left->binding, proj_by_index, get_by_index, lref) &&
				    ResolveBindingToBaseRef(right->binding, proj_by_index, get_by_index, rref)) {
					edges.emplace_back(std::move(lref), std::move(rref));
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectInnerJoinEdges(child.get(), proj_by_index, get_by_index, edges);
	}
}

static void CollectWindowPartitionRefs(LogicalOperator *op,
                                       const unordered_map<idx_t, LogicalProjection *> &proj_by_index,
                                       const unordered_map<idx_t, LogicalGet *> &get_by_index,
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
				BaseColumnRef base;
				if (!ResolveBindingToBaseRef(bcr->binding, proj_by_index, get_by_index, base)) {
					continue;
				}
				for (auto &stored : partition_columns) {
					auto pos = stored.find('=');
					string output_col = pos == string::npos ? stored : stored.substr(0, pos);
					string source_col = pos == string::npos ? stored : stored.substr(pos + 1);
					if (!StringUtil::CIEquals(source_col, base.column)) {
						continue;
					}
					WindowLineageOp op;
					op.kind = "direct";
					op.output_col = output_col;
					op.source_table = base.table;
					op.source_col = base.column;
					direct_ops.push_back(std::move(op));
				}
			}
		}
	}
	for (auto &child : op->children) {
		CollectWindowPartitionRefs(child.get(), proj_by_index, get_by_index, partition_columns, direct_ops);
	}
}

static bool SameRef(const BaseColumnRef &a, const BaseColumnRef &b) {
	return StringUtil::CIEquals(a.table, b.table) && StringUtil::CIEquals(a.column, b.column);
}

static bool SameOp(const WindowLineageOp &a, const WindowLineageOp &b) {
	return a.kind == b.kind && StringUtil::CIEquals(a.output_col, b.output_col) &&
	       StringUtil::CIEquals(a.source_table, b.source_table) && StringUtil::CIEquals(a.source_col, b.source_col) &&
	       StringUtil::CIEquals(a.lookup_table, b.lookup_table) && StringUtil::CIEquals(a.lookup_col, b.lookup_col) &&
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

static bool HasEquivalentPartitionRef(const vector<WindowLineageOp> &ops, const BaseColumnRef &ref,
                                      const string &output_col) {
	for (auto &op : ops) {
		if (op.kind == "direct" && StringUtil::CIEquals(op.output_col, output_col) &&
		    StringUtil::CIEquals(op.source_table, ref.table) && StringUtil::CIEquals(op.source_col, ref.column)) {
			return true;
		}
	}
	return false;
}

string BuildWindowPartitionLineageEntryJson(LogicalOperator *plan, const vector<string> &partition_columns) {
	if (!plan || partition_columns.empty()) {
		return "";
	}
	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);

	vector<WindowLineageOp> direct_ops;
	CollectWindowPartitionRefs(plan, proj_by_index, get_by_index, partition_columns, direct_ops);
	if (direct_ops.empty()) {
		return "";
	}

	vector<pair<BaseColumnRef, BaseColumnRef>> edges;
	CollectInnerJoinEdges(plan, proj_by_index, get_by_index, edges);

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
				BaseColumnRef direct_ref {direct.source_table, direct.source_col};
				BaseColumnRef other;
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

	for (auto &edge : edges) {
		for (auto &direct : partition_ops) {
			BaseColumnRef lookup_output {direct.source_table, direct.source_col};
			BaseColumnRef lookup_join;
			BaseColumnRef changed_join;
			bool found = false;
			if (StringUtil::CIEquals(edge.first.table, direct.source_table) && !SameRef(edge.first, lookup_output)) {
				lookup_join = edge.first;
				changed_join = edge.second;
				found = true;
			} else if (StringUtil::CIEquals(edge.second.table, direct.source_table) &&
			           !SameRef(edge.second, lookup_output)) {
				lookup_join = edge.second;
				changed_join = edge.first;
				found = true;
			}
			if (!found) {
				continue;
			}
			WindowLineageOp lookup;
			lookup.kind = "lookup";
			lookup.output_col = direct.output_col;
			lookup.source_table = changed_join.table;
			lookup.source_col = changed_join.column;
			lookup.lookup_table = direct.source_table;
			lookup.lookup_col = lookup_join.column;
			lookup.lookup_output_col = direct.source_col;
			AddUniqueLineageOp(ops, std::move(lookup));
		}
	}

	string json = "{\"k\":\"window_partition\",\"ops\":[";
	for (idx_t i = 0; i < ops.size(); i++) {
		if (i > 0) {
			json += ",";
		}
		auto &op = ops[i];
		json += "{\"k\":" + SqlUtils::JsonQuote(op.kind) + ",\"out\":" + SqlUtils::JsonQuote(op.output_col) +
		        ",\"source\":" + SqlUtils::JsonQuote(op.source_table) +
		        ",\"source_col\":" + SqlUtils::JsonQuote(op.source_col);
		if (op.kind == "lookup") {
			json += ",\"lookup\":" + SqlUtils::JsonQuote(op.lookup_table) +
			        ",\"lookup_col\":" + SqlUtils::JsonQuote(op.lookup_col) +
			        ",\"lookup_out\":" + SqlUtils::JsonQuote(op.lookup_output_col);
		}
		json += "}";
	}
	json += "]}";
	return json;
}

void ResolveWindowPartitionOutputNames(LogicalOperator *plan, vector<string> &partition_columns,
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
			if (!output_col.empty() && !IncrementalTableNames::IsInternalColumn(output_col)) {
				partition_column = output_col + "=" + partition_column;
			}
			break;
		}
	}
}

void ResolveAggregateGroupColumnsThroughJoinKeys(LogicalOperator *plan, vector<string> &aggregate_columns,
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

string ExtractFullOuterJoinMetadata(LogicalOperator *plan) {
	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);
	string result;
	ExtractFullOuterJoinCols(plan, proj_by_index, get_by_index, result);
	return result;
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

static void SanitizeOutputNames(vector<string> &output_names) {
	for (auto &name : output_names) {
		name = SanitizeOutputName(name);
	}
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

static void AppendHiddenOutputTypes(LogicalOperator *plan, vector<LogicalType> &output_types) {
	auto output_count = plan->GetColumnBindings().size();
	auto *type_source = FindTopNameSource(plan);
	if (!type_source) {
		return;
	}
	if (type_source->types.size() == output_count) {
		while (output_types.size() < output_count) {
			output_types.push_back(type_source->types[output_types.size()]);
		}
		return;
	}
	if (type_source->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = type_source->Cast<LogicalProjection>();
		while (output_types.size() < output_count && output_types.size() < projection.expressions.size()) {
			output_types.push_back(projection.expressions[output_types.size()]->return_type);
		}
		return;
	}
	auto &aggregate = type_source->Cast<LogicalAggregate>();
	idx_t group_count = aggregate.groups.size();
	while (output_types.size() < output_count) {
		idx_t idx = output_types.size();
		if (idx < group_count) {
			output_types.push_back(aggregate.groups[idx]->return_type);
		} else {
			idx_t expr_idx = idx - group_count;
			if (expr_idx >= aggregate.expressions.size()) {
				break;
			}
			output_types.push_back(aggregate.expressions[expr_idx]->return_type);
		}
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
	SanitizeOutputNames(output_names);
	AppendHiddenOutputNames(select_plan, output_names);
	DeduplicateOutputNames(output_names);
	return output_names;
}

vector<LogicalType> PrepareOutputTypes(LogicalOperator *select_plan, const vector<LogicalType> &planner_types) {
	auto output_types = planner_types;
	AppendHiddenOutputTypes(select_plan, output_types);
	return output_types;
}

bool QueryHasUnsupportedIncrementalConstruct(const string &query) {
	string lower = StringUtil::Lower(query);
	return lower.find("pivot ") != string::npos || lower.find("(pivot ") != string::npos;
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
