#include "core/openivm_parser.hpp"

#include "core/ivm_checker.hpp"
#include "core/ivm_plan_rewrite.hpp"
#include "core/openivm_constants.hpp"
#include "lpts_pipeline.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"

#include "core/openivm_debug.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"

#include <regex>

namespace duckdb {

static constexpr const char *OPENIVM_DDL_RECONNECT = "__openivm_reconnect__";
static constexpr const char *OPENIVM_DDL_CLEANUP_PREFIX = "__openivm_cleanup__:";

/// Build "ORDER BY col1 ASC, col2 DESC LIMIT k [OFFSET n]".
/// Works for both LOGICAL_TOP_N (fused) and separate LOGICAL_ORDER_BY + LOGICAL_LIMIT nodes.
/// output_col_names is the sanitized output column list; BoundColumnRefs are resolved via
/// their column_index into that list.
static string BuildTopKSuffix(const vector<BoundOrderByNode> &orders, idx_t limit_val, idx_t offset_val,
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

static void InlineCtesIfPresent(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	if (!PlanContainsCte(plan.get())) {
		return;
	}
	RelaxMaterializedCtes(plan.get());
	Optimizer cte_opt(binder, context);
	CTEInlining cte_inlining(cte_opt);
	plan = cte_inlining.Optimize(std::move(plan));
}

static bool BoolSetting(ClientContext &context, const string &name) {
	Value value;
	return context.TryGetCurrentSetting(name, value) && !value.IsNull() && BooleanValue::Get(value);
}

static const char *ParserIVMTypeName(IVMType type) {
	switch (type) {
	case IVMType::AGGREGATE_HAVING:
		return "AGGREGATE_HAVING";
	case IVMType::AGGREGATE_GROUP:
		return "AGGREGATE_GROUP";
	case IVMType::SIMPLE_AGGREGATE:
		return "SIMPLE_AGGREGATE";
	case IVMType::SIMPLE_PROJECTION:
		return "SIMPLE_PROJECTION";
	case IVMType::WINDOW_PARTITION:
		return "WINDOW_PARTITION";
	case IVMType::GROUP_RECOMPUTE:
		return "GROUP_RECOMPUTE";
	case IVMType::DISTINCT_INCREMENTAL:
		return "DISTINCT_INCREMENTAL";
	case IVMType::SEMI_ANTI_RECOMPUTE:
		return "SEMI_ANTI_RECOMPUTE";
	case IVMType::TOP_K:
		return "TOP_K";
	case IVMType::FULL_REFRESH:
		return "FULL_REFRESH";
	default:
		return "UNKNOWN";
	}
}

static string StripTrailingSemicolon(string sql) {
	StringUtil::Trim(sql);
	if (!sql.empty() && sql.back() == ';') {
		sql.pop_back();
		StringUtil::Trim(sql);
	}
	return sql;
}

static string ExplainInitialLoadQuery(Connection &con, const string &label, const string &query) {
	string sql = StripTrailingSemicolon(query);
	if (sql.empty()) {
		return label + "\n<empty query>\n";
	}
	auto old_explain = con.Query("SELECT current_setting('explain_output')");
	con.Query("SET explain_output='all'");
	auto explain = con.Query("EXPLAIN " + sql);
	if (old_explain && !old_explain->HasError() && old_explain->RowCount() > 0 && !old_explain->GetValue(0, 0).IsNull()) {
		con.Query("SET explain_output='" + OpenIVMUtils::EscapeSingleQuotes(old_explain->GetValue(0, 0).ToString()) + "'");
	}
	string result = label + "\n";
	if (!explain || explain->HasError()) {
		result += "<EXPLAIN failed: " + (explain ? explain->GetError() : string("no result")) + ">\n";
	} else {
		result += explain->ToString() + "\n";
	}
	return result;
}

static bool HasUnionBeforeAggregate(const LogicalOperator *op, bool seen_agg_above = false) {
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

static bool HasUnsupportedSetOperation(const LogicalOperator *op) {
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

static LogicalProjection *FindFirstProjection(LogicalOperator *op) {
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

static bool OuterJoinAggregateNeedsRecompute(LogicalOperator *op, idx_t group_index) {
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

static bool HasRepeatedCteRefUnderJoin(const LogicalOperator *plan) {
	std::map<idx_t, int> cte_ref_count;
	CountCteRefsUnderJoin(plan, false, cte_ref_count);
	for (auto &entry : cte_ref_count) {
		if (entry.second >= 2) {
			return true;
		}
	}
	return false;
}

struct OpenIVMDuckLakeTableInfo {
	string table_name;   // actual name as stored in DuckLake (case-preserved)
	string catalog_name; // DuckLake catalog name (e.g., "dl")
	string schema_name;  // DuckLake schema name
};

struct OpenIVMDeltaTableInfo {
	string table_name;   // actual name as stored in Delta catalog (case-preserved)
	string catalog_name; // Delta catalog name (e.g., "delta_table")
	string schema_name;  // Delta schema name
	string path;         // Delta table path
	int64_t version;
};

struct OpenIVMSourceTableInfo {
	string table_name;
	string catalog_name;
	string schema_name;
};

struct MVClassificationState {
	bool ivm_compatible;
	bool found_aggregation;
	bool found_projection;
	bool found_distinct;
	bool found_having;
	bool found_minmax;
	bool found_left_join;
	bool found_full_outer;
	bool found_semi_anti_join;
	bool found_window;
	bool found_join;
	bool found_top_k;
	bool found_count_distinct;
	bool found_grouping_sets;
	bool found_nested_aggregate;
	bool found_filtered_list;
	bool found_single_join;

	explicit MVClassificationState(const PlanAnalysis &analysis)
	    : ivm_compatible(analysis.ivm_compatible), found_aggregation(analysis.found_aggregation),
	      found_projection(analysis.found_projection), found_distinct(analysis.found_distinct),
	      found_having(analysis.found_having),
	      found_minmax(analysis.found_minmax || analysis.found_count_distinct || analysis.found_list),
	      found_left_join(analysis.found_left_join), found_full_outer(analysis.found_full_outer),
	      found_semi_anti_join(analysis.found_semi_anti_join), found_window(analysis.found_window),
	      found_join(analysis.found_join), found_top_k(analysis.found_top_k),
	      found_count_distinct(analysis.found_count_distinct), found_grouping_sets(analysis.found_grouping_sets),
	      found_nested_aggregate(analysis.found_nested_aggregate), found_filtered_list(analysis.found_filtered_list),
	      found_single_join(analysis.found_single_join) {
	}
};

static void CollectDuckLakeTables(LogicalOperator *op, const string &current_catalog,
                                  unordered_map<string, OpenIVMDuckLakeTableInfo> &dl_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.function.name == "ducklake_scan" && get.function.function_info) {
			auto &info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
			string lc = info.table_name;
			std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
			if (dl_table_info.find(lc) == dl_table_info.end()) {
				// Always pull the catalog from the DuckLakeTableEntry — it's the
				// one the `dl.ORDER_LINE` reference actually resolved to. Falling
				// back to `current_catalog` is wrong for cross-system MVs (native
				// MV reading from dl.*) because `current_catalog` is the physical
				// default, not the DuckLake catalog.
				string cat = info.table.ParentCatalog().GetName();
				if (cat.empty()) {
					cat = current_catalog.empty() ? "dl" : current_catalog;
				}
				dl_table_info[lc] = {info.table_name, cat, info.table.schema.name};
			}
		}
	}
	for (auto &child : op->children) {
		CollectDuckLakeTables(child.get(), current_catalog, dl_table_info);
	}
}

static void CollectDeltaTables(LogicalOperator *op, ClientContext &context,
                               unordered_map<string, OpenIVMDeltaTableInfo> &delta_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto table_ref = get.GetTable();
		if (table_ref.get() && table_ref->ParentCatalog().GetCatalogType() == "delta") {
			auto &table = *table_ref.get();
			string lc = table.name;
			std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
			if (delta_table_info.find(lc) == delta_table_info.end()) {
				auto &catalog = table.ParentCatalog();
				int64_t version = -1;
				delta_table_info[lc] = {table.name, catalog.GetName(), table.schema.name, catalog.GetDBPath(), version};
			}
		}
	}
	for (auto &child : op->children) {
		CollectDeltaTables(child.get(), context, delta_table_info);
	}
}

static void AnnotateDeltaScansForLpts(LogicalOperator *op) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto table_ref = get.GetTable();
		if (table_ref.get() && table_ref->ParentCatalog().GetCatalogType() == "delta") {
			auto &table = *table_ref.get();
			auto &catalog = table.ParentCatalog();
			get.named_parameters["openivm_delta_catalog"] = Value(catalog.GetName());
			get.named_parameters["openivm_delta_schema"] = Value(table.schema.name);
			get.named_parameters["openivm_delta_table"] = Value(table.name);
			get.named_parameters["openivm_delta_path"] = Value(catalog.GetDBPath());
		}
	}
	for (auto &child : op->children) {
		AnnotateDeltaScansForLpts(child.get());
	}
}

static bool HasUnannotatedDeltaScan(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.function.name == "delta_scan" && !get.GetTable().get()) {
			return true;
		}
	}
	for (auto &child : op->children) {
		if (HasUnannotatedDeltaScan(child.get())) {
			return true;
		}
	}
	return false;
}

static void CollectSourceTables(LogicalOperator *op, unordered_map<string, OpenIVMSourceTableInfo> &source_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto table_ref = get.GetTable();
		if (table_ref.get()) {
			auto &table = *table_ref.get();
			string table_name = table.name;
			if (!table_name.empty() && !OpenIVMUtils::IsDelta(table_name)) {
				source_table_info[table_name] = {table_name, table.ParentCatalog().GetName(), table.schema.name};
			}
		}
	}
	for (auto &child : op->children) {
		CollectSourceTables(child.get(), source_table_info);
	}
}

static string QualifiedTablePrefix(const string &catalog_name, const string &schema_name) {
	return KeywordHelper::WriteOptionallyQuoted(catalog_name) + "." +
	       KeywordHelper::WriteOptionallyQuoted(schema_name) + ".";
}

static string QuoteQualifiedPrefix(const string &prefix) {
	string result;
	size_t start = 0;
	while (start < prefix.size()) {
		size_t dot = prefix.find('.', start);
		string part = prefix.substr(start, dot == string::npos ? string::npos : dot - start);
		if (!part.empty()) {
			if (!result.empty()) {
				result += ".";
			}
			result += KeywordHelper::WriteOptionallyQuoted(part);
		}
		if (dot == string::npos) {
			break;
		}
		start = dot + 1;
	}
	return result.empty() ? "" : result + ".";
}

static bool RelationExists(Connection &con, const string &qualified_name) {
	auto result = con.Query("SELECT * FROM " + qualified_name + " LIMIT 0");
	return !result->HasError();
}

static void CollectProjectionIndex(LogicalOperator *op,
                                   unordered_map<idx_t, LogicalProjection *> &projections_by_index) {
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
		// Scalar correlated subqueries normally project the duplicate-eliminated outer keys
		// first. Use that shape as a fallback if binding aliases were hidden by projection
		// rewrites.
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
	unordered_map<idx_t, LogicalProjection *> projections_by_index;
	CollectProjectionIndex(plan, projections_by_index);
	bool seen_aggregate = false;
	CollectAggregateGroupColumns(plan, plan, projections_by_index, output_names, group_names, include_first_aggregate,
	                             seen_aggregate);
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
	unordered_map<idx_t, LogicalProjection *> proj_by_index;
	unordered_map<idx_t, LogicalGet *> get_by_index;
	IndexProjectionsAndGets(plan, proj_by_index, get_by_index);
	string result;
	ExtractFullOuterJoinCols(plan, proj_by_index, get_by_index, result);
	return result;
}

static string SanitizeOutputName(const string &name) {
	if (IVMTableNames::IsInternalColumn(name)) {
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
			output_names.push_back("_ivm_col_" + to_string(idx));
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
		output_names.push_back("_ivm_col_" + to_string(idx));
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
		if (IVMTableNames::IsInternalColumn(name)) {
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

static vector<string> PrepareOutputNames(LogicalOperator *select_plan, const vector<string> &planner_names) {
	auto output_names = planner_names;
	SanitizeOutputNames(output_names);
	AppendHiddenOutputNames(select_plan, output_names);
	DeduplicateOutputNames(output_names);
	return output_names;
}

static size_t FindKeywordToken(const string &text, const string &keyword, size_t from) {
	size_t pos = from;
	while (true) {
		pos = text.find(keyword, pos);
		if (pos == string::npos) {
			return string::npos;
		}
		bool ok_left = (pos == 0) || std::isspace(static_cast<unsigned char>(text[pos - 1])) || text[pos - 1] == '(';
		bool ok_right = (pos + keyword.size() == text.size()) ||
		                std::isspace(static_cast<unsigned char>(text[pos + keyword.size()])) ||
		                text[pos + keyword.size()] == '(';
		if (ok_left && ok_right) {
			return pos;
		}
		pos++;
	}
}

static bool StartsKeywordToken(const string &text, size_t pos, const string &keyword) {
	if (pos + keyword.size() > text.size()) {
		return false;
	}
	if (text.compare(pos, keyword.size(), keyword) != 0) {
		return false;
	}
	return (pos + keyword.size() == text.size()) ||
	       !std::isalnum(static_cast<unsigned char>(text[pos + keyword.size()]));
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

static bool QueryNeedsOriginalSqlForLpts(const string &query) {
	string lower = StringUtil::Lower(query);
	return lower.find("pivot ") != string::npos || lower.find("(pivot ") != string::npos;
}

static bool QueryHasUnsupportedIncrementalConstruct(const string &query) {
	string lower = StringUtil::Lower(query);
	return lower.find("pivot ") != string::npos || lower.find("(pivot ") != string::npos ||
	       lower.find(" unnest(") != string::npos || lower.find(" cross join unnest(") != string::npos;
}

static bool PlanNeedsOriginalSqlForLpts(LogicalOperator *op) {
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

static LogicalAggregate *FindOuterAggregate(LogicalOperator *op) {
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

static string JsonQuote(const string &value) {
	string result = "\"";
	for (char c : value) {
		if (c == '"' || c == '\\') {
			result += '\\';
			result += c;
		} else if (c == '\n') {
			result += "\\n";
		} else {
			result += c;
		}
	}
	result += "\"";
	return result;
}

static bool IsPacLoaded(ClientContext &context) {
	Value pac_check_val;
	return context.TryGetCurrentSetting("pac_check", pac_check_val);
}

static void ForwardPacSettingsIfLoaded(ClientContext &context, Connection &con) {
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

/// Extract a `(SELECT DISTINCT cols FROM source [WHERE p])` subquery from the
/// user's CREATE-MV SQL. Single-source v0 — succeeds only for the simple shape
/// where the DISTINCT body has exactly one base table after FROM (joins/CTEs
/// fail extraction; caller demotes to GROUP_RECOMPUTE).
///
/// Output:
///   `out_cols`        — column names from `DISTINCT a, b, c`
///   `out_input_sql`   — same SELECT body with `DISTINCT` keyword stripped
///   `out_source`      — the table referenced after `FROM` (alias-stripped)
///   `out_filter_sql`  — anything between `WHERE` and the next clause boundary,
///                       or empty if no WHERE; included so the aux-population
///                       and refresh-time delta queries apply the same filter
///
/// Returns false on: no DISTINCT, multiple DISTINCTs, a non-table FROM (subquery,
/// CTE, JOIN), unbalanced parens, or any structural surprise. The caller treats
/// false as "demote to GROUP_RECOMPUTE".
static bool ExtractInnerDistinct(const string &original_sql, vector<string> &out_cols, string &out_input_sql,
                                 string &out_source, string &out_filter_sql) {
	string lower = StringUtil::Lower(original_sql);
	// Find "select distinct" — must be a token (preceded by '(' or whitespace).
	size_t distinct_pos = FindKeywordToken(lower, "select distinct", 0);
	if (distinct_pos == string::npos) {
		return false;
	}
	// Multiple DISTINCTs in the same view → unsupported in v0.
	if (FindKeywordToken(lower, "select distinct", distinct_pos + 1) != string::npos) {
		return false;
	}

	// Find " from " at the same paren level as the SELECT DISTINCT.
	size_t cols_start = distinct_pos + strlen("select distinct ");
	int depth = 0;
	size_t from_pos = string::npos;
	for (size_t i = cols_start; i < lower.size(); i++) {
		if (lower[i] == '(') {
			depth++;
		} else if (lower[i] == ')') {
			if (depth == 0) {
				return false; // unbalanced, abort
			}
			depth--;
		} else if (depth == 0) {
			static const string from_kw = " from ";
			if (i + from_kw.size() <= lower.size() && lower.compare(i, from_kw.size(), from_kw) == 0) {
				from_pos = i;
				break;
			}
		}
	}
	if (from_pos == string::npos) {
		return false;
	}

	// Parse the column list (comma-split at depth 0). Use the original-case substring.
	string cols_text = original_sql.substr(cols_start, from_pos - cols_start);
	vector<string> cols;
	{
		int pd = 0;
		size_t last = 0;
		for (size_t i = 0; i < cols_text.size(); i++) {
			if (cols_text[i] == '(') {
				pd++;
			} else if (cols_text[i] == ')') {
				pd--;
			} else if (pd == 0 && cols_text[i] == ',') {
				string c = cols_text.substr(last, i - last);
				StringUtil::Trim(c);
				cols.push_back(std::move(c));
				last = i + 1;
			}
		}
		string last_c = cols_text.substr(last);
		StringUtil::Trim(last_c);
		cols.push_back(std::move(last_c));
	}
	if (cols.empty()) {
		return false;
	}
	for (auto &c : cols) {
		if (c.empty() || c == "*") {
			return false; // unqualified `*` would need source-schema introspection — punt.
		}
	}

	// Read the FROM clause. Source must be a single bare identifier (or
	// schema.table); subqueries, JOINs, and CTE references abort.
	size_t after_from = from_pos + strlen(" from ");
	// Skip leading whitespace.
	while (after_from < lower.size() && std::isspace(static_cast<unsigned char>(lower[after_from]))) {
		after_from++;
	}
	if (after_from >= lower.size() || lower[after_from] == '(') {
		return false; // FROM is a subquery — multi-source/complex shape, demote.
	}
	// Read identifier (allow letters, digits, underscores, dots, double-quotes).
	size_t src_end = after_from;
	bool in_quote = false;
	while (src_end < lower.size()) {
		char c = lower[src_end];
		if (in_quote) {
			if (c == '"') {
				in_quote = false;
			}
			src_end++;
			continue;
		}
		if (c == '"') {
			in_quote = true;
			src_end++;
			continue;
		}
		if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.') {
			src_end++;
			continue;
		}
		break;
	}
	out_source = original_sql.substr(after_from, src_end - after_from);
	if (out_source.empty()) {
		return false;
	}

	// Skip optional alias (single bare word after the source identifier).
	size_t alias_skip = src_end;
	while (alias_skip < lower.size() && std::isspace(static_cast<unsigned char>(lower[alias_skip]))) {
		alias_skip++;
	}
	// Recognised keywords that terminate the FROM section.
	if (alias_skip < lower.size() && lower[alias_skip] != ',' && lower[alias_skip] != ')' &&
	    !StartsKeywordToken(lower, alias_skip, "where") && !StartsKeywordToken(lower, alias_skip, "group") &&
	    !StartsKeywordToken(lower, alias_skip, "order") && !StartsKeywordToken(lower, alias_skip, "having") &&
	    !StartsKeywordToken(lower, alias_skip, "limit") && !StartsKeywordToken(lower, alias_skip, "union") &&
	    !StartsKeywordToken(lower, alias_skip, "join") && !StartsKeywordToken(lower, alias_skip, "left") &&
	    !StartsKeywordToken(lower, alias_skip, "right") && !StartsKeywordToken(lower, alias_skip, "inner") &&
	    !StartsKeywordToken(lower, alias_skip, "full") && !StartsKeywordToken(lower, alias_skip, "cross") &&
	    !StartsKeywordToken(lower, alias_skip, "on")) {
		// Treat as an alias word; consume it.
		size_t alias_end = alias_skip;
		while (alias_end < lower.size() &&
		       (std::isalnum(static_cast<unsigned char>(lower[alias_end])) || lower[alias_end] == '_')) {
			alias_end++;
		}
		alias_skip = alias_end;
	}
	// Anything other than WHERE / end-of-subquery here means we hit a JOIN or comma,
	// which we don't support in v0.
	while (alias_skip < lower.size() && std::isspace(static_cast<unsigned char>(lower[alias_skip]))) {
		alias_skip++;
	}
	size_t where_pos = string::npos;
	if (alias_skip < lower.size()) {
		if (StartsKeywordToken(lower, alias_skip, "where")) {
			where_pos = alias_skip;
		} else if (lower[alias_skip] != ')' && !StartsKeywordToken(lower, alias_skip, "group") &&
		           !StartsKeywordToken(lower, alias_skip, "order") && !StartsKeywordToken(lower, alias_skip, "limit") &&
		           !StartsKeywordToken(lower, alias_skip, "union")) {
			return false; // unsupported shape (JOIN, comma, etc.)
		}
	}

	// Compute the end of the DISTINCT subquery. If we're inside parens (the common
	// case `... FROM (SELECT DISTINCT ...) sub ...`), match the closing ')'. If not,
	// the subquery ends at the next clause boundary or end of statement.
	size_t end_pos = string::npos;
	int d = 0;
	for (size_t i = (where_pos != string::npos ? where_pos : alias_skip); i < lower.size(); i++) {
		if (lower[i] == '(') {
			d++;
		} else if (lower[i] == ')') {
			if (d == 0) {
				end_pos = i;
				break;
			}
			d--;
		} else if (d == 0 && (StartsKeywordToken(lower, i, "group") || StartsKeywordToken(lower, i, "order") ||
		                      StartsKeywordToken(lower, i, "limit") || StartsKeywordToken(lower, i, "union"))) {
			end_pos = i;
			break;
		}
	}
	if (end_pos == string::npos) {
		end_pos = lower.size();
	}

	// Build out_filter_sql from `WHERE ... <end>` (excluding WHERE keyword).
	if (where_pos != string::npos) {
		size_t filter_start = where_pos + strlen("where ");
		out_filter_sql = original_sql.substr(filter_start, end_pos - filter_start);
		StringUtil::Trim(out_filter_sql);
	} else {
		out_filter_sql.clear();
	}

	// Build input_sql: the original DISTINCT subquery span with `DISTINCT ` removed.
	string subq = original_sql.substr(distinct_pos, end_pos - distinct_pos);
	{
		string subq_lower = StringUtil::Lower(subq);
		size_t kw = subq_lower.find("distinct ");
		if (kw == string::npos) {
			return false;
		}
		out_input_sql = subq.substr(0, kw) + subq.substr(kw + strlen("distinct "));
		StringUtil::Trim(out_input_sql);
	}

	out_cols = std::move(cols);
	return true;
}

struct SemiAntiExtract {
	string join_type;
	string left_table;
	string left_alias;
	string right_table;
	string right_alias;
	string predicate;
	string post_filter;
	vector<string> output_cols;
	vector<string> output_exprs;
};

static bool ReadIdentifierToken(const string &sql, size_t &pos, string &out) {
	while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
		pos++;
	}
	if (pos >= sql.size()) {
		return false;
	}
	size_t start = pos;
	bool in_quote = false;
	while (pos < sql.size()) {
		char c = sql[pos];
		if (in_quote) {
			if (c == '"') {
				in_quote = false;
			}
			pos++;
			continue;
		}
		if (c == '"') {
			in_quote = true;
			pos++;
			continue;
		}
		if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.') {
			pos++;
			continue;
		}
		break;
	}
	out = sql.substr(start, pos - start);
	return !out.empty();
}

static string LastIdentifierPart(string ident) {
	StringUtil::Trim(ident);
	size_t dot = ident.find_last_of('.');
	if (dot != string::npos) {
		ident = ident.substr(dot + 1);
	}
	if (ident.size() >= 2 && ident.front() == '"' && ident.back() == '"') {
		ident = ident.substr(1, ident.size() - 2);
	}
	return ident;
}

static size_t FindTopLevelKeywordToken(const string &text, const string &keyword, size_t from) {
	bool in_quote = false;
	int depth = 0;
	for (size_t i = from; i < text.size(); i++) {
		char c = text[i];
		if (in_quote) {
			if (c == '\'') {
				in_quote = false;
			}
			continue;
		}
		if (c == '\'') {
			in_quote = true;
			continue;
		}
		if (c == '(') {
			depth++;
			continue;
		}
		if (c == ')') {
			if (depth > 0) {
				depth--;
			}
			continue;
		}
		if (depth == 0 && StartsKeywordToken(text, i, keyword)) {
			return i;
		}
	}
	return string::npos;
}

static size_t FindMatchingParen(const string &text, size_t open_pos) {
	if (open_pos >= text.size() || text[open_pos] != '(') {
		return string::npos;
	}
	bool in_quote = false;
	int depth = 0;
	for (size_t i = open_pos; i < text.size(); i++) {
		char c = text[i];
		if (in_quote) {
			if (c == '\'') {
				in_quote = false;
			}
			continue;
		}
		if (c == '\'') {
			in_quote = true;
			continue;
		}
		if (c == '(') {
			depth++;
		} else if (c == ')') {
			depth--;
			if (depth == 0) {
				return i;
			}
		}
	}
	return string::npos;
}

struct SelectOutputItem {
	string expr;
	string name;
};

static bool ParseSelectOutputItems(const string &original_sql, size_t select_pos, size_t from_pos,
                                   vector<SelectOutputItem> &items) {
	string select_list = original_sql.substr(select_pos + strlen("select"), from_pos - (select_pos + strlen("select")));
	auto parse_item = [](string item, SelectOutputItem &out) {
		StringUtil::Trim(item);
		size_t as_pos = FindTopLevelKeywordToken(StringUtil::Lower(item), "as", 0);
		if (as_pos != string::npos) {
			out.expr = item.substr(0, as_pos);
			StringUtil::Trim(out.expr);
			string alias = item.substr(as_pos + strlen("as"));
			StringUtil::Trim(alias);
			size_t alias_pos = 0;
			string alias_token;
			if (ReadIdentifierToken(alias, alias_pos, alias_token)) {
				out.name = LastIdentifierPart(alias_token);
				return !out.expr.empty() && !out.name.empty();
			}
		}
		out.expr = item;
		out.name = LastIdentifierPart(item);
		return !out.expr.empty() && !out.name.empty();
	};
	int depth = 0;
	size_t last = 0;
	for (size_t i = 0; i < select_list.size(); i++) {
		if (select_list[i] == '(') {
			depth++;
		} else if (select_list[i] == ')') {
			depth--;
		} else if (depth == 0 && select_list[i] == ',') {
			SelectOutputItem item;
			if (!parse_item(select_list.substr(last, i - last), item) || item.name == "*") {
				return false;
			}
			items.push_back(std::move(item));
			last = i + 1;
		}
	}
	SelectOutputItem item;
	if (!parse_item(select_list.substr(last), item) || item.name == "*") {
		return false;
	}
	items.push_back(std::move(item));
	return true;
}

static bool ParseSelectOutputColumns(const string &original_sql, size_t select_pos, size_t from_pos,
                                     vector<string> &output_cols, vector<string> *output_exprs = nullptr) {
	vector<SelectOutputItem> items;
	if (!ParseSelectOutputItems(original_sql, select_pos, from_pos, items)) {
		return false;
	}
	for (auto &item : items) {
		output_cols.push_back(item.name);
		if (output_exprs != nullptr) {
			output_exprs->push_back(item.expr);
		}
	}
	return true;
}

static string TrimAndConjunctions(string expr) {
	StringUtil::Trim(expr);
	string lower = StringUtil::Lower(expr);
	if (lower.rfind("and ", 0) == 0) {
		expr = expr.substr(strlen("and "));
		StringUtil::Trim(expr);
		lower = StringUtil::Lower(expr);
	}
	static const string and_suffix = " and";
	if (lower.size() >= and_suffix.size() &&
	    lower.compare(lower.size() - and_suffix.size(), and_suffix.size(), and_suffix) == 0) {
		expr = expr.substr(0, expr.size() - and_suffix.size());
		StringUtil::Trim(expr);
	}
	return expr;
}

static bool ExtractSemiAntiJoin(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	size_t semi_pos = FindKeywordToken(lower, "semi join", 0);
	size_t anti_pos = FindKeywordToken(lower, "anti join", 0);
	if (semi_pos != string::npos && anti_pos != string::npos) {
		return false;
	}
	bool is_semi = semi_pos != string::npos;
	size_t join_pos = is_semi ? semi_pos : anti_pos;
	if (join_pos == string::npos) {
		return false;
	}
	out.join_type = is_semi ? "semi" : "anti";

	size_t from_pos = FindKeywordToken(lower, "from", 0);
	if (from_pos == string::npos || from_pos > join_pos) {
		return false;
	}
	size_t select_pos = FindKeywordToken(lower, "select", 0);
	if (select_pos == string::npos || select_pos > from_pos) {
		return false;
	}

	vector<string> output_cols;
	vector<string> output_exprs;
	if (!ParseSelectOutputColumns(original_sql, select_pos, from_pos, output_cols, &output_exprs)) {
		return false;
	}

	size_t pos = from_pos + strlen("from");
	if (!ReadIdentifierToken(original_sql, pos, out.left_table)) {
		return false;
	}
	size_t alias_pos = pos;
	string maybe_alias;
	if (ReadIdentifierToken(original_sql, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "semi" && maybe_lower != "anti") {
			out.left_alias = maybe_alias;
			pos = alias_pos;
		}
	}
	if (out.left_alias.empty()) {
		out.left_alias = LastIdentifierPart(out.left_table);
	}
	string between_left_and_join = original_sql.substr(pos, join_pos - pos);
	string between_left_and_join_lower = StringUtil::Lower(between_left_and_join);
	if (FindKeywordToken(between_left_and_join_lower, "join", 0) != string::npos ||
	    between_left_and_join_lower.find(',') != string::npos) {
		return false;
	}

	size_t right_pos = join_pos + (is_semi ? strlen("semi join") : strlen("anti join"));
	if (!ReadIdentifierToken(original_sql, right_pos, out.right_table)) {
		return false;
	}
	alias_pos = right_pos;
	maybe_alias.clear();
	if (ReadIdentifierToken(original_sql, alias_pos, maybe_alias)) {
		if (StringUtil::Lower(maybe_alias) != "on") {
			out.right_alias = maybe_alias;
			right_pos = alias_pos;
		}
	}
	if (out.right_alias.empty()) {
		out.right_alias = LastIdentifierPart(out.right_table);
	}

	size_t on_pos = FindKeywordToken(lower, "on", right_pos);
	if (on_pos == string::npos) {
		return false;
	}
	size_t pred_start = on_pos + strlen("on");
	size_t pred_end = original_sql.size();
	size_t where_pos = string::npos;
	for (auto kw : {"where", "group", "order", "limit", "union"}) {
		size_t kw_pos = FindKeywordToken(lower, kw, pred_start);
		if (kw_pos != string::npos) {
			if (string(kw) == "where") {
				where_pos = kw_pos;
			}
			pred_end = std::min(pred_end, kw_pos);
		}
	}
	out.predicate = original_sql.substr(pred_start, pred_end - pred_start);
	StringUtil::Trim(out.predicate);
	if (out.predicate.empty()) {
		return false;
	}
	if (where_pos != string::npos) {
		size_t filter_start = where_pos + strlen("where");
		size_t filter_end = original_sql.size();
		for (auto kw : {"group", "order", "limit", "union"}) {
			size_t kw_pos = FindKeywordToken(lower, kw, filter_start);
			if (kw_pos != string::npos) {
				filter_end = std::min(filter_end, kw_pos);
			}
		}
		out.post_filter = original_sql.substr(filter_start, filter_end - filter_start);
		StringUtil::Trim(out.post_filter);
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

static bool ExtractExistsSubquery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	size_t select_pos = FindKeywordToken(lower, "select", 0);
	size_t from_pos = FindTopLevelKeywordToken(lower, "from", select_pos == string::npos ? 0 : select_pos);
	if (select_pos == string::npos || from_pos == string::npos || select_pos > from_pos) {
		return false;
	}

	vector<string> output_cols;
	vector<string> output_exprs;
	if (!ParseSelectOutputColumns(original_sql, select_pos, from_pos, output_cols, &output_exprs)) {
		return false;
	}

	size_t pos = from_pos + strlen("from");
	if (!ReadIdentifierToken(original_sql, pos, out.left_table)) {
		return false;
	}
	size_t alias_pos = pos;
	string maybe_alias;
	if (ReadIdentifierToken(original_sql, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "where") {
			out.left_alias = maybe_alias;
			pos = alias_pos;
		}
	}
	if (out.left_alias.empty()) {
		out.left_alias = LastIdentifierPart(out.left_table);
	}

	size_t where_pos = FindTopLevelKeywordToken(lower, "where", pos);
	if (where_pos == string::npos) {
		return false;
	}
	string between_left_and_where = StringUtil::Lower(original_sql.substr(pos, where_pos - pos));
	if (FindKeywordToken(between_left_and_where, "join", 0) != string::npos ||
	    between_left_and_where.find(',') != string::npos) {
		return false;
	}

	size_t not_exists_pos = FindTopLevelKeywordToken(lower, "not exists", where_pos);
	size_t exists_pos = FindTopLevelKeywordToken(lower, "exists", where_pos);
	bool is_anti = not_exists_pos != string::npos;
	size_t exists_kw_pos = is_anti ? not_exists_pos : exists_pos;
	if (exists_kw_pos == string::npos || (is_anti && exists_pos != string::npos && exists_pos < not_exists_pos)) {
		return false;
	}
	out.join_type = is_anti ? "anti" : "semi";

	size_t after_exists = exists_kw_pos + (is_anti ? strlen("not exists") : strlen("exists"));
	while (after_exists < original_sql.size() && std::isspace(static_cast<unsigned char>(original_sql[after_exists]))) {
		after_exists++;
	}
	size_t close_pos = FindMatchingParen(original_sql, after_exists);
	if (close_pos == string::npos) {
		return false;
	}

	string outer_filter =
	    original_sql.substr(where_pos + strlen("where"), exists_kw_pos - (where_pos + strlen("where")));
	outer_filter += original_sql.substr(close_pos + 1);
	out.post_filter = TrimAndConjunctions(outer_filter);

	string subquery = original_sql.substr(after_exists + 1, close_pos - after_exists - 1);
	string sub_lower = StringUtil::Lower(subquery);
	size_t sub_select = FindKeywordToken(sub_lower, "select", 0);
	size_t sub_from = FindTopLevelKeywordToken(sub_lower, "from", sub_select == string::npos ? 0 : sub_select);
	if (sub_select == string::npos || sub_from == string::npos || sub_select > sub_from) {
		return false;
	}
	size_t right_pos = sub_from + strlen("from");
	if (!ReadIdentifierToken(subquery, right_pos, out.right_table)) {
		return false;
	}
	alias_pos = right_pos;
	maybe_alias.clear();
	if (ReadIdentifierToken(subquery, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "where" && maybe_lower != "group" && maybe_lower != "order" && maybe_lower != "limit") {
			out.right_alias = maybe_alias;
			right_pos = alias_pos;
		}
	}
	if (out.right_alias.empty()) {
		out.right_alias = LastIdentifierPart(out.right_table);
	}

	size_t sub_where = FindTopLevelKeywordToken(sub_lower, "where", right_pos);
	string between_right_and_filter =
	    sub_lower.substr(right_pos, (sub_where == string::npos ? sub_lower.size() : sub_where) - right_pos);
	if (FindKeywordToken(between_right_and_filter, "join", 0) != string::npos ||
	    between_right_and_filter.find(',') != string::npos) {
		return false;
	}
	if (sub_where == string::npos) {
		out.predicate = "true";
	} else {
		size_t pred_start = sub_where + strlen("where");
		size_t pred_end = subquery.size();
		for (auto kw : {"group", "order", "limit", "union"}) {
			size_t kw_pos = FindTopLevelKeywordToken(sub_lower, kw, pred_start);
			if (kw_pos != string::npos) {
				pred_end = std::min(pred_end, kw_pos);
			}
		}
		out.predicate = subquery.substr(pred_start, pred_end - pred_start);
		StringUtil::Trim(out.predicate);
	}
	if (out.predicate.empty()) {
		return false;
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

static bool ExtractInSubquery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	size_t select_pos = FindKeywordToken(lower, "select", 0);
	size_t from_pos = FindTopLevelKeywordToken(lower, "from", select_pos == string::npos ? 0 : select_pos);
	if (select_pos == string::npos || from_pos == string::npos || select_pos > from_pos) {
		return false;
	}

	vector<string> output_cols;
	vector<string> output_exprs;
	if (!ParseSelectOutputColumns(original_sql, select_pos, from_pos, output_cols, &output_exprs)) {
		return false;
	}

	size_t where_pos = FindTopLevelKeywordToken(lower, "where", from_pos + strlen("from"));
	if (where_pos == string::npos) {
		return false;
	}
	string left_from = original_sql.substr(from_pos + strlen("from"), where_pos - (from_pos + strlen("from")));
	StringUtil::Trim(left_from);
	if (left_from.empty()) {
		return false;
	}
	string left_table_expr = left_from;
	string left_alias_expr = "_ivm_left";
	bool simple_left_table = false;
	size_t left_pos = 0;
	string left_ident;
	if (ReadIdentifierToken(left_from, left_pos, left_ident)) {
		const string &candidate_table = left_ident;
		string candidate_alias = LastIdentifierPart(left_ident);
		size_t left_alias_pos = left_pos;
		string maybe_left_alias;
		if (ReadIdentifierToken(left_from, left_alias_pos, maybe_left_alias)) {
			candidate_alias = maybe_left_alias;
			left_pos = left_alias_pos;
		}
		size_t tail_pos = left_pos;
		while (tail_pos < left_from.size() && std::isspace(static_cast<unsigned char>(left_from[tail_pos]))) {
			tail_pos++;
		}
		if (tail_pos == left_from.size()) {
			left_table_expr = candidate_table;
			left_alias_expr = candidate_alias;
			simple_left_table = true;
		}
	}
	if (!simple_left_table) {
		string select_list =
		    original_sql.substr(select_pos + strlen("select"), from_pos - (select_pos + strlen("select")));
		StringUtil::Trim(select_list);
		left_table_expr = "(SELECT " + select_list + " FROM " + left_from + ")";
		left_alias_expr = "_ivm_left";
	}

	size_t not_in_pos = FindTopLevelKeywordToken(lower, "not in", where_pos);
	size_t in_pos = FindTopLevelKeywordToken(lower, "in", where_pos);
	bool is_anti = not_in_pos != string::npos;
	size_t in_kw_pos = is_anti ? not_in_pos : in_pos;
	if (in_kw_pos == string::npos || (is_anti && in_pos != string::npos && in_pos < not_in_pos)) {
		return false;
	}

	string lhs = original_sql.substr(where_pos + strlen("where"), in_kw_pos - (where_pos + strlen("where")));
	lhs = TrimAndConjunctions(lhs);
	if (lhs.empty()) {
		return false;
	}

	size_t after_in = in_kw_pos + (is_anti ? strlen("not in") : strlen("in"));
	while (after_in < original_sql.size() && std::isspace(static_cast<unsigned char>(original_sql[after_in]))) {
		after_in++;
	}
	size_t close_pos = FindMatchingParen(original_sql, after_in);
	if (close_pos == string::npos) {
		return false;
	}
	string trailing_filter = original_sql.substr(close_pos + 1);
	out.post_filter = TrimAndConjunctions(trailing_filter);

	string subquery = original_sql.substr(after_in + 1, close_pos - after_in - 1);
	string sub_lower = StringUtil::Lower(subquery);
	if (FindTopLevelKeywordToken(sub_lower, "union", 0) != string::npos) {
		return false;
	}
	size_t sub_select = FindKeywordToken(sub_lower, "select", 0);
	size_t sub_from = FindTopLevelKeywordToken(sub_lower, "from", sub_select == string::npos ? 0 : sub_select);
	if (sub_select == string::npos || sub_from == string::npos || sub_select > sub_from) {
		return false;
	}
	string rhs_expr = subquery.substr(sub_select + strlen("select"), sub_from - (sub_select + strlen("select")));
	StringUtil::Trim(rhs_expr);
	string rhs_lower = StringUtil::Lower(rhs_expr);
	if (rhs_lower.rfind("distinct ", 0) == 0) {
		rhs_expr = rhs_expr.substr(strlen("distinct "));
		StringUtil::Trim(rhs_expr);
	}
	if (rhs_expr.empty() || rhs_expr.find(',') != string::npos) {
		return false;
	}

	size_t right_pos = sub_from + strlen("from");
	if (!ReadIdentifierToken(subquery, right_pos, out.right_table)) {
		return false;
	}
	size_t alias_pos = right_pos;
	string maybe_alias;
	if (ReadIdentifierToken(subquery, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "where" && maybe_lower != "group" && maybe_lower != "order" && maybe_lower != "limit") {
			out.right_alias = maybe_alias;
			right_pos = alias_pos;
		}
	}
	if (out.right_alias.empty()) {
		out.right_alias = LastIdentifierPart(out.right_table);
	}
	string original_right_alias = out.right_alias;

	size_t sub_where = FindTopLevelKeywordToken(sub_lower, "where", right_pos);
	string right_filter;
	if (sub_where != string::npos) {
		size_t pred_start = sub_where + strlen("where");
		size_t pred_end = subquery.size();
		for (auto kw : {"group", "order", "limit"}) {
			size_t kw_pos = FindTopLevelKeywordToken(sub_lower, kw, pred_start);
			if (kw_pos != string::npos) {
				pred_end = std::min(pred_end, kw_pos);
			}
		}
		right_filter = subquery.substr(pred_start, pred_end - pred_start);
		StringUtil::Trim(right_filter);
	}

	out.join_type = is_anti ? "anti" : "semi";
	out.left_table = left_table_expr;
	out.left_alias = left_alias_expr;
	out.right_alias = "_ivm_right";
	if (simple_left_table) {
		out.predicate = StringUtil::Replace(lhs, out.left_alias + ".", out.left_alias + ".");
		out.predicate =
		    StringUtil::Replace(out.predicate, LastIdentifierPart(out.left_table) + ".", out.left_alias + ".");
	} else {
		out.predicate = out.left_alias + "." + KeywordHelper::WriteOptionallyQuoted(LastIdentifierPart(lhs));
	}
	out.predicate += " IS NOT DISTINCT FROM ";
	out.predicate += StringUtil::Replace(rhs_expr, original_right_alias + ".", out.right_alias + ".");
	out.predicate =
	    StringUtil::Replace(out.predicate, LastIdentifierPart(out.right_table) + ".", out.right_alias + ".");
	if (!right_filter.empty()) {
		string rewritten_filter = StringUtil::Replace(right_filter, original_right_alias + ".", out.right_alias + ".");
		rewritten_filter =
		    StringUtil::Replace(rewritten_filter, LastIdentifierPart(out.right_table) + ".", out.right_alias + ".");
		out.predicate += " AND (" + rewritten_filter + ")";
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

static bool ExtractSemiAntiQuery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	if (FindTopLevelKeywordToken(lower, "union", 0) != string::npos ||
	    FindTopLevelKeywordToken(lower, "intersect", 0) != string::npos ||
	    FindTopLevelKeywordToken(lower, "except", 0) != string::npos) {
		return false;
	}
	return ExtractSemiAntiJoin(original_sql, out) || ExtractExistsSubquery(original_sql, out) ||
	       ExtractInSubquery(original_sql, out);
}

static unique_ptr<FunctionData> IVMDDLBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// DDL statements are passed via result.parameters from the plan function.
	if (!input.inputs.empty()) {
		auto &db = DatabaseInstance::GetDatabase(context);
		auto conn = make_uniq<Connection>(db);
		auto configure_openivm_connection = [&]() {
			// OpenIVM-managed DDL materializes relational state where physical insertion
			// order is not observable. Disabling order preservation avoids DuckDB holding
			// large ordering buffers for wide CTAS-style MV builds.
			auto preserve_result = conn->Query("SET preserve_insertion_order=false");
			if (preserve_result->HasError()) {
				throw CatalogException("Failed to configure OpenIVM DDL connection: " + preserve_result->GetError());
			}
		};
		configure_openivm_connection();
		vector<string> cleanup_ddl;
		auto run_cleanup = [&]() {
			for (const auto &cleanup : cleanup_ddl) {
				OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Cleanup DDL: %s\n", cleanup.c_str());
				auto cleanup_result = conn->Query(cleanup);
				if (cleanup_result->HasError()) {
					OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Cleanup failed: %s\n",
					                    cleanup_result->GetError().c_str());
				}
			}
		};
		for (auto &param : input.inputs) {
			auto q = param.GetValue<string>();
			if (q.empty()) {
				continue;
			}
			if (StringUtil::StartsWith(q, OPENIVM_DDL_CLEANUP_PREFIX)) {
				cleanup_ddl.push_back(q.substr(strlen(OPENIVM_DDL_CLEANUP_PREFIX)));
				continue;
			}
			if (q == OPENIVM_DDL_RECONNECT) {
				// DuckLake keeps read transaction state on the connection. Reconnect
				// between staged DuckLake reads and DuckLake writes so the write-side
				// commit cannot self-block on an earlier read from the same CREATE MV.
				conn = make_uniq<Connection>(db);
				configure_openivm_connection();
				continue;
			}
			OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Executing DDL: %s\n", q.c_str());
			auto r = conn->Query(q);
			if (r->HasError()) {
				// The unique index on the MV data table is an upsert-time optimization
				// (helps MERGE find the matching row quickly). If the classifier's
				// group_columns don't actually form a unique key for the MV (e.g. a
				// CTE with GROUP BY joined against another table — the outer JOIN
				// duplicates rows), CREATE UNIQUE INDEX fails with "Data contains
				// duplicates". Skip just that statement and continue; the refresh
				// still works via key-based MERGE, just a bit slower.
				bool is_unique_index = StringUtil::Contains(StringUtil::Lower(q), "create unique index") &&
				                       StringUtil::Contains(r->GetError(), "Data contains duplicates");
				if (is_unique_index) {
					Printer::Print("Warning: could not create unique index for MV — group_columns "
					               "are not unique in MV output. Refresh will still work (no index).");
					continue;
				}
				run_cleanup();
				throw CatalogException("Failed to execute IVM DDL: " + r->GetError());
			}
		}
	}
	names.emplace_back("MATERIALIZED VIEW CREATION");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return make_uniq<IVMFunction::IVMBindData>(true);
}

static void IVMDDLExecuteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<IVMFunction::IVMBindData>();
	auto &gdata = dynamic_cast<IVMFunction::IVMGlobalData &>(*data_p.global_state);
	if (gdata.offset >= 1) {
		return;
	}
	output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
	output.SetCardinality(1);
	gdata.offset++;
}

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	auto query_lower = OpenIVMUtils::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);
	// Strip SQL line comments (-- to end of line) before whitespace normalization.
	// RemoveRedundantWhitespaces collapses '\n' to ' ', which would turn
	// "-- comment\n rest" into "-- comment rest" where the rest is eaten by the comment.
	OpenIVMUtils::StripLineComments(query_lower);
	OpenIVMUtils::RemoveRedundantWhitespaces(query_lower);

	// Handle ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' | SET REFRESH MANUAL
	if (StringUtil::Contains(query_lower, "alter materialized view")) {
		std::regex alter_re("alter\\s+materialized\\s+view\\s+(\"(?:[^\"]+)\"|[a-zA-Z0-9_.]+)\\s+set\\s+refresh\\s+("
		                    "every\\s+'([^']+)'|manual)",
		                    std::regex::icase);
		std::smatch match;
		if (!std::regex_search(query_lower, match, alter_re)) {
			throw ParserException("Invalid ALTER MATERIALIZED VIEW syntax. "
			                      "Expected: ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' "
			                      "or ALTER MATERIALIZED VIEW <name> SET REFRESH MANUAL");
		}
		string alter_view_name = match[1].str();
		if (alter_view_name.size() >= 2 && alter_view_name.front() == '"' && alter_view_name.back() == '"') {
			alter_view_name = alter_view_name.substr(1, alter_view_name.size() - 2);
		}
		string refresh_type = StringUtil::Lower(match[2].str());
		string update_sql;
		if (refresh_type == "manual") {
			update_sql = "UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_interval = NULL WHERE view_name = '" +
			             OpenIVMUtils::EscapeSingleQuotes(alter_view_name) + "'";
		} else {
			int64_t interval = OpenIVMUtils::ParseRefreshInterval(match[3].str());
			update_sql = "UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_interval = " + to_string(interval) +
			             " WHERE view_name = '" + OpenIVMUtils::EscapeSingleQuotes(alter_view_name) + "'";
		}
		// Pass the UPDATE SQL through IVMParseData; IVMPlanFunction will execute it
		Parser alter_parser;
		alter_parser.ParseQuery("SELECT 1");
		auto parse_data =
		    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(alter_parser.statements[0]), true);
		dynamic_cast<IVMParseData &>(*parse_data).alter_sql = update_sql;
		return ParserExtensionParseResult(std::move(parse_data));
	}

	if (!StringUtil::Contains(query_lower, "create materialized view") &&
	    !StringUtil::Contains(query_lower, "create or replace materialized view")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	// Detect CREATE OR REPLACE MATERIALIZED VIEW
	bool is_replace = false;
	std::regex or_replace_re("\\bcreate\\s+or\\s+replace\\s+materialized\\s+view\\b", std::regex::icase);
	if (std::regex_search(query_lower, or_replace_re)) {
		is_replace = true;
		// Strip "or replace" so the rest of the pipeline sees "create materialized view"
		query_lower = std::regex_replace(query_lower, std::regex("\\bor\\s+replace\\s+"), "");
		OpenIVMUtils::RemoveRedundantWhitespaces(query_lower);
	}

	// Extract REFRESH EVERY clause before structural rewrite (strips it from the query)
	int64_t refresh_interval = OpenIVMUtils::ExtractRefreshInterval(query_lower);
	OPENIVM_DEBUG_PRINT("[CREATE MV] Refresh interval: %lld seconds\n", (long long)refresh_interval);

	OpenIVMUtils::ReplaceMaterializedView(query_lower);
	// All other rewrites (DISTINCT, AVG, LEFT JOIN key, aggregate aliases) are done
	// at the plan level in IVMPlanFunction via IVMPlanRewrite + LPTS.
	OPENIVM_DEBUG_PRINT("[CREATE MV] After structural rewrite: %s\n", query_lower.c_str());

	Parser p;
	p.ParseQuery(query_lower);

	auto parse_data =
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(p.statements[0]), true, refresh_interval);
	dynamic_cast<IVMParseData &>(*parse_data).is_replace = is_replace;
	return ParserExtensionParseResult(std::move(parse_data));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	// CREATE MATERIALIZED VIEW stores a relation. Physical insertion order is not
	// semantically observable unless users query with ORDER BY, so keep OpenIVM's
	// whole execution path on DuckDB's lower-memory unordered mode.
	ClientConfig::GetConfig(context).user_settings.SetUserSetting(PreserveInsertionOrderSetting::SettingIndex,
	                                                             Value::BOOLEAN(false));
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	ParserExtensionPlanResult result;

	if (ivm_parse_data.plan) {
		Connection con(*context.db.get());

		// Capture the current catalog/schema from the originating context. IVMDDLBindFunction
		// creates a fresh Connection that reflects the DatabaseInstance's physical default
		// catalog (not the session's USE setting). We only inject "USE catalog.schema" when
		// the session's active catalog differs from that physical default — e.g. when DuckLake
		// ("dl") is active but the file DB ("rewriter_benchmark_sf1") is the physical default.
		string current_catalog;
		string current_schema;
		{
			auto &sp = ClientData::Get(context).catalog_search_path;
			auto def = sp->GetDefault();
			current_catalog = def.catalog;
			current_schema = def.schema.empty() ? "main" : def.schema;
		}
		// Query the physical default by running SELECT current_database() on the fresh `con`
		// (created above without any USE, so it reflects the DB's true default, not the session).
		string default_db;
		string default_schema = "main";
		{
			auto res = con.Query("SELECT current_database()");
			if (!res->HasError() && res->RowCount() > 0) {
				default_db = res->GetValue(0, 0).ToString();
			}
			auto schema_res = con.Query("SELECT current_schema()");
			if (!schema_res->HasError() && schema_res->RowCount() > 0) {
				default_schema = schema_res->GetValue(0, 0).ToString();
			}
		}
		string default_catalog_schema = KeywordHelper::WriteOptionallyQuoted(default_db) + "." +
		                                KeywordHelper::WriteOptionallyQuoted(default_schema);
		string current_catalog_schema = KeywordHelper::WriteOptionallyQuoted(current_catalog) + "." +
		                                KeywordHelper::WriteOptionallyQuoted(current_schema);

		// Handle ALTER MATERIALIZED VIEW — just execute the metadata UPDATE
		if (!ivm_parse_data.alter_sql.empty()) {
			auto r = con.Query(ivm_parse_data.alter_sql);
			if (r->HasError()) {
				throw CatalogException("Failed to alter materialized view: " + r->GetError());
			}
			// Return via the DDL executor with no DDL to run (the UPDATE already executed)
			result.function =
			    TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction, IVMFunction::IVMInit);
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			return result;
		}

		// PAC compatibility boundary: internal planning uses a fresh connection, so
		// forward PAC settings when that extension is loaded in the caller session.
		bool pac_loaded = IsPacLoaded(context);
		ForwardPacSettingsIfLoaded(context, con);

		auto full_view_name = OpenIVMUtils::ExtractTableName(statement->query);
		bool statement_needs_original_sql_for_lpts = QueryNeedsOriginalSqlForLpts(statement->query);
		// Keep the user's raw AS-query as the source of truth for original-SQL fallback.
		// Do not recover this from DuckDB's parsed QueryNode::ToString(): that path is a
		// best-effort pretty-printer and has segfaulted on set-operation query nodes with
		// incomplete CTE/query internals. LPTS remains the normalized serializer below
		// for supported logical plans; this string is only the safe fallback input.
		auto original_view_query = OpenIVMUtils::ExtractViewQuery(statement->query);

		// Split catalog-qualified name (e.g. "dl.mv_totals") into prefix and bare name.
		// The target prefix is used only for the user-facing view. DuckLake-backed MVs keep
		// OpenIVM's internal state in the physical default catalog so CREATE/REFRESH does not
		// need to atomically write DuckLake metadata and DuckDB catalog objects together.
		string view_catalog_prefix; // e.g. "dl." or "" for default catalog
		string view_name;           // bare name without catalog, e.g. "mv_totals"
		auto dot_pos = full_view_name.rfind('.');
		if (dot_pos != string::npos) {
			view_catalog_prefix = QuoteQualifiedPrefix(full_view_name.substr(0, dot_pos + 1));
			view_name = full_view_name.substr(dot_pos + 1);
		} else {
			view_name = full_view_name;
			// When the MV name is unqualified but the session is in a non-default catalog
			// (e.g. USE dl.main), explicitly qualify so data/view tables land in dl rather
			// than the physical default. Metadata tables (unqualified) stay in the physical
			// default — PRAGMA ivm() always uses a fresh connection without USE.
			if (!current_catalog.empty() && current_catalog != default_db) {
				view_catalog_prefix = QualifiedTablePrefix(current_catalog, current_schema);
			}
		}
		string internal_catalog_prefix = view_catalog_prefix;
		if (!view_catalog_prefix.empty() && default_db != "memory") {
			internal_catalog_prefix = QualifiedTablePrefix(default_db, default_schema);
		}
		string data_table = IVMTableNames::DataTableName(view_name);
		string qdt = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table);
		string qvn = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
		string view_query = original_view_query; // will be overwritten by LPTS for DDL
		string top_k_suffix;                     // ORDER BY … LIMIT k, appended to the CREATE VIEW

		// Apply the session's active catalog to `con` so unqualified table references in the
		// MV query resolve in the user's catalog (e.g. `dl.main`) rather than the physical
		// default. Without this, `CREATE MATERIALIZED VIEW mv AS SELECT * FROM WAREHOUSE`
		// issued under `USE dl.main` fails during planning with "Table WAREHOUSE does not
		// exist" because the fresh connection resolves against the physical-default catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			con.Query("USE " + current_catalog + "." + current_schema);
		}

		if (!ivm_parse_data.is_replace) {
			// Fail before registering cleanup DDL. Otherwise a duplicate CREATE attempt
			// can fail on the pre-existing backing table and then cleanup would drop the
			// original MV's user-facing view/data table.
			if (RelationExists(con, qvn) || RelationExists(con, qdt)) {
				throw CatalogException("Table with name \"" + view_name + "\" already exists!");
			}
		}

		// Use con for planning — sees all committed state from previous bind-phase DDL
		con.BeginTransaction();
		// GetTableNames binds the query internally. For MV queries that DuckDB's binder
		// can't evaluate out-of-context (e.g. multi-column `(a, b) IN (SELECT x, y FROM t)`
		// triggers an ARRAY-sublink path that rejects the 2-column subquery), the call
		// throws. Catch and use an empty table_names — the later ExtractViewQuery path
		// re-derives what it needs from the plan.
		unordered_set<string> table_names;
		try {
			table_names = con.GetTableNames(statement->query);
		} catch (const std::exception &e) {
			OPENIVM_DEBUG_PRINT("[CREATE MV] GetTableNames failed: %s — continuing\n", e.what());
		}

		// Plan the full CREATE TABLE AS SELECT statement (for plan walking)
		Planner planner(*con.context);
		planner.CreatePlan(statement->Copy());
		auto plan = std::move(planner.plan);

		// Inline CTEs in this plan too so AnalyzePlan / find_group_cols / HasLeftJoin
		// walks see the folded structure. DuckDB's binder defaults to
		// CTE_MATERIALIZE_ALWAYS, which makes CTEInlining bail — relax to DEFAULT first.
		// (The SELECT-only `select_plan` below does the same for LPTS serialization.)
		InlineCtesIfPresent(context, *planner.binder, plan);

		unordered_map<string, OpenIVMSourceTableInfo> source_table_info;
		CollectSourceTables(plan.get(), source_table_info);
		if (!source_table_info.empty()) {
			table_names.clear();
			for (const auto &entry : source_table_info) {
				table_names.insert(entry.second.table_name);
			}
		}
		unordered_map<string, OpenIVMDuckLakeTableInfo> dl_table_info_for_classification;
		CollectDuckLakeTables(plan.get(), current_catalog, dl_table_info_for_classification);
		unordered_map<string, OpenIVMDeltaTableInfo> delta_table_info_for_classification;
		CollectDeltaTables(plan.get(), *con.context, delta_table_info_for_classification);
		bool has_delta_source_for_classification = !delta_table_info_for_classification.empty();
		if (!has_delta_source_for_classification) {
			for (const auto &table_name : table_names) {
				auto delta_catalog_result =
				    con.Query("SELECT type FROM duckdb_databases() WHERE lower(database_name) = lower('" +
				              OpenIVMUtils::EscapeSingleQuotes(table_name) + "') AND type = 'delta'");
				if (!delta_catalog_result->HasError() && delta_catalog_result->RowCount() > 0) {
					has_delta_source_for_classification = true;
					break;
				}
			}
		}

		// Plan the raw SELECT query separately for IVM plan rewrite + LPTS conversion
		vector<string> output_names;
		string having_predicate;    // HAVING predicate as SQL (for VIEW WHERE clause, empty if no HAVING)
		bool lpts_fallback = false; // set when LPTS can't serialize the plan and we fall back to SQL
		bool original_has_repeated_cte_ref_under_join = false;
		{
			Parser select_parser;
			select_parser.ParseQuery(original_view_query);
			Planner select_planner(*con.context);
			select_planner.CreatePlan(std::move(select_parser.statements[0]));
			auto select_plan = std::move(select_planner.plan);
			original_has_repeated_cte_ref_under_join = HasRepeatedCteRefUnderJoin(select_plan.get());

			// Inline CTEs so the refresh rewriter sees one maintainable operator tree.
			// Query-bound CTEs become LOGICAL_MATERIALIZED_CTE + LOGICAL_CTE_REF nodes
			// in the bound plan; CTEInlining rewrites small/non-recursive ones into
			// direct subqueries. Running the full DuckDB optimizer here would also
			// reorder joins / push filters, which conflicts with IVM's subsequent plan
			// rewrites — so we run only the CTE-inlining pass, and only when a CTE
			// reference actually appears in the plan (the optimizer isn't always a
			// no-op on CTE-free plans — e.g. it can rewrite DISTINCT subqueries in ways
			// that confuse the downstream structural rewrites).
			// DuckDB's binder sets LogicalMaterializedCTE::materialize to
			// CTE_MATERIALIZE_ALWAYS by default. CTEInlining bails early on ALWAYS
			// and leaves the CTE as a materialized node. The refresh path has no
			// delta-consolidation rule for LOGICAL_CTE_REF, so relax every CTE to
			// CTE_MATERIALIZE_DEFAULT before inlining. Single-ref CTEs always inline;
			// multi-ref CTEs inline when they're cheap and don't end in an aggregate
			// that would be wastefully re-materialized.
			InlineCtesIfPresent(context, *select_planner.binder, select_plan);

			// Apply IVM plan rewrites (DISTINCT → GROUP BY + COUNT, AVG → SUM + COUNT, LEFT JOIN key)
			IVMPlanRewrite(context, *select_planner.binder, select_plan, select_planner.names);

			output_names = PrepareOutputNames(select_plan.get(), select_planner.names);
			// Strip HAVING filter from plan — data table stores all groups.
			// The predicate is extracted as SQL (using output aliases) for the VIEW WHERE clause.
			having_predicate = StripHavingFilter(select_plan, output_names);

			// For aggregate+top-k: extract the ORDER BY/LIMIT suffix, then strip the top-k
			// wrapper(s) so LPTS serializes only the inner aggregate query. The suffix is
			// appended to the CREATE VIEW definition so ORDER BY LIMIT is applied at read
			// time over the fully-maintained data table.
			//
			// Two plan shapes depending on whether top_n optimizer ran:
			//   (A) LOGICAL_TOP_N → child  [top_n disabled by LPTS — never reached here]
			//   (B) LOGICAL_LIMIT → LOGICAL_ORDER_BY → child  [LPTS active — common case]
			//
			// Projection top-k uses the same split: maintain the unlimited projection in
			// the data table and apply ORDER BY ... LIMIT in the user-facing view.
			{
				LogicalOperator *limit_node = nullptr;
				LogicalOperator *order_node = nullptr;

				if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_TOP_N) {
					// Shape (A): fused top-n node
					limit_node = select_plan.get();
					order_node = select_plan.get(); // same node holds both orders + limit
				} else if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_LIMIT &&
				           !select_plan->children.empty() &&
				           select_plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
					// Shape (B): separate LIMIT + ORDER_BY nodes
					limit_node = select_plan.get();
					order_node = select_plan->children[0].get();
				}

				if (limit_node) {
					// Strip top-k unconditionally. The data table stores the unlimited result,
					// while the user-facing view applies ORDER BY ... LIMIT at read time.
					if (limit_node->type == LogicalOperatorType::LOGICAL_TOP_N) {
						auto &top_n = limit_node->Cast<LogicalTopN>();
						top_k_suffix = BuildTopKSuffix(top_n.orders, top_n.limit, top_n.offset, output_names);
						select_plan = std::move(select_plan->children[0]);
					} else {
						// Shape (B): extract from the two separate nodes
						auto &order_op = order_node->Cast<LogicalOrder>();
						auto &limit_op = limit_node->Cast<LogicalLimit>();
						idx_t lval = 0;
						idx_t oval = 0;
						if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
							lval = limit_op.limit_val.GetConstantValue();
						}
						if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
							oval = limit_op.offset_val.GetConstantValue();
						}
						top_k_suffix = BuildTopKSuffix(order_op.orders, lval, oval, output_names);
						// Strip: select_plan = LOGICAL_ORDER_BY's child
						select_plan = std::move(select_plan->children[0]->children[0]);
					}
					OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped top-k wrapper, suffix='%s'\n", top_k_suffix.c_str());
				}
			}

			// Strip a standalone ORDER_BY at the top of select_plan (e.g. DISTINCT + ORDER BY
			// without LIMIT, or simple projection + ORDER BY). The data table stores unordered
			// rows; the suffix is appended to the CREATE VIEW instead.
			if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_ORDER_BY && top_k_suffix.empty() &&
			    !select_plan->children.empty()) {
				auto &order_op = select_plan->Cast<LogicalOrder>();
				top_k_suffix = BuildTopKSuffix(order_op.orders, 0, 0, output_names);
				select_plan = std::move(select_plan->children[0]);
				OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped standalone ORDER_BY, suffix='%s'\n", top_k_suffix.c_str());
			}
			AnnotateDeltaScansForLpts(select_plan.get());

			if (statement_needs_original_sql_for_lpts || QueryNeedsOriginalSqlForLpts(original_view_query) ||
			    HasUnannotatedDeltaScan(select_plan.get())) {
				view_query = original_view_query;
				lpts_fallback = true;
				OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS can't round-trip this construct — using original SQL: %s\n",
				                    view_query.c_str());
			} else {
				try {
					auto ast = LogicalPlanToAst(*con.context, select_plan);
					auto cte_list = AstToCteList(*ast);
					view_query = cte_list->ToQuery(true, output_names);
					if (!view_query.empty() && view_query.back() == ';') {
						view_query.pop_back();
					}
					StringUtil::Trim(view_query);
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS view query: %s\n", view_query.c_str());
				} catch (const std::exception &e) {
					// LPTS doesn't support all operators (e.g., WINDOW). Fall back to original SQL.
					// This is fine for partition-recompute views that don't need LPTS-rewritten queries.
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS fallback (%s) to original query: %s\n", e.what(),
					                    view_query.c_str());
				} catch (...) {
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS fallback (unknown exception) to original query: %s\n",
					                    view_query.c_str());
				}
			}
			// For views that LPTS silently mis-serializes (GROUPING SETS / ROLLUP / CUBE
			// → plain GROUP BY; STRUCT_PACK field names → tN_col aliases; etc.), detect
			// structurally and prefer the original SQL. Those constructs never need the
			// LPTS-rewritten form anyway — they're maintained by recompute-style paths
			// using the original SQL, so the rewriter-rule path (which needs LPTS) isn't used.
			{
				if (PlanNeedsOriginalSqlForLpts(select_plan.get())) {
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS can't round-trip this construct — using original SQL: %s\n",
					                    view_query.c_str());
				}
			}
		}
		con.Rollback();

		OPENIVM_DEBUG_PRINT("[CREATE MV] View name: %s\n", view_name.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] View query: %s\n", view_query.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Logical plan:\n%s\n", plan->ToString().c_str());

		// Normalize FILTER aggregates in the full plan before analysis so the checker
		// sees CASE expressions instead of raw FILTER and doesn't set ivm_compatible=false.
		// (IVMPlanRewrite already rewrote select_plan for the LPTS view_query above.)
		RewriteAggregateFilters(context, plan);

		// Single-pass plan analysis: validates IVM compatibility AND extracts metadata
		auto analysis = AnalyzePlan(plan.get());
		MVClassificationState classification(analysis);
		if (analysis.found_delim_join && !classification.found_aggregation && !analysis.found_single_join) {
			// Preserve DuckDB's dependent/DELIM_JOIN plan shape for refresh. LPTS can
			// round-trip lateral table functions, but its CTE-normalized SQL lowers them
			// into ordinary joins/table-function scans; that bypasses IvmDelimJoinRule
			// and sends the refresh plan through the generic N-way join rule instead.
			view_query = original_view_query;
			lpts_fallback = true;
		}
		if (classification.found_filtered_list) {
			view_query = original_view_query;
			lpts_fallback = true;
			OPENIVM_DEBUG_PRINT("[CREATE MV] LIST FILTER requires original SQL for group-recompute: %s\n",
			                    view_query.c_str());
		}
		// Derive GROUP BY column names by walking the plan's projection above the aggregate.
		// The projection maps GROUP BY bindings (group_index, i) to SELECT-list aliases — these
		// aliases are the actual column names in the data table. Plan-walk aliases on agg.groups
		// are unreliable for CASE/COALESCE expressions.
		// For DISTINCT views, the checker already extracts aggregate_columns from distinct_targets.
		size_t group_count = analysis.group_count;
		idx_t group_index = analysis.group_index;
		vector<string> aggregate_columns;
		bool join_key_group_fallback = false;
		// DISTINCT is only the MV's grouping when its targets actually appear in the MV's
		// output columns. An inner-subquery DISTINCT (e.g. `SELECT COUNT(*) FROM (SELECT
		// DISTINCT x FROM t)`, or a CTE like `WITH cte AS (SELECT DISTINCT x FROM t) ...`)
		// exposes columns that never reach the data table, so its targets can't drive
		// AGGREGATE_GROUP classification or the unique index.
		bool distinct_at_top = false;
		if (classification.found_distinct && !analysis.aggregate_columns.empty() && !output_names.empty()) {
			unordered_set<string> output_lc;
			for (auto &n : output_names) {
				string lc = n;
				std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
				output_lc.insert(lc);
			}
			distinct_at_top = true;
			for (auto &t : analysis.aggregate_columns) {
				string lc = t;
				std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
				if (!output_lc.count(lc)) {
					distinct_at_top = false;
					break;
				}
			}
		}
		// Detect UNION / UNION ALL over aggregates once, shared by the group_cols block below
		// (to skip key extraction) and the classification chain (to route to RECOMPUTE).
		// True only when the UNION node is an ancestor of the aggregate (union-of-aggs pattern).
		// For agg-over-union (e.g. SELECT ... FROM (t1 UNION ALL t2) GROUP BY ...) the
		// UNION is a descendant of the aggregate — that is a normal AGGREGATE_GROUP view.
		bool has_union_over_agg = classification.found_aggregation && HasUnionBeforeAggregate(plan.get());
		bool union_distinct_over_agg = classification.found_distinct && distinct_at_top && has_union_over_agg;

		if (union_distinct_over_agg) {
			aggregate_columns =
			    DeriveGroupColumnNames(plan.get(), group_index, group_count, output_names, has_union_over_agg);
		} else if (distinct_at_top) {
			aggregate_columns = std::move(analysis.aggregate_columns);
		} else if (classification.found_distinct && analysis.aggregate_columns.empty()) {
			// Plain DISTINCT (no explicit targets) — trust the checker.
			aggregate_columns = std::move(analysis.aggregate_columns);
		} else if (group_count > 0 && group_index != DConstants::INVALID_INDEX) {
			auto group_names_list =
			    DeriveGroupColumnNames(plan.get(), group_index, group_count, output_names, has_union_over_agg);
			for (auto &name : group_names_list) {
				aggregate_columns.push_back(name);
			}
		}

		// LATERAL/correlated subquery aggregates are represented by DEPENDENT/DELIM joins.
		// The inner aggregate's group binding is not projected as a top-level GROUP BY, so
		// the normal group-column derivation above leaves aggregate_columns empty and would
		// misclassify the MV as a scalar SIMPLE_AGGREGATE. For this shape, the visible
		// non-aggregate output columns are the affected-key columns that scope
		// GROUP_RECOMPUTE.
		bool delim_aggregate_group_fallback = false;
		if (analysis.found_delim_join && classification.found_aggregation && aggregate_columns.empty() &&
		    output_names.size() > analysis.aggregate_types.size()) {
			idx_t key_count = output_names.size() - analysis.aggregate_types.size();
			for (idx_t i = 0; i < key_count; i++) {
				if (!output_names[i].empty() && !IVMTableNames::IsInternalColumn(output_names[i])) {
					aggregate_columns.push_back(output_names[i]);
				}
			}
			delim_aggregate_group_fallback = !aggregate_columns.empty();
			if (delim_aggregate_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] DELIM/DEPENDENT aggregate: using %zu visible key columns for "
				                    "GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		bool scalar_delim_projection_group_fallback = false;
		if (analysis.found_delim_join && classification.found_single_join && !classification.found_aggregation &&
		    aggregate_columns.empty()) {
			auto delim_key_names = DeriveScalarDelimKeyColumnNames(plan.get(), output_names);
			for (auto &name : delim_key_names) {
				aggregate_columns.push_back(name);
			}
			scalar_delim_projection_group_fallback = !aggregate_columns.empty();
			if (scalar_delim_projection_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Scalar DELIM/DEPENDENT projection: using %zu visible key columns "
				                    "for GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		bool nested_aggregate_group_fallback = false;
		if (classification.found_nested_aggregate && aggregate_columns.empty()) {
			auto nested_group_names = DeriveAggregateGroupColumnNames(plan.get(), output_names, false);
			for (auto &name : nested_group_names) {
				if (!IVMTableNames::IsInternalColumn(name)) {
					aggregate_columns.push_back(name);
				}
			}
			nested_aggregate_group_fallback = !aggregate_columns.empty();
			if (nested_aggregate_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Nested aggregate: using %zu visible inner group columns for "
				                    "GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		bool repeated_cte_aggregate_group_fallback = false;
		if (classification.found_aggregation && aggregate_columns.empty()) {
			auto cte_group_names = DeriveAggregateGroupColumnNames(plan.get(), output_names, true);
			for (auto &name : cte_group_names) {
				if (!IVMTableNames::IsInternalColumn(name)) {
					aggregate_columns.push_back(name);
				}
			}
			repeated_cte_aggregate_group_fallback = !aggregate_columns.empty();
			if (repeated_cte_aggregate_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Repeated CTE aggregate under join: using %zu visible group columns "
				                    "for GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		// Deduplicate aggregate_columns the same way we deduped output_names above.
		// When the user writes `SELECT DISTINCT w_id, w_id` (or groups twice on the
		// same column), the data table has columns `w_id, w_id_1` after the output_names
		// dedup — the unique index and MERGE ON/UPDATE SET clauses read group names
		// from aggregate_columns, so they must match the data table's deduped names.
		// Without this, the second occurrence gets treated as an aggregate column in
		// `UPDATE SET w_id_1 = v.w_id_1 + d.w_id_1` and values are summed instead of matched.
		{
			unordered_set<string> seen_group;
			for (auto &name : aggregate_columns) {
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
		auto aggregate_types = std::move(analysis.aggregate_types);
		auto window_partition_columns = std::move(analysis.window_partition_columns);
		ResolveWindowPartitionOutputNames(plan.get(), window_partition_columns, output_names);

		// Fallback group-key extraction for inner-aggregate-in-join patterns.
		// When the top-level SELECT joins against a subquery/CTE that performs GROUP BY,
		// find_group_cols fails (the inner aggregate's group_index isn't referenced by the
		// outer projection — the join key comes from the non-aggregate arm). Recover group
		// keys by matching the outermost JOIN's condition BCRs against the top projection.
		//
		// Use GROUP_RECOMPUTE for these views: AGGREGATE_GROUP would try to SUM pass-through
		// attributes (e.g. W_YTD from WAREHOUSE) as if they were aggregate results.
		if (classification.found_join && group_count > 0 && !has_union_over_agg) {
			// plan root is LOGICAL_CREATE_TABLE; descend to find the SELECT output
			// projection and outermost comparison join.
			auto *top_proj_ptr = FindFirstProjection(plan.get());
			auto *cjoin = FindFirstComparisonJoin(plan.get());
			if (top_proj_ptr && cjoin) {
				// Collect (table_index, column_index) pairs that appear in join conditions.
				unordered_map<idx_t, unordered_set<idx_t>> join_key_cols;
				for (auto &cond : cjoin->conditions) {
					AddJoinKeyColumn(cond.left, join_key_cols);
					AddJoinKeyColumn(cond.right, join_key_cols);
				}
				// Match those bindings against the top projection's output expressions.
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
					if (!IVMTableNames::IsInternalColumn(col_name)) {
						bool exists = false;
						for (auto &existing : aggregate_columns) {
							if (StringUtil::CIEquals(existing, col_name)) {
								exists = true;
								break;
							}
						}
						if (!exists) {
							aggregate_columns.push_back(col_name);
							join_key_group_fallback = true;
						}
					}
				}
			}
		}
		if (classification.found_join && classification.found_aggregation && !aggregate_columns.empty()) {
			ResolveAggregateGroupColumnsThroughJoinKeys(plan.get(), aggregate_columns, output_names);
		}

		bool join_aggregate_projection_fallback = false;
		if (classification.found_join && classification.found_aggregation && !aggregate_columns.empty()) {
			idx_t expected_linear_outputs = aggregate_columns.size() + aggregate_types.size();
			join_aggregate_projection_fallback = output_names.size() > expected_linear_outputs;
			if (join_aggregate_projection_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Join-over-aggregate exposes %zu columns but only %zu are "
				                    "group/aggregate outputs — using GROUP_RECOMPUTE\n",
				                    output_names.size(), expected_linear_outputs);
			}
		}

		// Window over join: non-DuckLake delta tables may not expose the partition key for
		// every changed source, so native window recompute could miss partitions. DuckLake
		// can compare the old/new view result at source snapshots, so it can safely keep
		// partition keys even for multi-source window joins.
		bool all_sources_are_ducklake = !table_names.empty();
		if (all_sources_are_ducklake) {
			for (const auto &table_name : table_names) {
				string table_lc = table_name;
				std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
				               [](unsigned char c) { return std::tolower(c); });
				bool is_ducklake_scan =
				    dl_table_info_for_classification.find(table_lc) != dl_table_info_for_classification.end();
				// DuckLake views created by OpenIVM expose a DuckLake catalog view over an
				// internal physical _ivm_data_* table. When DuckDB expands such a view while
				// planning a chained MV, the scan is physical even though the source's change
				// tracking is still DuckLake-backed.
				bool is_ducklake_mv_backing =
				    !view_catalog_prefix.empty() && StringUtil::StartsWith(table_name, ivm::DATA_TABLE_PREFIX);
				if (!is_ducklake_scan && !is_ducklake_mv_backing) {
					all_sources_are_ducklake = false;
					break;
				}
			}
		}
		bool single_source_window_join =
		    classification.found_window && classification.found_join && table_names.size() == 1;
		if (classification.found_window && classification.found_join && !single_source_window_join &&
		    !all_sources_are_ducklake) {
			window_partition_columns.clear();
		}

		// LEFT/RIGHT/OUTER JOIN aggregate with a non-trivial aggregate argument
		// (e.g. `SUM(COALESCE(h.x, 0))`, `AVG(1)`, `SUM(CASE …)`) or a non-BCR
		// projection wrapping an aggregate output (e.g. `COALESCE(SUM(…), 0)`)
		// breaks the Larson & Zhou MERGE logic: the NULL-semantics shortcut that
		// resets right-side aggregates to NULL when match_count=0 produces the
		// wrong value when the stored expression would evaluate to the COALESCE
		// default (0). Route these to group-recompute by setting found_minmax so
		// the view is compiled with has_minmax=true. Detection runs only when
		// the view already has a LEFT/RIGHT/OUTER JOIN and an AGGREGATE.
		//   query_1502: COALESCE(SUM(ol.OL_QUANTITY), 0) AS ordered
		//   query_1696/1699: SUM(COALESCE(h.H_AMOUNT, 0))
		//   query_1746/1749: SUM(COALESCE(1, 0)), AVG(1)
		if ((classification.found_left_join || classification.found_full_outer) && classification.found_aggregation) {
			if (OuterJoinAggregateNeedsRecompute(plan.get(), analysis.group_index)) {
				OPENIVM_DEBUG_PRINT(
				    "[CREATE MV] LEFT/OUTER JOIN aggregate with computed aggregate or projection wrapper — "
				    "using group-recompute (found_minmax=true)\n");
				classification.found_minmax = true;
			}
		}

		// FULL OUTER JOIN with aggregate (above OR below) cannot be safely maintained
		// via inclusion-exclusion deltas: outer-side null-padded rows don't survive
		// deletion deltas correctly, so SUM/COUNT over them drift. Force RECOMPUTE
		// regardless of how the aggregate's expressions are structured.
		// Catches q1353/q1355 (FULL OUTER JOIN + GROUP BY + SUM).
		bool has_full_outer_aggregate = classification.found_full_outer && classification.found_aggregation;

		// Materialized CTE referenced 2+ times below a JOIN: the planner emits one
		// base-table scan inside the CTE and shares it via CTE_REF nodes, so the
		// IvmJoinRule sees a single physical scan instead of an N-way self-join.
		// Inclusion-exclusion can't generate the right delta terms — rows for the
		// shared scan aren't replicated across both join sides. Route to RECOMPUTE.
		// Catches ducklake_0240 (CTE referenced twice on both sides of an INNER JOIN).
		bool has_cte_self_join = HasRepeatedCteRefUnderJoin(plan.get());
		bool has_unsupported_set_operation = HasUnsupportedSetOperation(plan.get());
		bool has_unsupported_incremental_construct = QueryHasUnsupportedIncrementalConstruct(original_view_query);
		if (has_unsupported_set_operation || has_unsupported_incremental_construct) {
			// These views are maintained by full refresh, so store the user's query directly.
			// The CREATE-time IVM rewrites can add hidden columns for incremental paths (e.g.
			// LEFT JOIN match keys) that do not survive SQL set-operation arity rules.
			view_query = original_view_query;
			lpts_fallback = true;
		}

		IVMType ivm_type;
		// Populated by ExtractInnerDistinct when classified as DISTINCT_INCREMENTAL.
		vector<string> distinct_extracted_cols;
		string distinct_extracted_input_sql;
		string distinct_extracted_source;
		string distinct_extracted_filter;
		// Outer-aggregate spec for the v0 aux-state pipeline. Single SUM(<arg>) only —
		// any other shape (multiple SUMs, AVG, COUNT, etc.) demotes to GROUP_RECOMPUTE.
		// `sum_arg` is the column name from the DISTINCT input (one of distinct_cols).
		// `sum_out` is the user-facing output column name in the data table.
		string distinct_sum_arg;
		string distinct_sum_out;
		SemiAntiExtract semi_anti_extract;
		vector<string> semi_anti_left_cols;

		if (has_unsupported_set_operation || has_unsupported_incremental_construct) {
			ivm_type = IVMType::FULL_REFRESH;
		} else if (classification.found_window) {
			// Window functions use partition-level recompute (not full IVM, but better than full refresh)
			ivm_type = IVMType::WINDOW_PARTITION;
		} else if (classification.found_grouping_sets) {
			// ROLLUP / CUBE / GROUPING SETS produce multiple grouping sets per row;
			// maintain them by recomputing the grouping-set keys touched by source
			// deltas.
			ivm_type = aggregate_columns.empty() ? IVMType::FULL_REFRESH : IVMType::GROUP_RECOMPUTE;
		} else if (classification.found_semi_anti_join && classification.found_aggregation) {
			// SEMI/ANTI joins are thresholded by match-count transitions. The aux-state path below
			// supports projection/filter stacks over one left base table; aggregates over SEMI/ANTI
			// need a separate transition-to-aggregate compiler. Use full recompute until that lands.
			ivm_type = IVMType::FULL_REFRESH;
		} else if (classification.found_semi_anti_join && !classification.found_aggregation) {
			if (ExtractSemiAntiQuery(original_view_query, semi_anti_extract)) {
				string left_table_name = LastIdentifierPart(semi_anti_extract.left_table);
				auto col_result =
				    con.Query("SELECT column_name FROM information_schema.columns WHERE lower(table_name) = lower('" +
				              OpenIVMUtils::EscapeSingleQuotes(left_table_name) + "') AND table_schema = '" +
				              OpenIVMUtils::EscapeSingleQuotes(current_schema) + "' ORDER BY ordinal_position");
				auto add_semi_anti_left_col = [&](const string &col_name) {
					for (auto &existing : semi_anti_left_cols) {
						if (StringUtil::CIEquals(existing, col_name)) {
							return;
						}
					}
					semi_anti_left_cols.push_back(col_name);
				};
				if (!semi_anti_extract.output_cols.empty()) {
					for (auto &col : semi_anti_extract.output_cols) {
						add_semi_anti_left_col(col);
					}
					// The aux state must evaluate the semi/anti predicate on refresh. If the
					// MV projects only a subset of the left table, keep the base columns as
					// hidden aux-state key columns as well (e.g. output C_ID/C_LAST, predicate
					// uses C_W_ID). The user-facing MV still emits only output_cols.
					if (semi_anti_extract.left_table.find('(') == string::npos && !col_result->HasError() &&
					    col_result->RowCount() > 0) {
						for (idx_t i = 0; i < col_result->RowCount(); i++) {
							add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
						}
					}
					ivm_type = semi_anti_left_cols.empty() ? IVMType::FULL_REFRESH : IVMType::SEMI_ANTI_RECOMPUTE;
				} else if (!col_result->HasError() && col_result->RowCount() > 0) {
					for (idx_t i = 0; i < col_result->RowCount(); i++) {
						add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
					}
					ivm_type = IVMType::SEMI_ANTI_RECOMPUTE;
				} else {
					ivm_type = IVMType::FULL_REFRESH;
				}
			} else {
				ivm_type = IVMType::FULL_REFRESH;
			}
		} else if (!classification.ivm_compatible) {
			ivm_type = IVMType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' uses constructs not supported for incremental maintenance. "
			               "Full refresh will be used.");
		} else if (classification.found_filtered_list && !aggregate_columns.empty()) {
			ivm_type = IVMType::GROUP_RECOMPUTE;
		} else if (classification.found_filtered_list) {
			ivm_type = IVMType::FULL_REFRESH;
		} else if (classification.found_count_distinct && !aggregate_columns.empty()) {
			ivm_type = IVMType::GROUP_RECOMPUTE;
		} else if (classification.found_distinct && !distinct_at_top && classification.found_aggregation) {
			// Inner DISTINCT under an aggregate. Two paths:
			//   - `ivm_distinct_aux_state = true` AND single-source body → DISTINCT_INCREMENTAL.
			//     Maintains per-DISTINCT-tuple count auxiliary state; on refresh emits ±1
			//     only on count transitions across zero (DBSP distinct(R)=sgn(R[t])). Strictly
			//     fewer rows reach the parent aggregate's MERGE than GROUP_RECOMPUTE.
			//   - Otherwise → GROUP_RECOMPUTE: re-evaluate only the outer GROUP BY keys touched
			//     by source deltas. Correctness-equivalent fallback; multi-source views stay
			//     there until the aux-state path can substitute each source independently.
			// Read the flag from the user's ClientContext (`context`), not the local
			// `con` — the local connection is a fresh one and doesn't inherit the
			// caller's session settings.
			Value aux_val;
			bool aux_enabled = false;
			if (context.TryGetCurrentSetting("ivm_distinct_aux_state", aux_val) && !aux_val.IsNull()) {
				aux_enabled = aux_val.GetValue<bool>();
			}
			bool single_source = table_names.size() == 1;
			if (aux_enabled && single_source) {
				ivm_type = IVMType::DISTINCT_INCREMENTAL;
			} else {
				ivm_type = IVMType::GROUP_RECOMPUTE;
			}
			// Try to extract the DISTINCT subquery from the user's original SQL. If the
			// shape isn't recognised (multi-source body, subquery FROM, etc.), demote to
			// GROUP_RECOMPUTE — the aux pipeline is single-source-only in v0.
			if (ivm_type == IVMType::DISTINCT_INCREMENTAL) {
				vector<string> dcols;
				string d_input_sql, d_source, d_filter;
				if (!ExtractInnerDistinct(original_view_query, dcols, d_input_sql, d_source, d_filter)) {
					ivm_type = IVMType::GROUP_RECOMPUTE;
					OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL extractor failed — demoting to "
					                    "GROUP_RECOMPUTE\n");
				} else {
					distinct_extracted_cols = std::move(dcols);
					distinct_extracted_input_sql = std::move(d_input_sql);
					distinct_extracted_source = std::move(d_source);
					distinct_extracted_filter = std::move(d_filter);
				}
			}
			// Walk the rewritten plan for the outer aggregate's expressions. v0 supports
			// exactly one SUM(<arg>) — `_ivm_count_star` (auto-injected by IVMPlanRewrite)
			// is allowed alongside it. Anything else (AVG, COUNT, MIN/MAX, multiple SUMs)
			// demotes back to GROUP_RECOMPUTE.
			if (ivm_type == IVMType::DISTINCT_INCREMENTAL) {
				LogicalAggregate *outer_agg = FindOuterAggregate(plan.get());
				int sum_count = 0;
				bool unsupported_agg = false;
				if (outer_agg) {
					for (auto &expr : outer_agg->expressions) {
						if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
							continue;
						}
						auto &bound = expr->Cast<BoundAggregateExpression>();
						const string &fname = bound.function.name;
						if (fname == "count_star") {
							continue; // injected by IvmPlanRewrite — fine
						}
						if (fname != "sum") {
							unsupported_agg = true;
							break;
						}
						if (bound.children.empty() ||
						    bound.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
							unsupported_agg = true;
							break;
						}
						auto &bcr = bound.children[0]->Cast<BoundColumnRefExpression>();
						distinct_sum_arg = bcr.alias.empty() ? bcr.GetName() : bcr.alias;
						distinct_sum_out = bound.alias;
						sum_count++;
					}
				}
				if (!outer_agg || unsupported_agg || sum_count != 1) {
					ivm_type = IVMType::GROUP_RECOMPUTE;
					distinct_sum_arg.clear();
					distinct_sum_out.clear();
					OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL outer-agg not single-SUM — demoting "
					                    "to GROUP_RECOMPUTE\n");
				} else if (distinct_sum_out.empty()) {
					// `SUM(c) AS s` puts the alias `s` on the SELECT-list BCR above the
					// aggregate, not on the BoundAggregateExpression itself. Recover it
					// from output_names: the SUM output column is the first non-group
					// position (group cols come first in the data table layout).
					if (aggregate_columns.size() < output_names.size()) {
						distinct_sum_out = output_names[aggregate_columns.size()];
					}
				}
			}
		} else if (union_distinct_over_agg && !aggregate_columns.empty()) {
			// UNION DISTINCT over grouped aggregate outputs must recompute by the branch
			// aggregate key, not by the full distinct output tuple. The old aggregate value
			// is no longer discoverable from delta-filtered recompute after an update.
			ivm_type = IVMType::GROUP_RECOMPUTE;
		} else if (classification.found_distinct && distinct_at_top && !aggregate_columns.empty()) {
			ivm_type = IVMType::AGGREGATE_GROUP;
		} else if (classification.found_having && classification.found_aggregation && !aggregate_columns.empty()) {
			ivm_type = IVMType::AGGREGATE_HAVING;
		} else if ((has_union_over_agg || join_key_group_fallback || delim_aggregate_group_fallback ||
		            scalar_delim_projection_group_fallback || join_aggregate_projection_fallback ||
		            nested_aggregate_group_fallback || repeated_cte_aggregate_group_fallback ||
		            (has_delta_source_for_classification && classification.found_join &&
		             classification.found_aggregation) ||
		            classification.found_nested_aggregate) &&
		           !aggregate_columns.empty()) {
			// UNION/UNION ALL over aggregates: group keys extracted positionally from output_names.
			// Inner-aggregate-in-join: group keys are the join-condition columns visible in the
			// top projection. DELIM/DEPENDENT aggregate or scalar-subquery projection: group
			// keys are the visible correlated output columns. Nested aggregate (outer COUNT(*)
			// over inner GROUP BY via CTE): outer COUNT counts inner groups, not source rows,
			// so linear delta is incorrect. These use GROUP_RECOMPUTE — not AGGREGATE_GROUP —
			// because non-key output columns may be pass-through attributes or non-linear
			// functions over inner/correlated expressions.
			ivm_type = IVMType::GROUP_RECOMPUTE;
		} else if (classification.found_aggregation && !aggregate_columns.empty()) {
			ivm_type = IVMType::AGGREGATE_GROUP;
		} else if (classification.found_aggregation && aggregate_columns.empty()) {
			ivm_type = IVMType::SIMPLE_AGGREGATE;
		} else if (classification.found_projection && !classification.found_aggregation) {
			ivm_type = IVMType::SIMPLE_PROJECTION;
		} else {
			ivm_type = IVMType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' has an unrecognized query pattern. Full refresh will be used.");
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Detected IVM type: %s (aggregation=%d, projection=%d, group_cols=%zu)\n",
		                    ivm_type == IVMType::AGGREGATE_GROUP        ? "AGGREGATE_GROUP"
		                    : ivm_type == IVMType::SIMPLE_AGGREGATE     ? "SIMPLE_AGGREGATE"
		                    : ivm_type == IVMType::SIMPLE_PROJECTION    ? "SIMPLE_PROJECTION"
		                    : ivm_type == IVMType::FULL_REFRESH         ? "FULL_REFRESH"
		                    : ivm_type == IVMType::WINDOW_PARTITION     ? "WINDOW_PARTITION"
		                    : ivm_type == IVMType::GROUP_RECOMPUTE      ? "GROUP_RECOMPUTE"
		                    : ivm_type == IVMType::DISTINCT_INCREMENTAL ? "DISTINCT_INCREMENTAL"
		                    : ivm_type == IVMType::SEMI_ANTI_RECOMPUTE  ? "SEMI_ANTI_RECOMPUTE"
		                                                                : "UNKNOWN",
		                    (int)classification.found_aggregation, (int)classification.found_projection,
		                    aggregate_columns.size());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Source tables:");
		for (const auto &t : table_names) {
			OPENIVM_DEBUG_PRINT(" %s", t.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n");

		// Build DDL vector directly in memory
		vector<string> ddl;
		vector<string> cleanup_ddl;
		vector<string> metadata_ddl;
		vector<string> aux_metadata_ddl;
		auto add_cleanup = [&](const string &query) {
			cleanup_ddl.push_back(string(OPENIVM_DDL_CLEANUP_PREFIX) + query);
		};

		// --- System tables DDL ---
		// Matcher metadata columns (signature_hash..nullified_columns_json) stay
		// NULL unless ivm_enable_view_matching=true; populated by Stage I wiring.
		ddl.push_back("create table if not exists " + string(ivm::VIEWS_TABLE) +
		              " (view_name varchar primary key, sql_string varchar, type tinyint,"
		              " has_minmax boolean default false, has_left_join boolean default false,"
		              " last_update timestamp, refresh_interval bigint default null,"
		              " refresh_in_progress boolean default false,"
		              " group_columns varchar default null,"
		              " aggregate_types varchar default null,"
		              " having_predicate varchar default null,"
		              " has_full_outer boolean default false,"
		              " full_outer_join_cols varchar default null,"
		              " signature_hash ubigint default null,"
		              " canonical_plan_blob blob default null,"
		              " output_columns_json varchar default null,"
		              " predicate_summary_json varchar default null,"
		              " fd_summary_json varchar default null,"
		              " source_tables_json varchar default null,"
		              " aggregate_decomposition_json varchar default null,"
		              " nullified_columns_json varchar default null,"
		              " distinct_aux_meta_json varchar default null,"
		              " semi_anti_aux_meta_json varchar default null)");
		// Forward-compat ALTER for existing DBs that pre-date `distinct_aux_meta_json`
		// (the CREATE IF NOT EXISTS above is a no-op when the table exists with the older schema).
		ddl.push_back("alter table " + string(ivm::VIEWS_TABLE) +
		              " add column if not exists distinct_aux_meta_json varchar default null");
		ddl.push_back("alter table " + string(ivm::VIEWS_TABLE) +
		              " add column if not exists semi_anti_aux_meta_json varchar default null");
		if (!ivm_parse_data.is_replace) {
			string escaped_view_name = OpenIVMUtils::EscapeSingleQuotes(view_name);
			string escaped_data_table = OpenIVMUtils::EscapeSingleQuotes(IVMTableNames::DataTableName(view_name));
			string stale_mv_condition =
			    "view_name = '" + escaped_view_name +
			    "' AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + escaped_view_name +
			    "') AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + escaped_data_table +
			    "')";
			// CREATE MV executes as multiple catalog statements. If a process dies or loses
			// a DuckDB file lock after writing metadata but before creating the physical
			// DuckLake/default-catalog objects, a retry should clean that stale row rather
			// than report a misleading duplicate MV.
			ddl.push_back("DELETE FROM " + string(ivm::VIEWS_TABLE) + " WHERE " + stale_mv_condition);
			ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + string(ivm::VIEWS_TABLE) +
			              " WHERE view_name = '" + escaped_view_name +
			              "') THEN error('Duplicate key: materialized view \"" + escaped_view_name +
			              "\" already exists') ELSE NULL END");
		}

		// Refresh hooks: extensions can register custom SQL to run on MV refresh
		// mode: 'replace' (instead of ivm), 'before' (before ivm), 'after' (after ivm)
		ddl.push_back("create table if not exists _duckdb_ivm_refresh_hooks"
		              " (view_name varchar primary key, hook_sql varchar not null,"
		              " mode varchar not null default 'after')");

		ddl.push_back("create table if not exists " + string(ivm::DELTA_TABLES_TABLE) +
		              " (view_name varchar, table_name varchar, last_update timestamp,"
		              " catalog_type varchar default 'duckdb', last_snapshot_id bigint default null,"
		              " last_refresh_ts timestamp default null,"
		              " pending_row_estimate bigint default null,"
		              " pending_estimate_ts timestamp default null,"
		              " source_catalog varchar default null,"
		              " source_schema varchar default null,"
		              " source_path varchar default null,"
		              " primary key(view_name, table_name))");
		// Backfill for existing databases without the columns (added post-release).
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists last_refresh_ts timestamp default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists pending_row_estimate bigint default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists pending_estimate_ts timestamp default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists source_catalog varchar default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists source_schema varchar default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists source_path varchar default null");

		// Refresh history: stores execution stats for learned cost model calibration.
		// Stage A.5 adds `strategy` (default 'incremental') for per-strategy regression.
		ddl.push_back("create table if not exists " + string(ivm::HISTORY_TABLE) +
		              " (view_name varchar, refresh_timestamp timestamp default current_timestamp,"
		              " method varchar, ivm_compute_est double, ivm_upsert_est double,"
		              " recompute_compute_est double, recompute_replace_est double,"
		              " actual_duration_ms bigint,"
		              " strategy varchar default 'incremental',"
		              " primary key(view_name, refresh_timestamp))");
		ddl.push_back("alter table " + string(ivm::HISTORY_TABLE) +
		              " add column if not exists strategy varchar default 'incremental'");
		ddl.push_back("create table if not exists " + string(ivm::PROFILE_TABLE) +
		              " (refresh_id varchar, view_name varchar,"
		              " profile_timestamp timestamp default current_timestamp,"
		              " step_order integer, step_name varchar, duration_ms bigint, detail varchar,"
		              " primary key(refresh_id, step_order))");

		// --- OR REPLACE: drop old MV if it exists ---
		if (ivm_parse_data.is_replace) {
			string qvn_drop = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
			string qdt_drop =
			    internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(view_name));
			string qdv_drop =
			    internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
			// Drop the user-facing VIEW, data table, and delta view table
			ddl.push_back("DROP VIEW IF EXISTS " + qvn_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdt_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdv_drop);
			// Clean metadata (the INSERT OR REPLACE below handles _duckdb_ivm_views)
			ddl.push_back("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
			              OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			ddl.push_back("DELETE FROM " + string(ivm::HISTORY_TABLE) + " WHERE view_name = '" +
			              OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		// Store the LPTS query in metadata — it has hidden columns (DISTINCT count, AVG sum/count,
		// LEFT JOIN key) and preserves user column names.
		string refresh_val = ivm_parse_data.refresh_interval > 0 ? to_string(ivm_parse_data.refresh_interval) : "null";
		// Store GROUP BY or PARTITION BY columns (mutually exclusive in our type system).
		// For WINDOW_PARTITION, store the PARTITION BY columns so the upsert compiler
		// can identify affected partitions from deltas.
		string group_cols_val = "null";
		auto &cols_to_store = classification.found_window ? window_partition_columns : aggregate_columns;
		if (!cols_to_store.empty()) {
			group_cols_val = "'";
			for (size_t i = 0; i < cols_to_store.size(); i++) {
				if (i > 0) {
					group_cols_val += ",";
				}
				group_cols_val += OpenIVMUtils::EscapeSingleQuotes(cols_to_store[i]);
			}
			group_cols_val += "'";
		}
		// Store per-column aggregate types for insert-only MIN/MAX optimization
		string agg_types_val = "null";
		if (!aggregate_types.empty()) {
			agg_types_val = "'";
			for (size_t i = 0; i < aggregate_types.size(); i++) {
				if (i > 0) {
					agg_types_val += ",";
				}
				agg_types_val += aggregate_types[i];
			}
			agg_types_val += "'";
		}
		string having_val =
		    having_predicate.empty() ? "null" : "'" + OpenIVMUtils::EscapeSingleQuotes(having_predicate) + "'";

		// Extract FULL OUTER JOIN condition: "left_table:left_col,right_table:right_col"
		string full_outer_join_cols_val = "null";
		if (classification.found_full_outer) {
			string foj_cols = ExtractFullOuterJoinMetadata(plan.get());
			if (!foj_cols.empty()) {
				full_outer_join_cols_val = "'" + OpenIVMUtils::EscapeSingleQuotes(foj_cols) + "'";
			}
		}

		// 10 trailing NULLs: 8 matcher metadata columns + distinct/semi-anti aux metadata.
		// Matcher metadata is populated by the Stage I block below when
		// ivm_enable_view_matching=true. distinct_aux_meta_json is populated by a
		// follow-up UPDATE if ivm_type == DISTINCT_INCREMENTAL and the extractor
		// recognised the DISTINCT shape.
		metadata_ddl.push_back("insert or replace into " + string(ivm::VIEWS_TABLE) + " values ('" + view_name +
		                       "', '" + OpenIVMUtils::EscapeSingleQuotes(view_query) + "', " +
		                       to_string((int)ivm_type) + ", " + (classification.found_minmax ? "true" : "false") +
		                       ", " + (classification.found_left_join ? "true" : "false") + ", now(), " + refresh_val +
		                       ", false, " + group_cols_val + ", " + agg_types_val + ", " + having_val + ", " +
		                       (classification.found_full_outer ? "true" : "false") + ", " + full_outer_join_cols_val +
		                       ", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

		Value match_flag_val;
		bool view_matching_enabled = context.TryGetCurrentSetting("ivm_enable_view_matching", match_flag_val) &&
		                             !match_flag_val.IsNull() && BooleanValue::Get(match_flag_val);
		if (view_matching_enabled) {
			// table_names may include _ivm_data_<x> when this MV reads from
			// another MV (DuckDB binds the user-facing view to its data table).
			// Strip the prefix so source_tables_json reflects user-facing names
			// and the dependency-edge lookup hits a registered MV row.
			vector<string> sorted_tables;
			sorted_tables.reserve(table_names.size());
			for (const auto &t : table_names) {
				if (StringUtil::StartsWith(t, ivm::DATA_TABLE_PREFIX)) {
					sorted_tables.push_back(t.substr(strlen(ivm::DATA_TABLE_PREFIX)));
				} else {
					sorted_tables.push_back(t);
				}
			}
			std::sort(sorted_tables.begin(), sorted_tables.end());
			// Build JSON without inner escaping; outer EscapeSingleQuotes runs
			// once when the SQL is assembled.
			string src_json = "[";
			for (idx_t i = 0; i < sorted_tables.size(); i++) {
				if (i) {
					src_json += ",";
				}
				src_json += "\"" + sorted_tables[i] + "\"";
			}
			src_json += "]";
			metadata_ddl.push_back("UPDATE " + string(ivm::VIEWS_TABLE) + " SET source_tables_json = '" +
			                       OpenIVMUtils::EscapeSingleQuotes(src_json) + "' WHERE view_name = '" +
			                       OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			// Replace any prior edges for this child, then re-emit. INSERTs are
			// conditional on the source being a registered MV (the SELECT
			// returns zero rows for non-MV sources).
			metadata_ddl.push_back("DELETE FROM " + string(ivm::MV_DEPS_TABLE) + " WHERE child_view = '" +
			                       OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			for (const auto &t : sorted_tables) {
				metadata_ddl.push_back("INSERT INTO " + string(ivm::MV_DEPS_TABLE) +
				                       " (parent_view, child_view, edge_kind) SELECT view_name, '" +
				                       OpenIVMUtils::EscapeSingleQuotes(view_name) + "', 'direct' FROM " +
				                       string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
				                       OpenIVMUtils::EscapeSingleQuotes(t) + "'");
			}
		}

		// DISTINCT_INCREMENTAL: create the per-tuple count auxiliary table and store its
		// metadata so refresh-time can find the source SQL, the column list, and the aux
		// table name. The aux table is populated from the (DISTINCT-stripped) input SQL
		// at CREATE time; refresh-time MERGE keeps it in sync with delta multiplicities.
		if (ivm_type == IVMType::DISTINCT_INCREMENTAL) {
			string aux_table = "_ivm_distinct_count_" + view_name;
			string cols_csv;
			for (size_t i = 0; i < distinct_extracted_cols.size(); i++) {
				if (i > 0) {
					cols_csv += ", ";
				}
				cols_csv += distinct_extracted_cols[i];
			}
			// CREATE+POPULATE the aux table from the extracted DISTINCT input SQL.
			// _count is a signed BIGINT (deltas can transiently push it negative during
			// concurrent refreshes; the post-update DELETE drops rows whose count <= 0).
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as select " + cols_csv +
			                    ", count(*)::BIGINT as _count from (" + distinct_extracted_input_sql + ") group by " +
			                    cols_csv;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));
			// Build a JSON metadata blob that the refresh-time compile reads back.
			string cols_json = "[";
			for (size_t i = 0; i < distinct_extracted_cols.size(); i++) {
				if (i > 0) {
					cols_json += ",";
				}
				cols_json += JsonQuote(distinct_extracted_cols[i]);
			}
			cols_json += "]";
			string meta_json = "{\"aux_table\":" + JsonQuote(aux_table) + ",\"cols\":" + cols_json +
			                   ",\"input_sql\":" + JsonQuote(distinct_extracted_input_sql) +
			                   ",\"source\":" + JsonQuote(distinct_extracted_source) +
			                   ",\"filter\":" + JsonQuote(distinct_extracted_filter) +
			                   ",\"sum_arg\":" + JsonQuote(distinct_sum_arg) +
			                   ",\"sum_out\":" + JsonQuote(distinct_sum_out) + "}";
			aux_metadata_ddl.push_back("UPDATE " + string(ivm::VIEWS_TABLE) + " SET distinct_aux_meta_json = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(meta_json) + "' WHERE view_name = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		if (ivm_type == IVMType::SEMI_ANTI_RECOMPUTE) {
			string aux_table = "_ivm_semi_anti_state_" + view_name;
			auto qualify_source_table = [&](const string &table_name) {
				if (current_catalog.empty() || current_catalog == default_db || table_name.find('.') != string::npos ||
				    table_name.find('(') != string::npos) {
					return table_name;
				}
				return current_catalog + "." + current_schema + "." + table_name;
			};
			string left_source_table = qualify_source_table(semi_anti_extract.left_table);
			string right_source_table = qualify_source_table(semi_anti_extract.right_table);
			string left_cols_csv;
			string left_source_select;
			string left_cols_qualified;
			string left_cols_lc;
			string left_cols_mc;
			string lc_mc_match;
			vector<string> semi_anti_left_exprs;
			for (size_t i = 0; i < semi_anti_left_cols.size(); i++) {
				if (i > 0) {
					left_cols_csv += ", ";
					left_source_select += ", ";
					left_cols_qualified += ", ";
					left_cols_lc += ", ";
					left_cols_mc += ", ";
					lc_mc_match += " AND ";
				}
				string qcol = KeywordHelper::WriteOptionallyQuoted(semi_anti_left_cols[i]);
				left_cols_csv += qcol;
				string source_expr = semi_anti_extract.left_alias + "." + qcol;
				for (size_t j = 0; j < semi_anti_extract.output_cols.size(); j++) {
					if (StringUtil::CIEquals(semi_anti_extract.output_cols[j], semi_anti_left_cols[i]) &&
					    j < semi_anti_extract.output_exprs.size()) {
						source_expr = semi_anti_extract.output_exprs[j];
						break;
					}
				}
				semi_anti_left_exprs.push_back(source_expr);
				left_source_select += source_expr + " AS " + qcol;
				left_cols_qualified += semi_anti_extract.left_alias + "." + qcol;
				left_cols_lc += "lc." + qcol;
				left_cols_mc += "mc." + qcol;
				lc_mc_match += "lc." + qcol + " IS NOT DISTINCT FROM mc." + qcol;
			}
			string left_source_filter;
			if (!semi_anti_extract.post_filter.empty()) {
				left_source_filter = " WHERE " + semi_anti_extract.post_filter;
			}
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as with left_source as (select " +
			                    left_source_select + " from " + left_source_table + " " + semi_anti_extract.left_alias +
			                    left_source_filter + "), left_counts as (select " + left_cols_csv +
			                    ", count(*)::BIGINT as _left_count from left_source group by " + left_cols_csv +
			                    "), match_counts as (select " + left_cols_qualified +
			                    ", count(*)::BIGINT as _match_count from (select distinct " + left_cols_csv +
			                    " from left_source) " + semi_anti_extract.left_alias + " join " + right_source_table +
			                    " " + semi_anti_extract.right_alias + " on " + semi_anti_extract.predicate +
			                    " group by " + left_cols_qualified + ") select " + left_cols_lc +
			                    ", lc._left_count, coalesce(mc._match_count, 0)::BIGINT as _match_count from "
			                    "left_counts lc left join match_counts mc on " +
			                    lc_mc_match;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));

			string left_cols_json = "[";
			string left_exprs_json = "[";
			for (size_t i = 0; i < semi_anti_left_cols.size(); i++) {
				if (i > 0) {
					left_cols_json += ",";
					left_exprs_json += ",";
				}
				left_cols_json += JsonQuote(semi_anti_left_cols[i]);
				left_exprs_json += JsonQuote(semi_anti_left_exprs[i]);
			}
			left_cols_json += "]";
			left_exprs_json += "]";
			string output_cols_json = "[";
			for (size_t i = 0; i < semi_anti_extract.output_cols.size(); i++) {
				if (i > 0) {
					output_cols_json += ",";
				}
				output_cols_json += JsonQuote(semi_anti_extract.output_cols[i]);
			}
			output_cols_json += "]";
			string meta_json =
			    "{\"aux_table\":" + JsonQuote(aux_table) + ",\"join_type\":" + JsonQuote(semi_anti_extract.join_type) +
			    ",\"left_table\":" + JsonQuote(semi_anti_extract.left_table) +
			    ",\"left_alias\":" + JsonQuote(semi_anti_extract.left_alias) +
			    ",\"right_table\":" + JsonQuote(semi_anti_extract.right_table) +
			    ",\"right_alias\":" + JsonQuote(semi_anti_extract.right_alias) +
			    ",\"predicate\":" + JsonQuote(semi_anti_extract.predicate) +
			    ",\"post_filter\":" + JsonQuote(semi_anti_extract.post_filter) + ",\"left_cols\":" + left_cols_json +
			    ",\"left_exprs\":" + left_exprs_json + ",\"output_cols\":" + output_cols_json + "}";
			aux_metadata_ddl.push_back("UPDATE " + string(ivm::VIEWS_TABLE) + " SET semi_anti_aux_meta_json = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(meta_json) + "' WHERE view_name = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		// Classify each base table by catalog type.
		// Externally versioned tables use source-native version tracking; DuckDB tables use delta tables.
		//
		// Catalog::GetEntry inside BeginTransaction() cannot see DuckLake entries:
		// DuckLake requires its own transaction protocol. Walk the logical plan's
		// DUCKLAKE_SCAN nodes instead — same approach used in ducklake_join.cpp.
		unordered_map<string, OpenIVMDuckLakeTableInfo> dl_table_info; // keyed by lowercased name
		CollectDuckLakeTables(plan.get(), current_catalog, dl_table_info);
		unordered_map<string, OpenIVMDeltaTableInfo> delta_table_info; // keyed by lowercased name
		CollectDeltaTables(plan.get(), *con.context, delta_table_info);

		unordered_set<string> ducklake_tables;
		unordered_set<string> externally_versioned_tables;
		auto delta_version_from_path = [&](const string &path) -> int64_t {
			auto version_result =
			    con.Query("SELECT COALESCE(MAX(CAST(regexp_extract(file, '([0-9]+)\\.json$', 1) AS BIGINT)), -1) "
			              "FROM glob('" +
			              OpenIVMUtils::EscapeSingleQuotes(path) + "/_delta_log/*.json')");
			if (version_result->HasError() || version_result->RowCount() == 0 ||
			    version_result->GetValue(0, 0).IsNull()) {
				return -1;
			}
			return version_result->GetValue(0, 0).GetValue<int64_t>();
		};
		// Single snapshot query per DuckLake catalog (all tables share the same snapshot).
		string dl_snapshot_val = "null";
		if (!dl_table_info.empty()) {
			// Use the first entry's catalog — all source tables in one MV share one catalog.
			string cat = dl_table_info.begin()->second.catalog_name;
			auto snap_result = con.Query("SELECT id FROM " + cat + ".current_snapshot()");
			if (!snap_result->HasError() && snap_result->RowCount() > 0) {
				dl_snapshot_val = snap_result->GetValue(0, 0).ToString();
			}
		}

		vector<string> source_metadata_ddl;
		unordered_set<string> inserted_meta_table_names;
		for (const auto &table_name : table_names) {
			string catalog_type = "duckdb";
			string snapshot_val = "null";
			string meta_table_name = OpenIVMUtils::DeltaName(table_name);
			string source_catalog_val = current_catalog.empty() ? "memory" : current_catalog;
			string source_schema_val = current_schema.empty() ? "main" : current_schema;
			string source_path_val = "";

			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			auto source_info_it = source_table_info.find(table_name);
			if (source_info_it != source_table_info.end()) {
				source_catalog_val = source_info_it->second.catalog_name;
				source_schema_val = source_info_it->second.schema_name;
			}
			auto it = dl_table_info.find(table_lc);
			if (it != dl_table_info.end()) {
				catalog_type = "ducklake";
				meta_table_name = it->second.table_name; // case-preserved name
				ducklake_tables.insert(it->second.table_name);
				ducklake_tables.insert(table_name); // also insert SQL-parsed name
				externally_versioned_tables.insert(it->second.table_name);
				externally_versioned_tables.insert(table_name); // also insert SQL-parsed name
				externally_versioned_tables.insert(table_lc);
				snapshot_val = dl_snapshot_val;
				source_catalog_val = it->second.catalog_name;
				source_schema_val = it->second.schema_name;
				OPENIVM_DEBUG_PRINT("[CREATE MV] DuckLake table '%s' → meta_name='%s', snap=%s\n", table_name.c_str(),
				                    meta_table_name.c_str(), snapshot_val.c_str());
			}
			auto delta_it = delta_table_info.find(table_lc);
			if (delta_it != delta_table_info.end()) {
				catalog_type = "delta";
				meta_table_name = delta_it->second.table_name; // case-preserved name
				externally_versioned_tables.insert(delta_it->second.table_name);
				externally_versioned_tables.insert(table_name); // also insert SQL-parsed name
				externally_versioned_tables.insert(table_lc);
				if (delta_it->second.version >= 0) {
					snapshot_val = to_string(delta_it->second.version);
				}
				source_catalog_val = delta_it->second.catalog_name;
				source_schema_val = delta_it->second.schema_name;
				source_path_val = delta_it->second.path;
				auto version = delta_version_from_path(source_path_val);
				if (version >= 0) {
					snapshot_val = to_string(version);
				}
				OPENIVM_DEBUG_PRINT("[CREATE MV] Delta table '%s' → meta_name='%s', version=%s path='%s'\n",
				                    table_name.c_str(), meta_table_name.c_str(), snapshot_val.c_str(),
				                    source_path_val.c_str());
			} else {
				auto delta_catalog_result = con.Query("SELECT database_name, path, type FROM duckdb_databases() "
				                                      "WHERE lower(database_name) = lower('" +
				                                      OpenIVMUtils::EscapeSingleQuotes(table_name) + "')");
				if (!delta_catalog_result->HasError() && delta_catalog_result->RowCount() > 0 &&
				    !delta_catalog_result->GetValue(2, 0).IsNull() &&
				    delta_catalog_result->GetValue(2, 0).ToString() == "delta") {
					catalog_type = "delta";
					meta_table_name = delta_catalog_result->GetValue(0, 0).ToString();
					externally_versioned_tables.insert(meta_table_name);
					externally_versioned_tables.insert(table_name);
					externally_versioned_tables.insert(table_lc);
					source_catalog_val = delta_catalog_result->GetValue(0, 0).ToString();
					source_schema_val = "main";
					source_path_val = delta_catalog_result->GetValue(1, 0).ToString();
					auto version = delta_version_from_path(source_path_val);
					if (version >= 0) {
						snapshot_val = to_string(version);
					}
					OPENIVM_DEBUG_PRINT("[CREATE MV] Delta catalog '%s' → meta_name='%s', version=%s path='%s'\n",
					                    table_name.c_str(), meta_table_name.c_str(), snapshot_val.c_str(),
					                    source_path_val.c_str());
				}
			}

			// A single physical source can appear under multiple logical names after planning.
			// DuckLake chained views are the common case: the query can contain both the
			// user-facing MV name and its backing _ivm_data_* table. Metadata is keyed by
			// (view_name, table_name), so emit only one dependency row for the canonical
			// metadata table name after the DuckLake mapping above.
			if (!inserted_meta_table_names.insert(meta_table_name).second) {
				continue;
			}

			source_metadata_ddl.push_back(
			    "insert or replace into " + string(ivm::DELTA_TABLES_TABLE) +
			    " (view_name, table_name, last_update, catalog_type, last_snapshot_id, "
			    "last_refresh_ts, source_catalog, source_schema, source_path) "
			    "values ('" +
			    view_name + "', '" + OpenIVMUtils::EscapeSingleQuotes(meta_table_name) + "', now(), '" + catalog_type +
			    "', " + snapshot_val + ", now(), '" + OpenIVMUtils::EscapeSingleQuotes(source_catalog_val) + "', '" +
			    OpenIVMUtils::EscapeSingleQuotes(source_schema_val) + "', " +
			    (source_path_val.empty() ? "NULL" : "'" + OpenIVMUtils::EscapeSingleQuotes(source_path_val) + "'") +
			    ")");
		}

		// --- Compiled DDL (MV creation, delta tables, delta view) ---
		// Physical data table stores all columns (including _ivm_* internal cols).
		// DuckLake-targeted MVs publish only the user-facing view in DuckLake. Internal
		// state remains in the physical default catalog, because DuckDB/DuckLake cannot
		// roll back one atomic transaction across both catalogs.
			string stage_table = "_ivm_stage_" + view_name;
			string qstage =
			    QualifiedTablePrefix(default_db, default_schema) + KeywordHelper::WriteOptionallyQuoted(stage_table);
			if (BoolSetting(context, "ivm_explain_initial_load")) {
				// This diagnostic intentionally reports the exact first heavy statement that
				// CREATE MV will run. DuckLake-targeted MVs may stage into the physical
				// catalog before copying to _ivm_data_*, so they are not always a single CTAS.
				if (!current_catalog.empty() && current_catalog != default_db) {
					con.Query("USE " + current_catalog_schema);
				}
				string initial_load_statement = view_catalog_prefix.empty() ? "CREATE TABLE " + qdt + " AS " + view_query
				                                                            : "CREATE TABLE " + qstage + " AS " + view_query;
				string diagnostic;
				diagnostic += "\n[OpenIVM initial-load diagnostic]\n";
				diagnostic += "view_name: " + view_name + "\n";
				diagnostic += "ivm_type: " + string(ParserIVMTypeName(ivm_type)) + "\n";
				diagnostic += "lpts_fallback: " + string(lpts_fallback ? "true" : "false") + "\n";
				diagnostic += "uses_staging_table: " + string(view_catalog_prefix.empty() ? "false" : "true") + "\n";
				diagnostic += "initial_load_statement:\n" + initial_load_statement + "\n\n";
				if (!view_catalog_prefix.empty()) {
					diagnostic += "follow_up_copy_statement:\nCREATE TABLE " + qdt + " AS SELECT * FROM " + qstage +
					              "\n\n";
				}
				diagnostic += "original_view_query:\n" + original_view_query + "\n\n";
				diagnostic += "generated_view_query:\n" + view_query + "\n\n";
				diagnostic += ExplainInitialLoadQuery(con, "EXPLAIN original_view_query:", original_view_query);
				diagnostic += ExplainInitialLoadQuery(con, "EXPLAIN generated_view_query:", view_query);
				diagnostic += ExplainInitialLoadQuery(con, "EXPLAIN initial_load_statement:", initial_load_statement);
				Printer::Print(diagnostic);

				Value files_path_val;
				if (context.TryGetCurrentSetting("ivm_files_path", files_path_val) && !files_path_val.IsNull()) {
					OpenIVMUtils::WriteFile(files_path_val.ToString() + "/ivm_initial_load_explain_" + view_name + ".txt",
					                        false, diagnostic);
				}
				if (BoolSetting(context, "ivm_explain_initial_load_only")) {
					result.function = TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction,
					                                IVMFunction::IVMInit);
					result.requires_valid_transaction = true;
					result.return_type = StatementReturnType::QUERY_RESULT;
					return result;
				}
			}
			// The view_query may contain unqualified base-table references (e.g. `FROM WAREHOUSE`
			// when the user wrote the MV under `USE dl.main`). The DDL executor's fresh
			// Connection starts in the physical-default catalog, so apply USE before CREATE
		// TABLE AS so those unqualified names resolve in the MV's catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			ddl.push_back("use " + current_catalog_schema);
		}
		if (!view_catalog_prefix.empty()) {
			// DuckLake can self-lock when one statement reads DuckLake tables and commits
			// a DuckLake CTAS. Stage the SELECT into the physical default DB first, then
			// persist OpenIVM's internal data table in that same physical catalog. The
			// DuckLake write is limited to the final user-facing view metadata.
			ddl.push_back("DROP TABLE IF EXISTS " + qstage);
			ddl.push_back("create table " + qstage + " as " + view_query);
			ddl.push_back(OPENIVM_DDL_RECONNECT);
			ddl.push_back("use " + default_catalog_schema);
			ddl.push_back("create table " + qdt + " as select * from " + qstage);
			ddl.push_back("DROP TABLE IF EXISTS " + qstage);
		} else {
			ddl.push_back("create table " + qdt + " as " + view_query);
		}
		add_cleanup("DROP VIEW IF EXISTS " + qvn);
		add_cleanup("DROP TABLE IF EXISTS " + qdt);
		add_cleanup("DROP TABLE IF EXISTS " + qstage);
		if (pac_loaded) {
			ddl.push_back("SET pac_check = false");
			ddl.push_back("SET pac_rewrite = false");
		}

		// User-facing VIEW hides internal _ivm_* columns via EXCLUDE.
		// If LPTS fell back to the original SQL, the data table has only the
		// user-visible columns — no `_ivm_*` columns even if the rewritten plan
		// would have added them via AVG/STDDEV decomposition. Skip the EXCLUDE
		// list in that case; otherwise CREATE VIEW fails on nonexistent columns.
		{
			// Collect internal column names from the LPTS output
			vector<string> internal_cols;
			if (!lpts_fallback) {
				for (auto &name : output_names) {
					if (IVMTableNames::IsInternalColumn(name)) {
						internal_cols.push_back(name);
					}
				}
			}
			string having_where = having_predicate.empty() ? "" : " where " + having_predicate;
			// For aggregate+top-k the VIEW appends ORDER BY … LIMIT k after the HAVING WHERE.
			// The data table stores ALL groups; the ORDER BY LIMIT is applied at read time.
			string view_tail = having_where + (top_k_suffix.empty() ? "" : " " + top_k_suffix);
			if (internal_cols.empty()) {
				ddl.push_back("create view " + qvn + " as select * from " + qdt + view_tail);
			} else {
				string exclude_list;
				for (size_t i = 0; i < internal_cols.size(); i++) {
					if (i > 0) {
						exclude_list += ", ";
					}
					exclude_list += internal_cols[i];
				}
				ddl.push_back("create view " + qvn + " as select * exclude (" + exclude_list + ") from " + qdt +
				              view_tail);
			}
		}

		for (const auto &table_name : table_names) {
			// Externally versioned tables don't need synthetic delta tables — source-native
			// version tracking supplies changes. `externally_versioned_tables` stores the
			// catalog-normalized (lowercase) name, so
			// compare against a normalized copy of the SQL-parsed name.
			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			if (externally_versioned_tables.count(table_name) || externally_versioned_tables.count(table_lc)) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Skipping delta table for externally versioned table '%s'\n",
				                    table_name.c_str());
				continue;
			}

			Value catalog_value;
			Value schema_value;
			auto source_it = source_table_info.find(table_name);
			if (source_it != source_table_info.end()) {
				catalog_value = Value(source_it->second.catalog_name);
				schema_value = Value(source_it->second.schema_name);
			}

			if (catalog_value.IsNull() && !context.db->config.options.database_path.empty()) {
				// Look up the catalog name for this table via Catalog API
				con.BeginTransaction();
				auto entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA,
				                                                  table_name, OnEntryNotFound::RETURN_NULL);
				if (entry) {
					catalog_value = Value(entry->ParentCatalog().GetName());
					schema_value = Value(entry->schema.name);
				}
				con.Rollback();
			}
			if (catalog_value.IsNull()) {
				catalog_value = Value(current_catalog.empty() ? "memory" : current_catalog);
			}

			if (schema_value.IsNull()) {
				schema_value = Value(current_schema.empty() ? "main" : current_schema);
			}

			auto catalog_schema = QualifiedTablePrefix(catalog_value.ToString(), schema_value.ToString());

			ddl.push_back("create table if not exists " + catalog_schema +
			              KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(table_name)) +
			              " as select *, 1::INTEGER as " + string(ivm::MULTIPLICITY_COL) + ", now()::timestamp as " +
			              string(ivm::TIMESTAMP_COL) + " from " + catalog_schema +
			              KeywordHelper::WriteOptionallyQuoted(table_name) + " limit 0");
		}

		// Delta table for the MV — based on the DATA table (has all columns)
		string qdv = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
		ddl.push_back("create table if not exists " + qdv + " as select *, 1::INTEGER as " +
		              string(ivm::MULTIPLICITY_COL) + ", now()::timestamp as " + string(ivm::TIMESTAMP_COL) + " from " +
		              qdt + " limit 0");
		ddl.push_back("alter table " + qdv + " alter " + string(ivm::TIMESTAMP_COL) + " set default now()");
		add_cleanup("DROP TABLE IF EXISTS " + qdv);

		// --- Index DDL (for aggregate group queries) ---
		// DuckLake source scans do not support indexes. DuckLake-targeted MVs store the
		// internal data table in the physical default catalog, but source-side DuckLake
		// change tracking still skips this optional index.
		if ((ivm_type == IVMType::AGGREGATE_GROUP || ivm_type == IVMType::AGGREGATE_HAVING) &&
		    !aggregate_columns.empty() && ducklake_tables.empty() && view_catalog_prefix.empty()) {
			string index_name = KeywordHelper::WriteOptionallyQuoted(data_table + ivm::INDEX_SUFFIX);
			string index_query_view = "create unique index " + index_name + " on " + qdt + "(";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				index_query_view += KeywordHelper::WriteOptionallyQuoted(aggregate_columns[i]);
				if (i != aggregate_columns.size() - 1) {
					index_query_view += ", ";
				}
			}
			index_query_view += ")";
			ddl.push_back(index_query_view);
		}

		// Restore physical-default catalog so subsequent unqualified references to
		// system tables (_duckdb_ivm_delta_tables, etc.) resolve correctly. The USE
		// inserted before `create table qdt as view_query` routed unqualified base
		// tables through the user's catalog; flip back for the metadata UPDATE below.
		if (!current_catalog.empty() && current_catalog != default_db) {
			ddl.push_back("use " + default_catalog_schema);
		}

		// Record source-table metadata only after physical MV objects exist. If a later
		// DuckLake publish fails, the DDL executor removes these rows before the retry.
		ddl.insert(ddl.end(), source_metadata_ddl.begin(), source_metadata_ddl.end());
		add_cleanup("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
		            OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");

		// After all tables are created and populated, update DuckLake snapshot IDs
		// to the current snapshot. This ensures the first refresh only sees changes
		// made AFTER the MV was created (not the initial data load).
		for (const auto &table_name : table_names) {
			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			if (ducklake_tables.count(table_name) || ducklake_tables.count(table_lc)) {
				// Use the DuckLake catalog for current_snapshot() — NOT view_catalog_prefix
				// or current_catalog. For cross-system MVs (native MV reading from dl.*),
				// view_catalog_prefix is empty and current_catalog is the physical-default
				// (e.g. the file DB) which doesn't have `current_snapshot()`.
				// table_names entries may be lowercased by the parser; the metadata row was
				// inserted above with the case-preserved DuckLake name (`dl_table_info`).
				string table_lc_for_lookup = table_name;
				std::transform(table_lc_for_lookup.begin(), table_lc_for_lookup.end(), table_lc_for_lookup.begin(),
				               [](unsigned char c) { return std::tolower(c); });
				auto info_it = dl_table_info.find(table_lc_for_lookup);
				string meta_table_name = (info_it != dl_table_info.end()) ? info_it->second.table_name : table_name;
				string dl_cat_name = (info_it != dl_table_info.end()) ? info_it->second.catalog_name : "dl";
				ddl.push_back("UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = (SELECT id FROM " +
				              dl_cat_name + ".current_snapshot()) WHERE view_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(view_name) + "' AND table_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(meta_table_name) + "'");
			}
		}

		// Publish the MV metadata last. CREATE MV touches both the physical DuckDB catalog
		// and DuckLake's external metadata catalog, so OpenIVM cannot rely on a single
		// cross-catalog transaction. The executor registers cleanup DDL up front and this
		// late publish keeps incomplete attempts out of _duckdb_ivm_views.
		ddl.insert(ddl.end(), metadata_ddl.begin(), metadata_ddl.end());
		ddl.insert(ddl.end(), aux_metadata_ddl.begin(), aux_metadata_ddl.end());
		add_cleanup("DELETE FROM " + string(ivm::MV_DEPS_TABLE) + " WHERE child_view = '" +
		            OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		add_cleanup("DELETE FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
		            OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");

		OPENIVM_DEBUG_PRINT("[CREATE MV] Compiled %lu DDL queries for bind phase\n", (unsigned long)ddl.size());

		// Write reference SQL files if ivm_files_path is set
		Value files_path_val;
		if (context.TryGetCurrentSetting("ivm_files_path", files_path_val) && !files_path_val.IsNull()) {
			string base_path = files_path_val.ToString();
			// System tables DDL (first 3 statements: _duckdb_ivm_views, _duckdb_ivm_refresh_hooks,
			// _duckdb_ivm_delta_tables)
			string system_tables_sql;
			// Compiled queries (everything after the system tables)
			string compiled_sql;
			for (size_t i = 0; i < ddl.size(); i++) {
				if (i < 3) {
					system_tables_sql += ddl[i] + ";\n\n";
				} else {
					compiled_sql += ddl[i] + ";\n\n";
				}
			}
			OpenIVMUtils::WriteFile(base_path + "/ivm_system_tables.sql", false, system_tables_sql);
			OpenIVMUtils::WriteFile(base_path + "/ivm_compiled_queries_" + view_name + ".sql", false, compiled_sql);
		}

		// Pass DDL via result.parameters — the bind function receives them as input.inputs.
		// This replaces the fragile thread-local g_ivm_pending_ddl mechanism.
		for (auto &q : cleanup_ddl) {
			result.parameters.push_back(Value(q));
		}
		for (auto &q : ddl) {
			result.parameters.push_back(Value(q));
		}
	}

	// Return DDL executor table function
	result.function =
	    TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction, IVMFunction::IVMInit);
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}
} // namespace duckdb
