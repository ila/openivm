#include "rules/schema_evolution.hpp"

#include "core/openivm_constants.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/list.hpp"

#include <unordered_set>

namespace duckdb {

static bool NamesMatch(const string &left, const string &right) {
	return StringUtil::CIEquals(SqlUtils::LastIdentifierPart(left), SqlUtils::LastIdentifierPart(right));
}

static vector<string> GetDependentViews(Connection &con, const string &delta_name) {
	vector<string> views;
	auto result = con.Query("SELECT DISTINCT d.view_name FROM " + string(openivm::DELTA_TABLES_TABLE) +
	                        " d WHERE d.table_name = '" + SqlUtils::EscapeValue(delta_name) + "' ORDER BY 1");
	if (result->HasError()) {
		return views;
	}
	for (idx_t i = 0; i < result->RowCount(); i++) {
		views.push_back(result->GetValue(0, i).ToString());
	}
	return views;
}

static void UpdateViewMetadataValue(Connection &con, const string &view_name, const string &column_name,
                                    const string &value) {
	auto result =
	    con.Query("UPDATE " + string(openivm::VIEWS_TABLE) + " SET " + column_name + " = '" +
	              SqlUtils::EscapeValue(value) + "' WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError()) {
		throw CatalogException("Cannot update metadata for materialized view '" + view_name +
		                       "': " + result->GetError());
	}
}

static void AddAlias(unordered_set<string> &aliases, const string &alias) {
	if (!alias.empty()) {
		aliases.insert(StringUtil::Lower(alias));
		aliases.insert(StringUtil::Lower(SqlUtils::LastIdentifierPart(alias)));
	}
}

static void AddLocalTableAliases(TableRef &ref, const string &table_name, unordered_set<string> &all_aliases,
                                 unordered_set<string> &target_aliases) {
	AddAlias(all_aliases, ref.alias);
	if (ref.type == TableReferenceType::JOIN) {
		auto &join = ref.Cast<JoinRef>();
		AddLocalTableAliases(*join.left, table_name, all_aliases, target_aliases);
		AddLocalTableAliases(*join.right, table_name, all_aliases, target_aliases);
		return;
	}
	if (ref.type != TableReferenceType::BASE_TABLE) {
		return;
	}
	auto &base = ref.Cast<BaseTableRef>();
	AddAlias(all_aliases, base.table_name);
	if (!NamesMatch(base.table_name, table_name)) {
		return;
	}
	AddAlias(target_aliases, base.table_name);
	if (!ref.alias.empty()) {
		AddAlias(target_aliases, ref.alias);
	}
}

static unordered_set<string> ApplyLocalAliases(const unordered_set<string> &inherited_target_aliases,
                                               const unordered_set<string> &local_aliases,
                                               const unordered_set<string> &local_target_aliases) {
	auto visible = inherited_target_aliases;
	for (auto &alias : local_aliases) {
		if (!local_target_aliases.count(alias)) {
			visible.erase(alias);
		}
	}
	for (auto &alias : local_target_aliases) {
		visible.insert(alias);
	}
	return visible;
}

static bool ColumnRefTargetsTable(const ColumnRefExpression &ref, const unordered_set<string> &aliases,
                                  bool allow_unqualified) {
	if (ref.column_names.size() == 1) {
		return allow_unqualified;
	}
	for (idx_t i = 0; i + 1 < ref.column_names.size(); i++) {
		if (aliases.count(StringUtil::Lower(ref.column_names[i]))) {
			return true;
		}
	}
	return false;
}

static bool RewriteQueryNodeScoped(QueryNode &node, const string &table_name, const string &old_name,
                                   const string &new_name, const unordered_set<string> &inherited_target_aliases);

static bool RewriteExpressionScoped(unique_ptr<ParsedExpression> &expr, const string &table_name,
                                    const string &old_name, const string &new_name,
                                    const unordered_set<string> &target_aliases, bool allow_unqualified) {
	if (!expr) {
		return false;
	}
	bool changed = false;
	ParsedExpressionIterator::VisitExpressionMutable<SubqueryExpression>(*expr, [&](SubqueryExpression &subquery) {
		if (subquery.subquery && subquery.subquery->node) {
			changed |= RewriteQueryNodeScoped(*subquery.subquery->node, table_name, old_name, new_name, target_aliases);
		}
	});
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(*expr, [&](ColumnRefExpression &ref) {
		if (!ref.column_names.empty() && StringUtil::CIEquals(ref.column_names.back(), old_name) &&
		    ColumnRefTargetsTable(ref, target_aliases, allow_unqualified)) {
			ref.column_names.back() = new_name;
			changed = true;
		}
	});
	return changed;
}

static bool RewriteTableRefScoped(TableRef &ref, const string &table_name, const string &old_name,
                                  const string &new_name, const unordered_set<string> &target_aliases,
                                  bool allow_unqualified) {
	bool changed = false;
	switch (ref.type) {
	case TableReferenceType::EXPRESSION_LIST: {
		auto &el_ref = ref.Cast<ExpressionListRef>();
		for (auto &row : el_ref.values) {
			for (auto &expr : row) {
				changed |=
				    RewriteExpressionScoped(expr, table_name, old_name, new_name, target_aliases, allow_unqualified);
			}
		}
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		changed |= RewriteTableRefScoped(*join.left, table_name, old_name, new_name, target_aliases, allow_unqualified);
		changed |=
		    RewriteTableRefScoped(*join.right, table_name, old_name, new_name, target_aliases, allow_unqualified);
		changed |=
		    RewriteExpressionScoped(join.condition, table_name, old_name, new_name, target_aliases, allow_unqualified);
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref.Cast<PivotRef>();
		changed |=
		    RewriteTableRefScoped(*pivot.source, table_name, old_name, new_name, target_aliases, allow_unqualified);
		for (auto &aggr : pivot.aggregates) {
			changed |= RewriteExpressionScoped(aggr, table_name, old_name, new_name, target_aliases, allow_unqualified);
		}
		for (auto &pivot_col : pivot.pivots) {
			for (auto &expr : pivot_col.pivot_expressions) {
				changed |=
				    RewriteExpressionScoped(expr, table_name, old_name, new_name, target_aliases, allow_unqualified);
			}
			for (auto &entry : pivot_col.entries) {
				changed |= RewriteExpressionScoped(entry.expr, table_name, old_name, new_name, target_aliases,
				                                   allow_unqualified);
			}
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			changed |= RewriteQueryNodeScoped(*subquery.subquery->node, table_name, old_name, new_name, target_aliases);
		}
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &tf_ref = ref.Cast<TableFunctionRef>();
		changed |=
		    RewriteExpressionScoped(tf_ref.function, table_name, old_name, new_name, target_aliases, allow_unqualified);
		if (tf_ref.subquery && tf_ref.subquery->node) {
			changed |= RewriteQueryNodeScoped(*tf_ref.subquery->node, table_name, old_name, new_name, target_aliases);
		}
		break;
	}
	case TableReferenceType::SHOW_REF: {
		auto &show = ref.Cast<ShowRef>();
		if (show.query) {
			changed |= RewriteQueryNodeScoped(*show.query, table_name, old_name, new_name, target_aliases);
		}
		break;
	}
	case TableReferenceType::BASE_TABLE:
	case TableReferenceType::EMPTY_FROM:
	case TableReferenceType::COLUMN_DATA:
	case TableReferenceType::DELIM_GET:
	case TableReferenceType::CTE:
	case TableReferenceType::BOUND_TABLE_REF:
		break;
	case TableReferenceType::INVALID:
		throw InternalException("Invalid table reference while rewriting materialized view schema metadata");
	}
	return changed;
}

static bool RewriteQueryModifiersScoped(QueryNode &node, const string &table_name, const string &old_name,
                                        const string &new_name, const unordered_set<string> &target_aliases,
                                        bool allow_unqualified) {
	bool changed = false;
	ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, [&](unique_ptr<ParsedExpression> &expr) {
		changed |= RewriteExpressionScoped(expr, table_name, old_name, new_name, target_aliases, allow_unqualified);
	});
	return changed;
}

static bool RewriteQueryNodeScoped(QueryNode &node, const string &table_name, const string &old_name,
                                   const string &new_name, const unordered_set<string> &inherited_target_aliases) {
	bool changed = false;
	for (auto &kv : node.cte_map.map) {
		changed |=
		    RewriteQueryNodeScoped(*kv.second->query->node, table_name, old_name, new_name, inherited_target_aliases);
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		unordered_set<string> local_aliases;
		unordered_set<string> local_target_aliases;
		AddLocalTableAliases(*select.from_table, table_name, local_aliases, local_target_aliases);
		auto target_aliases = ApplyLocalAliases(inherited_target_aliases, local_aliases, local_target_aliases);
		bool allow_unqualified = !local_target_aliases.empty();
		for (auto &expr : select.select_list) {
			changed |= RewriteExpressionScoped(expr, table_name, old_name, new_name, target_aliases, allow_unqualified);
		}
		for (auto &expr : select.groups.group_expressions) {
			changed |= RewriteExpressionScoped(expr, table_name, old_name, new_name, target_aliases, allow_unqualified);
		}
		changed |= RewriteExpressionScoped(select.where_clause, table_name, old_name, new_name, target_aliases,
		                                   allow_unqualified);
		changed |=
		    RewriteExpressionScoped(select.having, table_name, old_name, new_name, target_aliases, allow_unqualified);
		changed |=
		    RewriteExpressionScoped(select.qualify, table_name, old_name, new_name, target_aliases, allow_unqualified);
		changed |= RewriteTableRefScoped(*select.from_table, table_name, old_name, new_name, target_aliases,
		                                 allow_unqualified);
		changed |= RewriteQueryModifiersScoped(node, table_name, old_name, new_name, target_aliases, allow_unqualified);
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			changed |= RewriteQueryNodeScoped(*child, table_name, old_name, new_name, inherited_target_aliases);
		}
		changed |= RewriteQueryModifiersScoped(node, table_name, old_name, new_name, inherited_target_aliases, false);
		break;
	}
	default:
		throw NotImplementedException("Query node type not implemented for schema metadata rewrite");
	}
	return changed;
}

static void PreserveTopLevelOutputAliases(SelectStatement &stmt, const vector<string> &output_columns) {
	if (!stmt.node || stmt.node->type != QueryNodeType::SELECT_NODE) {
		return;
	}
	auto &select = stmt.node->Cast<SelectNode>();
	if (select.select_list.size() != output_columns.size()) {
		return;
	}
	for (idx_t i = 0; i < output_columns.size(); i++) {
		select.select_list[i]->alias = output_columns[i];
	}
}

static bool RewriteStoredViewQuery(Connection &con, RefreshMetadata &metadata, const string &view_name,
                                   const string &table_name, const string &old_name, const string &new_name,
                                   bool persist) {
	string sql = metadata.GetViewQuery(view_name);
	if (sql.empty()) {
		return false;
	}
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.empty() || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw CatalogException("Cannot rewrite materialized view '" + view_name +
		                       "' after column rename: stored query is not a SELECT");
	}
	auto &stmt = parser.statements[0]->Cast<SelectStatement>();
	bool changed = RewriteQueryNodeScoped(*stmt.node, table_name, old_name, new_name, unordered_set<string>());
	if (!changed) {
		return false;
	}
	if (!persist) {
		return true;
	}
	PreserveTopLevelOutputAliases(stmt,
	                              metadata.GetTableColumns("", "", IncrementalTableNames::DataTableName(view_name)));
	string rewritten = stmt.ToString();
	UpdateViewMetadataValue(con, view_name, "sql_string", rewritten);
	return true;
}

static bool AuxSourceMatches(const string &source, const string &table_name) {
	return !source.empty() && NamesMatch(source, table_name);
}

static unordered_set<string> SingleQualifier(const string &qualifier) {
	unordered_set<string> qualifiers;
	AddAlias(qualifiers, qualifier);
	return qualifiers;
}

static bool RewriteDistinctAuxMetaFields(RefreshMetadata::DistinctAuxMeta &meta, const string &table_name,
                                         const string &old_name, const string &new_name) {
	if (!AuxSourceMatches(meta.source, table_name)) {
		return false;
	}
	bool changed = false;
	if (meta.source_exprs.size() != meta.cols.size()) {
		meta.source_exprs = meta.cols;
	}
	auto qualifiers = SingleQualifier(meta.source);
	for (auto &expr : meta.source_exprs) {
		changed |= SqlUtils::RewriteColumnReferences(expr, old_name, new_name, qualifiers, true);
	}
	changed |= SqlUtils::RewriteColumnReferences(meta.filter, old_name, new_name, qualifiers, true);
	changed |= SqlUtils::RewriteColumnReferences(meta.input_sql, old_name, new_name, qualifiers, true);
	return changed;
}

static bool RewriteDistinctAuxMeta(Connection &con, RefreshMetadata &metadata, const string &view_name,
                                   const string &table_name, const string &old_name, const string &new_name) {
	RefreshMetadata::DistinctAuxMeta meta;
	if (metadata.GetDistinctAuxMeta(view_name, meta) &&
	    RewriteDistinctAuxMetaFields(meta, table_name, old_name, new_name)) {
		UpdateViewMetadataValue(con, view_name, "distinct_aux_meta_json", RefreshMetadata::DistinctAuxMetaToJson(meta));
		return true;
	}
	return false;
}

static bool RewriteFilteredGroupCountMetaFields(RefreshMetadata::FilteredGroupCountAuxMeta &meta,
                                                const string &table_name, const string &old_name,
                                                const string &new_name) {
	if (!AuxSourceMatches(meta.source, table_name)) {
		return false;
	}
	bool changed = false;
	if (meta.source_group_expr.empty()) {
		meta.source_group_expr = meta.group_col;
	}
	if (meta.source_sum_expr.empty()) {
		meta.source_sum_expr = meta.sum_col;
	}
	auto qualifiers = SingleQualifier(meta.source);
	changed |= SqlUtils::RewriteColumnReferences(meta.source_group_expr, old_name, new_name, qualifiers, true);
	changed |= SqlUtils::RewriteColumnReferences(meta.source_sum_expr, old_name, new_name, qualifiers, true);
	return changed;
}

static bool RewriteFilteredGroupCountMeta(Connection &con, RefreshMetadata &metadata, const string &view_name,
                                          const string &table_name, const string &old_name, const string &new_name) {
	RefreshMetadata::FilteredGroupCountAuxMeta meta;
	if (metadata.GetFilteredGroupCountAuxMeta(view_name, meta) &&
	    RewriteFilteredGroupCountMetaFields(meta, table_name, old_name, new_name)) {
		UpdateViewMetadataValue(con, view_name, "aggregate_decomposition_json",
		                        RefreshMetadata::FilteredGroupCountAuxMetaToJson(meta));
		return true;
	}
	return false;
}

static bool RewriteSemiAntiAuxMetaFields(RefreshMetadata::SemiAntiAuxMeta &meta, const string &table_name,
                                         const string &old_name, const string &new_name,
                                         bool include_stable_left_predicate) {
	bool changed = false;
	if (AuxSourceMatches(meta.left_table, table_name)) {
		auto qualifiers = SingleQualifier(meta.left_alias.empty() ? meta.left_table : meta.left_alias);
		for (auto &expr : meta.left_exprs) {
			changed |= SqlUtils::RewriteColumnReferences(expr, old_name, new_name, qualifiers, true);
		}
		changed |= SqlUtils::RewriteColumnReferences(meta.post_filter, old_name, new_name, qualifiers, true);
		// The left side of predicate is evaluated against stable aux-state columns;
		// left_exprs above maps renamed source columns back to those stable names.
		if (include_stable_left_predicate) {
			changed |= SqlUtils::RewriteColumnReferences(meta.predicate, old_name, new_name, qualifiers, true);
		}
	}
	if (AuxSourceMatches(meta.right_table, table_name)) {
		auto qualifiers = SingleQualifier(meta.right_alias.empty() ? meta.right_table : meta.right_alias);
		changed |= SqlUtils::RewriteColumnReferences(meta.predicate, old_name, new_name, qualifiers, true);
	}
	return changed;
}

static bool RewriteSemiAntiAuxMeta(Connection &con, RefreshMetadata &metadata, const string &view_name,
                                   const string &table_name, const string &old_name, const string &new_name) {
	RefreshMetadata::SemiAntiAuxMeta meta;
	if (metadata.GetSemiAntiAuxMeta(view_name, meta) &&
	    RewriteSemiAntiAuxMetaFields(meta, table_name, old_name, new_name, /*include_stable_left_predicate=*/false)) {
		UpdateViewMetadataValue(con, view_name, "semi_anti_aux_meta_json",
		                        RefreshMetadata::SemiAntiAuxMetaToJson(meta));
		return true;
	}
	return false;
}

static bool RewriteWindowLineageFields(vector<RefreshMetadata::WindowPartitionLineageOp> &ops, const string &table_name,
                                       const string &old_name, const string &new_name) {
	bool changed = false;
	for (auto &op : ops) {
		if (AuxSourceMatches(op.source, table_name) && StringUtil::CIEquals(op.source_col, old_name)) {
			op.source_col = new_name;
			changed = true;
		}
		if (op.kind == "lookup" && AuxSourceMatches(op.lookup, table_name)) {
			if (StringUtil::CIEquals(op.lookup_col, old_name)) {
				op.lookup_col = new_name;
				changed = true;
			}
			if (StringUtil::CIEquals(op.lookup_out, old_name)) {
				op.lookup_out = new_name;
				changed = true;
			}
		}
	}
	return changed;
}

static bool RewriteProjectionLineageFields(RefreshMetadata::ProjectionKeyLineage &lineage, const string &table_name,
                                           const string &old_name, const string &new_name) {
	bool changed = false;
	if (AuxSourceMatches(lineage.key_source, table_name) && StringUtil::CIEquals(lineage.key_col, old_name)) {
		lineage.key_col = new_name;
		changed = true;
	}
	for (auto &arm : lineage.arms) {
		if (AuxSourceMatches(arm.source, table_name) && StringUtil::CIEquals(arm.source_col, old_name)) {
			arm.source_col = new_name;
			changed = true;
		}
		for (auto &step : arm.steps) {
			if (!AuxSourceMatches(step.table, table_name)) {
				continue;
			}
			if (StringUtil::CIEquals(step.lookup_col, old_name)) {
				step.lookup_col = new_name;
				changed = true;
			}
			if (StringUtil::CIEquals(step.lookup_out, old_name)) {
				step.lookup_out = new_name;
				changed = true;
			}
		}
	}
	return changed;
}

static bool RewriteLineageMeta(Connection &con, RefreshMetadata &metadata, const string &view_name,
                               const string &table_name, const string &old_name, const string &new_name) {
	vector<string> entries;
	bool changed = false;
	vector<RefreshMetadata::WindowPartitionLineageOp> window_ops;
	if (metadata.GetWindowPartitionLineage(view_name, window_ops)) {
		changed |= RewriteWindowLineageFields(window_ops, table_name, old_name, new_name);
		entries.push_back(RefreshMetadata::WindowPartitionLineageToJson(window_ops));
	}
	RefreshMetadata::ProjectionKeyLineage projection;
	if (metadata.GetProjectionKeyLineage(view_name, projection)) {
		changed |= RewriteProjectionLineageFields(projection, table_name, old_name, new_name);
		entries.push_back(RefreshMetadata::ProjectionKeyLineageToJson(projection));
	}
	if (changed) {
		string json = BuildRefreshLineageJson(entries);
		UpdateViewMetadataValue(con, view_name, "lineage_json", json);
	}
	return changed;
}

static bool RewriteWindowGroupColumnSources(Connection &con, RefreshMetadata &metadata, const string &view_name,
                                            const string &old_name, const string &new_name) {
	auto group_columns = metadata.GetGroupColumns(view_name);
	if (group_columns.empty()) {
		return false;
	}
	vector<string> rewritten;
	bool changed = false;
	for (auto &token : group_columns) {
		auto pos = token.find('=');
		if (pos != string::npos) {
			string lhs = token.substr(0, pos);
			string rhs = token.substr(pos + 1);
			if (StringUtil::CIEquals(rhs, old_name)) {
				rhs = new_name;
				changed = true;
			}
			rewritten.push_back(lhs + "=" + rhs);
		} else {
			rewritten.push_back(token);
		}
	}
	if (changed) {
		string value = StringUtil::Join(rewritten, rewritten.size(), ",", [](const string &item) { return item; });
		UpdateViewMetadataValue(con, view_name, "group_columns", value);
	}
	return changed;
}

static bool AuxMetadataReferencesColumn(RefreshMetadata &metadata, const string &view_name, const string &table_name,
                                        const string &col_name) {
	RefreshMetadata::DistinctAuxMeta distinct;
	if (metadata.GetDistinctAuxMeta(view_name, distinct) &&
	    RewriteDistinctAuxMetaFields(distinct, table_name, col_name, col_name)) {
		return true;
	}
	RefreshMetadata::FilteredGroupCountAuxMeta filtered;
	if (metadata.GetFilteredGroupCountAuxMeta(view_name, filtered) && AuxSourceMatches(filtered.source, table_name)) {
		if (StringUtil::CIEquals(filtered.group_col, col_name) || StringUtil::CIEquals(filtered.sum_col, col_name) ||
		    RewriteFilteredGroupCountMetaFields(filtered, table_name, col_name, col_name)) {
			return true;
		}
	}
	RefreshMetadata::SemiAntiAuxMeta semi;
	if (metadata.GetSemiAntiAuxMeta(view_name, semi) &&
	    RewriteSemiAntiAuxMetaFields(semi, table_name, col_name, col_name, /*include_stable_left_predicate=*/true)) {
		return true;
	}
	vector<RefreshMetadata::WindowPartitionLineageOp> window_ops;
	if (metadata.GetWindowPartitionLineage(view_name, window_ops) &&
	    RewriteWindowLineageFields(window_ops, table_name, col_name, col_name)) {
		return true;
	}
	RefreshMetadata::ProjectionKeyLineage projection;
	if (metadata.GetProjectionKeyLineage(view_name, projection) &&
	    RewriteProjectionLineageFields(projection, table_name, col_name, col_name)) {
		return true;
	}
	return false;
}

string FirstMVReferencingColumn(Connection &con, const string &delta_name, const string &table_name,
                                const string &col_name) {
	RefreshMetadata metadata(con);
	for (auto &view_name : GetDependentViews(con, delta_name)) {
		if (RewriteStoredViewQuery(con, metadata, view_name, table_name, col_name, col_name, /*persist=*/false) ||
		    AuxMetadataReferencesColumn(metadata, view_name, table_name, col_name)) {
			return view_name;
		}
	}
	return "";
}

void RewriteDependentViewMetadataForRename(Connection &con, const string &delta_name, const string &table_name,
                                           const string &old_name, const string &new_name) {
	RefreshMetadata metadata(con);
	for (auto &view_name : GetDependentViews(con, delta_name)) {
		ViewLockGuard view_guard(view_name);
		bool tx_open = false;
		try {
			con.BeginTransaction();
			tx_open = true;
			RewriteStoredViewQuery(con, metadata, view_name, table_name, old_name, new_name, /*persist=*/true);
			RewriteDistinctAuxMeta(con, metadata, view_name, table_name, old_name, new_name);
			RewriteFilteredGroupCountMeta(con, metadata, view_name, table_name, old_name, new_name);
			RewriteSemiAntiAuxMeta(con, metadata, view_name, table_name, old_name, new_name);
			RewriteLineageMeta(con, metadata, view_name, table_name, old_name, new_name);
			RewriteWindowGroupColumnSources(con, metadata, view_name, old_name, new_name);
			con.Commit();
			tx_open = false;
		} catch (std::exception &) {
			if (tx_open) {
				try {
					con.Rollback();
				} catch (std::exception &) {
				}
			}
			throw;
		}
	}
}

} // namespace duckdb
