#include "upsert/openivm_upsert.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_refresh_locks.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/main/client_data.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_compile_upsert.hpp"
#include "upsert/openivm_cost_model.hpp"
#include "lpts_pipeline.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include <cctype>
#include <chrono>

namespace duckdb {

/// Parsed FULL OUTER JOIN metadata: table and column names for both sides.
struct FojJoinInfo {
	string left_table, left_col, right_table, right_col;
	string dt_left_name, dt_right_name; // delta table names

	static FojJoinInfo Parse(IVMMetadata &metadata, const string &view_name, const vector<string> &delta_table_names) {
		FojJoinInfo info;
		string raw = metadata.GetFullOuterJoinCols(view_name);
		auto comma_pos = raw.find(',');
		if (comma_pos != string::npos) {
			string left_part = raw.substr(0, comma_pos);
			string right_part = raw.substr(comma_pos + 1);
			auto lc = left_part.find(':');
			if (lc != string::npos) {
				info.left_table = left_part.substr(0, lc);
				info.left_col = left_part.substr(lc + 1);
			}
			auto rc = right_part.find(':');
			if (rc != string::npos) {
				info.right_table = right_part.substr(0, rc);
				info.right_col = right_part.substr(rc + 1);
			}
		}
		// Case-insensitive compare: delta_table_names are stored lowercased (DuckDB's parser
		// lowercases unquoted identifiers), while full_outer_join_cols preserves the case of
		// the MV's FROM clause (e.g. "OORDER:O_W_ID").
		for (auto &dt_name : delta_table_names) {
			string base = StringUtil::StartsWith(dt_name, ivm::DELTA_PREFIX) ? dt_name.substr(strlen(ivm::DELTA_PREFIX))
			                                                                 : dt_name;
			if (StringUtil::CIEquals(base, info.left_table)) {
				info.dt_left_name = dt_name;
			}
			if (StringUtil::CIEquals(base, info.right_table)) {
				info.dt_right_name = dt_name;
			}
		}
		return info;
	}
};

static string JoinQuotedColumns(const vector<string> &columns) {
	string result;
	for (size_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(columns[i]);
	}
	return result;
}

static string BuildAllNullPredicate(const vector<string> &columns) {
	string result;
	for (size_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += KeywordHelper::WriteOptionallyQuoted(columns[i]) + " IS NULL";
	}
	return result;
}

static string BuildNullSafeKeyPredicate(const vector<string> &columns, const string &left_prefix,
                                        const string &right_prefix) {
	string result;
	for (size_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		auto col = KeywordHelper::WriteOptionallyQuoted(columns[i]);
		result += left_prefix + col + " IS NOT DISTINCT FROM " + right_prefix + col;
	}
	return result;
}

static string NormalizeColumnNameForMatch(const string &name) {
	string normalized;
	for (auto c : name) {
		if (std::isalnum(static_cast<unsigned char>(c))) {
			normalized += static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
		}
	}
	return normalized;
}

static bool GroupColumnMatchesJoinColumn(const string &group_col, const string &join_col) {
	auto group_norm = NormalizeColumnNameForMatch(group_col);
	if (group_norm == NormalizeColumnNameForMatch(join_col)) {
		return true;
	}
	auto underscore = join_col.find('_');
	if (underscore != string::npos && underscore + 1 < join_col.size()) {
		return group_norm == NormalizeColumnNameForMatch(join_col.substr(underscore + 1));
	}
	return false;
}

static string FindFullOuterJoinKeyGroupColumn(const vector<string> &group_cols, const FojJoinInfo &foj) {
	for (auto &group_col : group_cols) {
		if ((!foj.left_col.empty() && GroupColumnMatchesJoinColumn(group_col, foj.left_col)) ||
		    (!foj.right_col.empty() && GroupColumnMatchesJoinColumn(group_col, foj.right_col))) {
			return group_col;
		}
	}
	return "";
}

static string BuildFullOuterAffectedGroupsSubquery(IVMMetadata &metadata, const string &view_name,
                                                   const vector<string> &delta_table_names,
                                                   const vector<string> &group_cols, const string &view_query_sql,
                                                   const string &delta_ts_filter, const string &catalog_prefix) {
	auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	bool dt_left_is_versioned =
	    !foj.dt_left_name.empty() && metadata.IsExternallyVersionedTable(view_name, foj.dt_left_name);
	bool dt_right_is_versioned =
	    !foj.dt_right_name.empty() && metadata.IsExternallyVersionedTable(view_name, foj.dt_right_name);
	string delta_where_left = dt_left_is_versioned ? "" : delta_where;
	string delta_where_right = dt_right_is_versioned ? "" : delta_where;

	string keys_tuple = JoinQuotedColumns(group_cols);
	string affected;
	string qdv = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
	affected = "SELECT DISTINCT " + keys_tuple + " FROM " + qdv + delta_where;

	auto key_group_col = FindFullOuterJoinKeyGroupColumn(group_cols, foj);
	if (!key_group_col.empty()) {
		string key_col = KeywordHelper::WriteOptionallyQuoted(key_group_col);
		string changed_keys;
		auto append_changed_keys = [&](const string &delta_table, const string &join_col, const string &where_clause) {
			if (delta_table.empty() || join_col.empty()) {
				return;
			}
			if (!changed_keys.empty()) {
				changed_keys += "\n  UNION\n  ";
			}
			string q_delta = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(delta_table);
			changed_keys += "SELECT DISTINCT " + KeywordHelper::WriteOptionallyQuoted(join_col) +
			                " AS _ivm_foj_key FROM " + q_delta + where_clause;
		};
		append_changed_keys(foj.dt_left_name, foj.left_col, delta_where_left);
		append_changed_keys(foj.dt_right_name, foj.right_col, delta_where_right);
		if (!changed_keys.empty()) {
			affected += "\n  UNION\n  SELECT DISTINCT " + keys_tuple + " FROM (" + view_query_sql +
			            ") _ivm_foj_groups WHERE _ivm_foj_groups." + key_col + " IN (SELECT _ivm_foj_key FROM (\n  " +
			            changed_keys + "\n  ) _ivm_changed_foj_keys)";
		}
	}

	if (group_cols.size() == 1) {
		if (!foj.dt_left_name.empty()) {
			string q_dt_left = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_left_name);
			affected += "\n  UNION\n  SELECT DISTINCT " + keys_tuple + " FROM " + q_dt_left + delta_where_left;
		}
		if (!foj.dt_right_name.empty() && !foj.left_table.empty()) {
			string q_dt_right = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_right_name);
			string q_left_base = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.left_table);
			affected += "\n  UNION\n  SELECT DISTINCT " + keys_tuple + " FROM " + q_left_base + " WHERE " +
			            KeywordHelper::WriteOptionallyQuoted(foj.left_col) + " IN (SELECT DISTINCT " +
			            KeywordHelper::WriteOptionallyQuoted(foj.right_col) + " FROM " + q_dt_right +
			            delta_where_right + ")";
		}
	}
	return affected;
}

static string BuildFullOuterAffectedGroupRefresh(IVMMetadata &metadata, const string &view_name,
                                                 const vector<string> &delta_table_names,
                                                 const vector<string> &group_cols, const string &data_table,
                                                 const string &view_query_sql, const string &delta_ts_filter,
                                                 const string &catalog_prefix, const string &recompute_alias) {
	string affected = BuildFullOuterAffectedGroupsSubquery(metadata, view_name, delta_table_names, group_cols,
	                                                       view_query_sql, delta_ts_filter, catalog_prefix);
	string null_check = BuildAllNullPredicate(group_cols);
	string ncmp_del = BuildNullSafeKeyPredicate(group_cols, "_a.", data_table + ".");
	string ncmp_ins = BuildNullSafeKeyPredicate(group_cols, "_a.", recompute_alias + ".");
	string where_delete =
	    "EXISTS (SELECT 1 FROM (" + affected + "\n) _a WHERE " + ncmp_del + ") OR (" + null_check + ")";
	string where_insert =
	    "EXISTS (SELECT 1 FROM (" + affected + "\n) _a WHERE " + ncmp_ins + ") OR (" + null_check + ")";
	return "DELETE FROM " + data_table + " WHERE " + where_delete + ";\n" + "INSERT INTO " + data_table +
	       "\nSELECT * FROM (" + view_query_sql + ") " + recompute_alias + "\nWHERE " + where_insert + ";\n";
}

static string BuildDeltaTimestampFilter(Connection &con, const string &view_name, bool has_ts_col) {
	if (!has_ts_col) {
		return "";
	}
	auto last_refresh_result =
	    con.Query("SELECT COALESCE(last_refresh_ts, last_update) FROM " + string(ivm::DELTA_TABLES_TABLE) +
	              " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' LIMIT 1");
	if (last_refresh_result->HasError() || last_refresh_result->RowCount() == 0) {
		return "";
	}
	auto ts = last_refresh_result->GetValue(0, 0);
	if (ts.IsNull()) {
		return "";
	}
	return string(ivm::TIMESTAMP_COL) + " > '" + ts.ToString() + "'::TIMESTAMP";
}

static bool IsEmptyDeltaPlan(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		return true;
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return op->children.size() == 1 && IsEmptyDeltaPlan(op->children[0].get());
	case LogicalOperatorType::LOGICAL_UNION:
		if (op->children.empty()) {
			return false;
		}
		for (auto &child : op->children) {
			if (!IsEmptyDeltaPlan(child.get())) {
				return false;
			}
		}
		return true;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = op->Cast<LogicalAggregate>();
		return !agg.groups.empty() && op->children.size() == 1 && IsEmptyDeltaPlan(op->children[0].get());
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN: {
		if (op->children.size() != 2) {
			return false;
		}
		auto *join = dynamic_cast<LogicalJoin *>(op);
		if (!join || join->join_type == JoinType::INNER || op->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			return IsEmptyDeltaPlan(op->children[0].get()) || IsEmptyDeltaPlan(op->children[1].get());
		}
		if (join->join_type == JoinType::LEFT) {
			return IsEmptyDeltaPlan(op->children[0].get());
		}
		if (join->join_type == JoinType::RIGHT) {
			return IsEmptyDeltaPlan(op->children[1].get());
		}
		if (join->join_type == JoinType::OUTER) {
			return IsEmptyDeltaPlan(op->children[0].get()) && IsEmptyDeltaPlan(op->children[1].get());
		}
		return false;
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		return !op->children.empty() && IsEmptyDeltaPlan(op->children[0].get());
	}
	default:
		return false;
	}
}

static string BuildEmptyDeltaInsert(const string &view_name, const vector<string> &column_names,
                                    const vector<LogicalType> &column_types) {
	string sql = "INSERT INTO " + OpenIVMUtils::DeltaName(view_name) + " SELECT ";
	for (size_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		auto type = i < column_types.size() ? column_types[i] : LogicalType::VARCHAR;
		sql += "NULL::" + type.ToString();
	}
	sql += " WHERE false;\n";
	return sql;
}

static string BuildCompactDeltaViewSQL(const string &view_name, const string &delta_view_name,
                                       const vector<string> &column_names, const string &delta_ts_filter) {
	vector<string> data_columns;
	for (auto &col : column_names) {
		if (col != string(ivm::MULTIPLICITY_COL)) {
			data_columns.push_back(col);
		}
	}
	if (data_columns.empty()) {
		return "";
	}

	string col_list;
	string data_select;
	string group_by;
	for (size_t i = 0; i < data_columns.size(); i++) {
		if (i > 0) {
			data_select += ", ";
			group_by += ", ";
		}
		data_select += OpenIVMUtils::QuoteIdentifier(data_columns[i]);
		group_by += OpenIVMUtils::QuoteIdentifier(data_columns[i]);
	}
	for (size_t i = 0; i < column_names.size(); i++) {
		if (i > 0) {
			col_list += ", ";
		}
		col_list += OpenIVMUtils::QuoteIdentifier(column_names[i]);
	}

	string where_clause = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string delete_filter = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string temp_name = string(ivm::TEMP_TABLE_PREFIX) + "compact_" + view_name;
	string qtemp = KeywordHelper::WriteOptionallyQuoted(temp_name);
	string qmul = OpenIVMUtils::QuoteIdentifier(string(ivm::MULTIPLICITY_COL));

	string sql;
	sql += "CREATE TEMP TABLE " + qtemp + " AS SELECT " + data_select + ", SUM(" + qmul + ")::INTEGER AS " + qmul +
	       " FROM " + delta_view_name + where_clause + " GROUP BY " + group_by + " HAVING SUM(" + qmul + ") <> 0;\n";
	sql += "DELETE FROM " + delta_view_name + delete_filter + ";\n";
	sql += "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + col_list + " FROM " + qtemp + ";\n";
	sql += "DROP TABLE " + qtemp + ";\n";
	return sql;
}

static const char *IVMTypeName(IVMType type) {
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

static string ResolveDuckLakeCatalogName(Connection &con, const string &view_catalog_name,
                                         const string &attached_db_catalog_name) {
	if (!attached_db_catalog_name.empty()) {
		return attached_db_catalog_name;
	}
	auto probe = con.Query("SELECT database_name FROM duckdb_databases() WHERE type = 'ducklake' LIMIT 1");
	if (probe && !probe->HasError() && probe->RowCount() > 0 && !probe->GetValue(0, 0).IsNull()) {
		return probe->GetValue(0, 0).ToString();
	}
	if (!view_catalog_name.empty() && view_catalog_name != "memory") {
		return view_catalog_name;
	}
	return "dl";
}

static constexpr const char *DUCKLAKE_SNAPSHOT_PLACEHOLDER = "__OPENIVM_DUCKLAKE_SNAPSHOT_ID__";

static string BuildExternallyVersionedSnapshotUpdateSQL(IVMMetadata &metadata, Connection &con, const string &view_name,
                                                        const vector<string> &delta_table_names,
                                                        const string &view_catalog_name,
                                                        const string &attached_db_catalog_name, bool cross_system) {
	// Externally versioned sources have no OpenIVM delta table to clean up. Advancing
	// their source snapshot is what makes a full recompute a proper refresh boundary;
	// otherwise the next incremental refresh replays rows already included by the full scan.
	string dl_catalog_name = ResolveDuckLakeCatalogName(con, view_catalog_name, attached_db_catalog_name);
	string dl_snapshot_expr =
	    cross_system ? DUCKLAKE_SNAPSHOT_PLACEHOLDER : "(SELECT id FROM " + dl_catalog_name + ".current_snapshot())";
	string snapshot_update_query;
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			snapshot_update_query += "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
			                         " SET last_snapshot_id = " + dl_snapshot_expr + " WHERE view_name = '" +
			                         OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
			                         OpenIVMUtils::EscapeValue(dt) + "';\n";
		} else if (metadata.IsDeltaTable(view_name, dt)) {
			auto source_path = metadata.GetSourcePath(view_name, dt);
			if (source_path.empty()) {
				throw Exception(ExceptionType::CATALOG, "IVM: missing Delta Lake source path for table '" + dt +
				                                            "' in view '" + view_name + "'");
			}
			snapshot_update_query +=
			    "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
			    " SET last_snapshot_id = ("
			    "SELECT COALESCE(MAX(CAST(regexp_extract(file, '([0-9]+)\\.json$', 1) AS "
			    "BIGINT)), -1) FROM glob('" +
			    OpenIVMUtils::EscapeSingleQuotes(source_path) + "/_delta_log/*.json')) WHERE view_name = '" +
			    OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" + OpenIVMUtils::EscapeValue(dt) + "';\n";
		}
	}
	return snapshot_update_query;
}

static string BuildRecomputeQuery(IVMMetadata &metadata, Connection &con, const string &view_name,
                                  const string &view_query_sql, bool cross_system, const string &attached_catalog = "",
                                  const string &attached_schema = "", const string &catalog_prefix = "",
                                  string *out_post_meta = nullptr) {
	string qdt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(view_name));
	string query = "DELETE FROM " + qdt + ";\n";
	query += "INSERT INTO " + qdt + " " + view_query_sql + ";\n\n";

	// Timestamp update must run AFTER the DELETE+INSERT executes so that last_update
	// is only advanced when the recompute actually committed.
	// - Non-cross-system: include directly in the returned SQL (same transaction as the data ops).
	// - Cross-system (DuckLake): can't include in the data-op transaction (cross-catalog write
	//   restriction). Append to out_post_meta so RefreshViewLocked runs it on meta_con after
	//   exec_con.Query() succeeds.
	string update_ts_sql = "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
	                       " SET last_update = now(), last_refresh_ts = now() WHERE view_name = '" +
	                       OpenIVMUtils::EscapeValue(view_name) + "';\n";
	auto delta_tables = metadata.GetDeltaTables(view_name);
	update_ts_sql += BuildExternallyVersionedSnapshotUpdateSQL(metadata, con, view_name, delta_tables, attached_catalog,
	                                                           attached_catalog, cross_system);
	string update_ts;
	if (!cross_system) {
		update_ts = update_ts_sql;
	} else if (out_post_meta != nullptr) {
		*out_post_meta += update_ts_sql;
	}

	string delta_cleanup;
	for (auto &dt : delta_tables) {
		if (metadata.IsExternallyVersionedTable(view_name, dt)) {
			continue;
		}
		string resolved = dt;
		if (cross_system) {
			auto source_meta =
			    con.Query("SELECT source_catalog, source_schema FROM " + string(ivm::DELTA_TABLES_TABLE) +
			              " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
			              OpenIVMUtils::EscapeValue(dt) + "'");
			if (!source_meta->HasError() && source_meta->RowCount() > 0 && !source_meta->GetValue(0, 0).IsNull() &&
			    !source_meta->GetValue(1, 0).IsNull()) {
				resolved = OpenIVMUtils::QuoteIdentifier(source_meta->GetValue(0, 0).ToString()) + "." +
				           OpenIVMUtils::QuoteIdentifier(source_meta->GetValue(1, 0).ToString()) + "." +
				           OpenIVMUtils::QuoteIdentifier(dt);
			} else if (!attached_catalog.empty() && !attached_schema.empty()) {
				resolved = OpenIVMUtils::QuoteIdentifier(attached_catalog) + "." +
				           OpenIVMUtils::QuoteIdentifier(attached_schema) + "." + OpenIVMUtils::QuoteIdentifier(dt);
			}
		}
		delta_cleanup += IVMMetadata::BuildDeltaCleanupSQL(resolved, dt);
	}

	return query + update_ts + "\n" + delta_cleanup;
}

static string BuildFullOuterProjectionRefresh(IVMMetadata &metadata, const string &view_name,
                                              const vector<string> &delta_table_names, const string &data_table,
                                              const string &view_query_sql, const string &delta_ts_filter,
                                              const string &catalog_prefix) {
	// FULL OUTER JOIN bidirectional partial recompute (Zhang & Larson):
	// Metadata format: "left_table:left_col,right_table:right_col"
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string lk = KeywordHelper::WriteOptionallyQuoted(string(ivm::LEFT_KEY_COL));
	string rk = KeywordHelper::WriteOptionallyQuoted(string(ivm::RIGHT_KEY_COL));

	auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
	// Externally versioned base tables lack `_duckdb_ivm_timestamp` — drop the ts filter
	// when the delta source is versioned (delta_table_names stores the base name itself).
	bool dt_left_is_versioned =
	    !foj.dt_left_name.empty() && metadata.IsExternallyVersionedTable(view_name, foj.dt_left_name);
	bool dt_right_is_versioned =
	    !foj.dt_right_name.empty() && metadata.IsExternallyVersionedTable(view_name, foj.dt_right_name);
	string delta_where_left = dt_left_is_versioned ? "" : delta_where;
	string delta_where_right = dt_right_is_versioned ? "" : delta_where;

	string union_parts;
	if (!foj.dt_left_name.empty() && !foj.left_col.empty()) {
		string dt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_left_name);
		union_parts += "SELECT DISTINCT " + KeywordHelper::WriteOptionallyQuoted(foj.left_col) + " AS _k FROM " + dt +
		               delta_where_left;
	}
	if (!foj.dt_right_name.empty() && !foj.right_col.empty()) {
		if (!union_parts.empty()) {
			union_parts += "\n  UNION\n  ";
		}
		string dt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_right_name);
		union_parts += "SELECT DISTINCT " + KeywordHelper::WriteOptionallyQuoted(foj.right_col) + " AS _k FROM " + dt +
		               delta_where_right;
	}

	string where_clause;
	string affected_ctes;
	if (!union_parts.empty()) {
		affected_ctes = "WITH _ivm_affected AS (\n  " + union_parts + "\n)\n";
		where_clause = lk + " IN (SELECT _k FROM _ivm_affected) OR " + rk + " IN (SELECT _k FROM _ivm_affected)";
	} else {
		where_clause = "TRUE";
	}

	return affected_ctes + "DELETE FROM " + data_table + " WHERE " + where_clause + ";\n" + affected_ctes +
	       "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql + ") _ivm_foj\nWHERE " + where_clause +
	       ";\n";
}

static string BuildLeftJoinProjectionRefresh(const string &view_name, const string &data_table,
                                             const string &view_query_sql, const string &delta_ts_filter,
                                             const string &catalog_prefix) {
	string delta_where = delta_ts_filter.empty() ? "" : " AND " + delta_ts_filter;
	string qdv = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
	string lk = KeywordHelper::WriteOptionallyQuoted(string(ivm::LEFT_KEY_COL));
	string affected = "EXISTS (SELECT 1 FROM " + qdv + " _d WHERE _d." + lk + " IS NOT DISTINCT FROM ";
	return "DELETE FROM " + data_table + " WHERE " + affected + data_table + "." + lk + delta_where + ");\n" +
	       "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql + ") _ivm_lj\nWHERE " + affected +
	       "_ivm_lj." + lk + delta_where + ");\n";
}

static string CompileProjectionRefresh(IVMMetadata &metadata, const string &view_name,
                                       const vector<string> &column_names, const vector<string> &delta_table_names,
                                       const string &data_table, const string &view_query_sql,
                                       const string &delta_ts_filter, const string &catalog_prefix, bool has_full_outer,
                                       bool has_left_join, bool skip_proj_delete) {
	if (has_full_outer) {
		return BuildFullOuterProjectionRefresh(metadata, view_name, delta_table_names, data_table, view_query_sql,
		                                       delta_ts_filter, catalog_prefix);
	}
	if (has_left_join) {
		return BuildLeftJoinProjectionRefresh(view_name, data_table, view_query_sql, delta_ts_filter, catalog_prefix);
	}
	return CompileProjectionsFilters(view_name, column_names, delta_ts_filter, catalog_prefix, skip_proj_delete);
}

static void AppendSimpleAggregateEmptySourceNulling(IVMMetadata &metadata, string &upsert_query,
                                                    const string &view_name, const vector<string> &column_names,
                                                    const string &data_table, Connection &con,
                                                    const string &view_catalog_name, const string &view_schema_name,
                                                    const string &attached_db_catalog_name,
                                                    const string &attached_db_schema_name) {
	auto source_tables = metadata.GetDeltaTables(view_name);
	for (auto &dt : source_tables) {
		static const string prefix(ivm::DELTA_PREFIX);
		string base_name = dt.size() > prefix.size() && dt.rfind(prefix, 0) == 0 ? dt.substr(prefix.size()) : dt;
		string catalog_name = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
		string schema_name = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
		auto meta = con.Query("SELECT source_catalog, source_schema FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                      " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
		                      OpenIVMUtils::EscapeValue(dt) + "'");
		if (!meta->HasError() && meta->RowCount() > 0) {
			if (!meta->GetValue(0, 0).IsNull()) {
				catalog_name = meta->GetValue(0, 0).ToString();
			}
			if (!meta->GetValue(1, 0).IsNull()) {
				schema_name = meta->GetValue(1, 0).ToString();
			}
		}
		if (catalog_name.empty()) {
			catalog_name = "memory";
		}
		if (schema_name.empty()) {
			schema_name = "main";
		}
		string source = OpenIVMUtils::QuoteIdentifier(catalog_name) + "." + OpenIVMUtils::QuoteIdentifier(schema_name) +
		                "." + OpenIVMUtils::QuoteIdentifier(base_name);
		string null_cols;
		for (auto &col : column_names) {
			if (col == string(ivm::MULTIPLICITY_COL)) {
				continue;
			}
			if (!null_cols.empty()) {
				null_cols += ", ";
			}
			null_cols += KeywordHelper::WriteOptionallyQuoted(col) + " = NULL";
		}
		upsert_query += "UPDATE " + data_table + " SET " + null_cols + " WHERE NOT EXISTS (SELECT 1 FROM " + source +
		                " LIMIT 1);\n";
	}
}

struct ViewLocation {
	string catalog_name;
	string schema_name;
	bool cross_system;
};

static string CurrentDatabase(Connection &con) {
	auto res = con.Query("SELECT current_database()");
	if (!res->HasError() && res->RowCount() > 0 && !res->GetValue(0, 0).IsNull()) {
		return res->GetValue(0, 0).ToString();
	}
	return "";
}

static ViewLocation ResolveViewLocation(Connection &con, const string &view_name, const string &fallback_catalog,
                                        const string &fallback_schema) {
	string catalog_name = fallback_catalog;
	string schema_name = fallback_schema;
	string query = "SELECT table_catalog, table_schema FROM information_schema.tables WHERE table_type = 'VIEW' "
	               "AND table_name = '" +
	               OpenIVMUtils::EscapeValue(view_name) + "' ORDER BY CASE WHEN table_catalog = '" +
	               OpenIVMUtils::EscapeValue(fallback_catalog) + "' AND table_schema = '" +
	               OpenIVMUtils::EscapeValue(fallback_schema) +
	               "' THEN 0 ELSE 1 END, table_catalog, table_schema LIMIT 1";
	auto found = con.Query(query);
	if (!found->HasError() && found->RowCount() > 0) {
		catalog_name = found->GetValue(0, 0).ToString();
		schema_name = found->GetValue(1, 0).ToString();
	}
	auto current_database = CurrentDatabase(con);
	bool cross_system = !catalog_name.empty() && !current_database.empty() && catalog_name != current_database;
	return {catalog_name, schema_name, cross_system};
}

struct DuckLakeSourceLocation {
	string catalog_name;
	string schema_name;
	string table_name;
};

static DuckLakeSourceLocation ResolveDuckLakeSourceLocation(Connection &con, const string &view_name,
                                                            const string &table_name, const string &fallback_catalog,
                                                            const string &fallback_schema,
                                                            const string &attached_catalog,
                                                            const string &attached_schema) {
	DuckLakeSourceLocation loc;
	loc.catalog_name = attached_catalog.empty() ? fallback_catalog : attached_catalog;
	loc.schema_name = attached_schema.empty() ? fallback_schema : attached_schema;
	loc.table_name = table_name;

	auto meta = con.Query("SELECT source_catalog, source_schema FROM " + string(ivm::DELTA_TABLES_TABLE) +
	                      " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
	                      OpenIVMUtils::EscapeValue(table_name) + "'");
	if (!meta->HasError() && meta->RowCount() > 0) {
		if (!meta->GetValue(0, 0).IsNull()) {
			loc.catalog_name = meta->GetValue(0, 0).ToString();
		}
		if (!meta->GetValue(1, 0).IsNull()) {
			loc.schema_name = meta->GetValue(1, 0).ToString();
		}
	}

	if (StringUtil::StartsWith(table_name, ivm::DATA_TABLE_PREFIX)) {
		string source_view = table_name.substr(strlen(ivm::DATA_TABLE_PREFIX));
		auto source_view_location = ResolveViewLocation(con, source_view, loc.catalog_name, loc.schema_name);
		loc.catalog_name = source_view_location.catalog_name;
		loc.schema_name = source_view_location.schema_name;
	}

	if (loc.catalog_name.empty()) {
		loc.catalog_name = fallback_catalog.empty() ? "memory" : fallback_catalog;
	}
	if (loc.schema_name.empty()) {
		loc.schema_name = fallback_schema.empty() ? "main" : fallback_schema;
	}
	return loc;
}

static vector<GroupRecomputeDeltaSpec> BuildGroupRecomputeDeltaSpecs(IVMMetadata &metadata, const string &view_name,
                                                                     Connection &con,
                                                                     const vector<string> &delta_table_names,
                                                                     const string &ducklake_catalog,
                                                                     const string &ducklake_schema) {
	vector<GroupRecomputeDeltaSpec> delta_specs;
	for (auto &dt : delta_table_names) {
		GroupRecomputeDeltaSpec spec;
		spec.base_table = dt;
		static const string prefix(ivm::DELTA_PREFIX);
		if (spec.base_table.size() > prefix.size() && spec.base_table.rfind(prefix, 0) == 0) {
			spec.base_table = spec.base_table.substr(prefix.size());
		}
		spec.last_update = metadata.GetLastUpdate(view_name, dt);
		spec.is_ducklake = metadata.IsDuckLakeTable(view_name, dt);
		spec.is_delta = metadata.IsDeltaTable(view_name, dt);
		if (spec.is_ducklake) {
			auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, ducklake_catalog, ducklake_schema, "", "");
			spec.ducklake_catalog = loc.catalog_name;
			spec.ducklake_schema = loc.schema_name;
			spec.last_snapshot_id = metadata.GetLastSnapshotId(view_name, dt);
			auto snap_result =
			    con.Query("SELECT id FROM " + OpenIVMUtils::QuoteIdentifier(loc.catalog_name) + ".current_snapshot()");
			if (!snap_result->HasError() && snap_result->RowCount() > 0 && !snap_result->GetValue(0, 0).IsNull()) {
				spec.current_snapshot_id = snap_result->GetValue(0, 0).GetValue<int64_t>();
			}
		} else if (spec.is_delta) {
			auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, ducklake_catalog, ducklake_schema, "", "");
			spec.delta_catalog = loc.catalog_name;
			spec.delta_schema = loc.schema_name;
			spec.last_snapshot_id = metadata.GetLastSnapshotId(view_name, dt);
			auto source_path = metadata.GetSourcePath(view_name, dt);
			auto version_result = con.Query(
			    "SELECT COALESCE(MAX(CAST(regexp_extract(file, '([0-9]+)\\.json$', 1) AS BIGINT)), -1) FROM glob('" +
			    OpenIVMUtils::EscapeSingleQuotes(source_path) + "/_delta_log/*.json')");
			if (!version_result->HasError() && version_result->RowCount() > 0 &&
			    !version_result->GetValue(0, 0).IsNull()) {
				spec.current_snapshot_id = version_result->GetValue(0, 0).GetValue<int64_t>();
			}
		}
		delta_specs.push_back(std::move(spec));
	}
	return delta_specs;
}

static string BuildLptsTablePrefix(const string &view_catalog_name, const string &view_schema_name) {
	string lpts_cat = view_catalog_name.empty() ? "memory" : view_catalog_name;
	string lpts_sch = view_schema_name.empty() ? "main" : view_schema_name;
	return OpenIVMUtils::QuoteIdentifier(lpts_cat) + "." + OpenIVMUtils::QuoteIdentifier(lpts_sch) + ".";
}

static string QualifiedName(const string &catalog_name, const string &schema_name, const string &table_name) {
	return OpenIVMUtils::QuoteIdentifier(catalog_name) + "." + OpenIVMUtils::QuoteIdentifier(schema_name) + "." +
	       OpenIVMUtils::QuoteIdentifier(table_name);
}

static string ResolveStandardDeltaQualifiedName(Connection &con, const string &view_name,
                                                const string &delta_table_name, const string &fallback_catalog,
                                                const string &fallback_schema) {
	string catalog_name = fallback_catalog;
	string schema_name = fallback_schema;
	auto meta = con.Query("SELECT source_catalog, source_schema FROM " + string(ivm::DELTA_TABLES_TABLE) +
	                      " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
	                      OpenIVMUtils::EscapeValue(delta_table_name) + "'");
	if (!meta->HasError() && meta->RowCount() > 0) {
		if (!meta->GetValue(0, 0).IsNull()) {
			catalog_name = meta->GetValue(0, 0).ToString();
		}
		if (!meta->GetValue(1, 0).IsNull()) {
			schema_name = meta->GetValue(1, 0).ToString();
		}
	}
	if (catalog_name.empty()) {
		return OpenIVMUtils::QuoteIdentifier(delta_table_name);
	}
	if (schema_name.empty()) {
		schema_name = "main";
	}
	return QualifiedName(catalog_name, schema_name, delta_table_name);
}

static string BaseTableNameFromDeltaKey(const string &delta_key) {
	static const string prefix(ivm::DELTA_PREFIX);
	if (delta_key.size() > prefix.size() && delta_key.rfind(prefix, 0) == 0) {
		return delta_key.substr(prefix.size());
	}
	return delta_key;
}

static bool IsIdentifierChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.';
}

static bool IdentifierMatchesTable(const string &identifier, const string &table_name) {
	if (StringUtil::CIEquals(identifier, table_name)) {
		return true;
	}
	if (identifier.size() <= table_name.size()) {
		return false;
	}
	auto suffix = identifier.substr(identifier.size() - table_name.size());
	return identifier[identifier.size() - table_name.size() - 1] == '.' && StringUtil::CIEquals(suffix, table_name);
}

static bool ReadIdentifierSegment(const string &sql, idx_t &pos, string &segment) {
	if (pos >= sql.size()) {
		return false;
	}
	idx_t start = pos;
	if (sql[pos] == '"') {
		pos++;
		string unquoted;
		while (pos < sql.size()) {
			char c = sql[pos++];
			if (c == '"') {
				if (pos < sql.size() && sql[pos] == '"') {
					unquoted += '"';
					pos++;
					continue;
				}
				segment = unquoted;
				return true;
			}
			unquoted += c;
		}
		pos = start;
		return false;
	}
	if (!(std::isalpha(static_cast<unsigned char>(sql[pos])) || sql[pos] == '_')) {
		return false;
	}
	pos++;
	while (pos < sql.size() && IsIdentifierChar(sql[pos]) && sql[pos] != '.') {
		pos++;
	}
	segment = sql.substr(start, pos - start);
	return true;
}

static bool ReadQualifiedIdentifier(const string &sql, idx_t start, idx_t &end, string &identifier) {
	idx_t pos = start;
	string segment;
	if (!ReadIdentifierSegment(sql, pos, segment)) {
		return false;
	}
	identifier = segment;
	while (pos < sql.size() && sql[pos] == '.') {
		idx_t dot_pos = pos++;
		if (!ReadIdentifierSegment(sql, pos, segment)) {
			pos = dot_pos;
			break;
		}
		identifier += "." + segment;
	}
	end = pos;
	return true;
}

static string ReplaceTableReferences(const string &sql, const string &table_name, const string &replacement) {
	if (table_name.empty()) {
		return sql;
	}
	string result;
	result.reserve(sql.size());
	bool in_single_quote = false;
	bool expect_table = false;
	for (idx_t i = 0; i < sql.size();) {
		char c = sql[i];
		if (c == '\'') {
			result += c;
			i++;
			if (in_single_quote && i < sql.size() && sql[i] == '\'') {
				result += sql[i++];
				continue;
			}
			in_single_quote = !in_single_quote;
			continue;
		}
		if (!in_single_quote && (std::isalpha(static_cast<unsigned char>(c)) || c == '_' || c == '"')) {
			idx_t start = i;
			if (expect_table) {
				idx_t end = i;
				string qualified_identifier;
				if (ReadQualifiedIdentifier(sql, start, end, qualified_identifier) &&
				    IdentifierMatchesTable(qualified_identifier, table_name)) {
					idx_t at_pos = end;
					while (at_pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[at_pos]))) {
						at_pos++;
					}
					if (StringUtil::CIStartsWith(sql.substr(at_pos), "AT (")) {
						idx_t depth = 0;
						while (at_pos < sql.size()) {
							if (sql[at_pos] == '(') {
								depth++;
							} else if (sql[at_pos] == ')') {
								depth--;
								if (depth == 0) {
									at_pos++;
									break;
								}
							}
							at_pos++;
						}
						end = at_pos;
					}
					result += replacement;
					i = end;
					expect_table = false;
					continue;
				}
			}
			bool in_identifier_quote = c == '"';
			i++;
			while (i < sql.size()) {
				char nc = sql[i];
				if (in_identifier_quote) {
					i++;
					if (nc == '"') {
						break;
					}
					continue;
				}
				if (!IsIdentifierChar(nc)) {
					break;
				}
				i++;
			}
			string token = sql.substr(start, i - start);
			string unquoted = token;
			if (unquoted.size() >= 2 && unquoted.front() == '"' && unquoted.back() == '"') {
				unquoted = unquoted.substr(1, unquoted.size() - 2);
			}
			if (expect_table && IdentifierMatchesTable(unquoted, table_name)) {
				result += replacement;
				expect_table = false;
				continue;
			}
			result += token;
			string lower = StringUtil::Lower(unquoted);
			expect_table = lower == "from" || lower == "join" || lower == "update" || lower == "into";
			continue;
		}
		result += c;
		if (!std::isspace(static_cast<unsigned char>(c))) {
			expect_table = false;
		}
		i++;
	}
	return result;
}

static string BuildDuckLakeSnapshotQuery(IVMMetadata &metadata, Connection &con, const string &view_name,
                                         const string &view_query_sql, const vector<string> &delta_table_names,
                                         const string &view_catalog_name, const string &view_schema_name,
                                         const string &attached_db_catalog_name,
                                         const string &attached_db_schema_name) {
	string snapshot_query = view_query_sql;
	for (auto &dt : delta_table_names) {
		if (!metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		int64_t old_snap = metadata.GetLastSnapshotId(view_name, dt);
		auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name);
		string source_name = loc.table_name;
		if (StringUtil::StartsWith(source_name, ivm::DATA_TABLE_PREFIX)) {
			source_name = source_name.substr(strlen(ivm::DATA_TABLE_PREFIX));
		}
		string visible_cols;
		auto cols = con.Query("SELECT column_name FROM information_schema.columns WHERE table_catalog = '" +
		                      OpenIVMUtils::EscapeValue(loc.catalog_name) + "' AND table_schema = '" +
		                      OpenIVMUtils::EscapeValue(loc.schema_name) + "' AND table_name = '" +
		                      OpenIVMUtils::EscapeValue(source_name) + "' ORDER BY ordinal_position");
		if (!cols->HasError()) {
			for (idx_t i = 0; i < cols->RowCount(); i++) {
				if (!visible_cols.empty()) {
					visible_cols += ", ";
				}
				visible_cols += OpenIVMUtils::QuoteIdentifier(cols->GetValue(0, i).ToString());
			}
		}
		if (visible_cols.empty()) {
			visible_cols = "*";
		}
		string replacement = "(SELECT " + visible_cols + " FROM " +
		                     QualifiedName(loc.catalog_name, loc.schema_name, loc.table_name) + " AT (VERSION => " +
		                     to_string(old_snap) + "))";
		snapshot_query = ReplaceTableReferences(snapshot_query, loc.table_name, replacement);
		if (!StringUtil::CIEquals(source_name, loc.table_name)) {
			snapshot_query = ReplaceTableReferences(snapshot_query, source_name, replacement);
		}
	}
	return snapshot_query;
}

static string QualifyViewQuerySources(IVMMetadata &metadata, Connection &con, const string &view_name,
                                      const string &view_query_sql, const vector<string> &delta_table_names,
                                      const string &view_catalog_name, const string &view_schema_name,
                                      const string &attached_db_catalog_name, const string &attached_db_schema_name) {
	string qualified_query = view_query_sql;
	for (auto &dt : delta_table_names) {
		string base_name = BaseTableNameFromDeltaKey(dt);
		auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name);
		loc.table_name = base_name;
		if (loc.catalog_name.empty() || loc.schema_name.empty()) {
			continue;
		}
		// LPTS may serialize source scans with bare table names even when the MV
		// reads another catalog/schema. Refresh runs on a fresh connection, so make
		// those sources explicit before compiling recompute and delta SQL.
		qualified_query = ReplaceTableReferences(qualified_query, base_name,
		                                         QualifiedName(loc.catalog_name, loc.schema_name, base_name));
	}
	return qualified_query;
}

struct RefreshProfileStep {
	int32_t step_order;
	string step_name;
	int64_t duration_ms;
	string detail;
};

class RefreshProfiler {
public:
	RefreshProfiler(ClientContext &context, string view_name_p)
	    : enabled(false), retention_days(31), view_name(std::move(view_name_p)), next_step(0),
	      total_start(std::chrono::steady_clock::now()) {
		Value profile_val;
		enabled = context.TryGetCurrentSetting("ivm_profile_refresh", profile_val) && !profile_val.IsNull() &&
		          profile_val.GetValue<bool>();
		if (enabled) {
			Value retention_val;
			if (context.TryGetCurrentSetting("ivm_profile_retention_days", retention_val) && !retention_val.IsNull()) {
				retention_days = std::max<int64_t>(0, retention_val.GetValue<int64_t>());
			}
			auto now = std::chrono::steady_clock::now().time_since_epoch();
			refresh_id = view_name + "_" + to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
		}
	}

	bool Enabled() const {
		return enabled;
	}

	void AddStep(const string &step_name, std::chrono::steady_clock::time_point start,
	             const string &detail = string()) {
		if (!enabled) {
			return;
		}
		auto end = std::chrono::steady_clock::now();
		auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		steps.push_back({next_step++, step_name, duration_ms, detail});
	}

	void AddTotal() {
		AddStep("total_refresh", total_start);
	}

	void Flush(DatabaseInstance &db) {
		if (!enabled || steps.empty()) {
			return;
		}
		Connection profile_con(db);
		profile_con.Query("DELETE FROM " + string(ivm::PROFILE_TABLE) +
		                  " WHERE profile_timestamp < current_timestamp::TIMESTAMP - INTERVAL '" +
		                  to_string(retention_days) + " days'");
		for (auto &step : steps) {
			auto result = profile_con.Query(
			    "INSERT OR REPLACE INTO " + string(ivm::PROFILE_TABLE) +
			    " (refresh_id, view_name, step_order, step_name, duration_ms, detail) VALUES ('" +
			    OpenIVMUtils::EscapeValue(refresh_id) + "', '" + OpenIVMUtils::EscapeValue(view_name) + "', " +
			    to_string(step.step_order) + ", '" + OpenIVMUtils::EscapeValue(step.step_name) + "', " +
			    to_string(step.duration_ms) + ", '" + OpenIVMUtils::EscapeValue(step.detail) + "')");
			if (result->HasError()) {
				OPENIVM_DEBUG_PRINT("[PROFILE] Failed to record refresh step '%s': %s\n", step.step_name.c_str(),
				                    result->GetError().c_str());
				return;
			}
		}
	}

private:
	bool enabled;
	int64_t retention_days;
	string view_name;
	string refresh_id;
	int32_t next_step;
	std::chrono::steady_clock::time_point total_start;
	vector<RefreshProfileStep> steps;
};

// Generate refresh SQL for a single view (no cascade logic).
static string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name,
                                 const string &view_schema_name, const string &view_name, bool cross_system,
                                 const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                 string *out_pre_meta = nullptr, string *out_post_meta = nullptr);

// Generate and execute refresh SQL for a single view under its per-view lock.
// When ivm_adaptive_refresh is on, also computes a cost estimate before execution
// and records execution history for the learned cost model.
static void RefreshViewLocked(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                              const string &vn, bool cross_system, const string &attached_db_catalog_name,
                              const string &attached_db_schema_name) {
	RefreshProfiler profiler(context, vn);
	auto lock_start = std::chrono::steady_clock::now();
	ViewLockGuard view_guard(vn);
	// Acquire delta-table locks in sorted order to serialize parallel refreshes that
	// share base tables (e.g. mv_A and mv_B both reading STOCK → both write to
	// `delta_STOCK` inside their transactions → "Conflict on tuple deletion!" when
	// the second tx tries to delete rows the first already processed). Sorting
	// guarantees the same acquisition order across all views, so no deadlock is
	// possible between concurrent refreshes.
	vector<unique_ptr<DeltaLockGuard>> delta_guards;
	Connection probe_con(*context.db.get());
	IVMMetadata probe_meta(probe_con);
	auto delta_table_names = probe_meta.GetDeltaTables(vn);
	std::sort(delta_table_names.begin(), delta_table_names.end());
	delta_table_names.erase(std::unique(delta_table_names.begin(), delta_table_names.end()), delta_table_names.end());
	for (auto &dt : delta_table_names) {
		delta_guards.push_back(make_uniq<DeltaLockGuard>(dt));
	}
	profiler.AddStep("acquire_locks", lock_start, to_string(delta_table_names.size()) + " delta locks");
	// Track whether we're inside an open exec_con transaction so any exception path can
	// rollback cleanly. Without explicit rollback, a throw mid-transaction relies on the
	// Connection destructor, which can leak uncommitted writes into the WAL under some
	// failure modes (e.g. rebinding errors thrown by Query itself, not reported as
	// HasError()). Rollback-then-throw keeps the WAL clean and leaves the DB valid.
	Connection exec_con(*context.db.get());
	bool tx_open = false;
	try {
		// Check if adaptive cost model is active — if so, compute estimate for history recording.
		bool record_history = false;
		IVMCostEstimate cost_estimate = {};
		Value adaptive_val;
		if (context.TryGetCurrentSetting("ivm_adaptive_refresh", adaptive_val) && !adaptive_val.IsNull() &&
		    adaptive_val.GetValue<bool>()) {
			auto adaptive_start = std::chrono::steady_clock::now();
			// Compute cost estimate before refresh (for history recording).
			// GenerateRefreshSQL also computes this when adaptive is on, but we need
			// the estimate here to record alongside the actual execution time.
			Connection cost_con(*context.db.get());
			cost_con.BeginTransaction();
			IVMMetadata cost_meta(cost_con);
			auto vq = cost_meta.GetViewQuery(vn);
			if (!vq.empty()) {
				Parser cp;
				cp.ParseQuery(vq);
				Planner pl(*cost_con.context);
				pl.CreatePlan(cp.statements[0]->Copy());
				Optimizer opt(*pl.binder, *cost_con.context);
				auto plan = opt.Optimize(std::move(pl.plan));
				cost_estimate = EstimateIVMCost(*cost_con.context, *plan, vn);
				record_history = true;
			}
			cost_con.Rollback();
			profiler.AddStep("adaptive_cost_estimate", adaptive_start,
			                 record_history ? "record_history=true" : "record_history=false");
		}

		// For cross_system (DuckLake) MVs, split the refresh SQL into data ops (dl catalog)
		// and metadata ops (physical-default catalog) to avoid the cross-catalog write error.
		string meta_pre_sql, meta_post_sql;
		auto generate_start = std::chrono::steady_clock::now();
		string sql = GenerateRefreshSQL(
		    context, view_catalog_name, view_schema_name, vn, cross_system, attached_db_catalog_name,
		    attached_db_schema_name, cross_system ? &meta_pre_sql : nullptr, cross_system ? &meta_post_sql : nullptr);
		profiler.AddStep("generate_refresh_sql", generate_start,
		                 "sql_bytes=" + to_string(sql.size()) + ", meta_pre_bytes=" + to_string(meta_pre_sql.size()) +
		                     ", meta_post_bytes=" + to_string(meta_post_sql.size()));
		// IVM-generated SQL can nest deeply for multi-table joins + CTEs (N-term telescoping
		// over 7+ tables produces hundreds of chained projections). Lift the default 1000
		// expression-depth limit so the binder doesn't reject legitimate plans.
		exec_con.Query("SET max_expression_depth = 10000");
		// Refreshes update relational MV state; physical insertion order is not part of
		// the contract. Let DuckDB avoid large order-preservation buffers for big
		// INSERT/CREATE TABLE style refresh plans.
		exec_con.Query("SET preserve_insertion_order=false");
		// Keep refresh execution in the physical default catalog. DuckLake-targeted MVs
		// now write only physical _ivm_data_*/delta_* state; DuckLake source references
		// emitted by LPTS are catalog-qualified, so switching USE to DuckLake would make
		// any remaining unqualified internal references bind to the wrong catalog.
		OPENIVM_DEBUG_PRINT("[UPSERT] Executing refresh SQL:\n%s\n", sql.c_str());

		// Wrap the entire refresh in a transaction so that a failed refresh leaves the MV
		// and delta tables in a clean state (atomically rolled back). The refresh_in_progress
		// flag is inside the same transaction, so it also rolls back on failure — WAL recovery
		// handles the crash case automatically.
		// DuckLake (cross_system) skips this: DuckDB forbids writing to two attached databases
		// (metadata catalog + dl) within a single transaction. Instead, we run metadata ops on
		// a separate meta_con (physical-default catalog) and data ops on exec_con (dl catalog).
		if (!cross_system) {
			exec_con.BeginTransaction();
			tx_open = true;
		} else if (!meta_pre_sql.empty()) {
			auto meta_pre_start = std::chrono::steady_clock::now();
			Connection meta_con(*context.db.get());
			auto meta_result = meta_con.Query(meta_pre_sql);
			if (meta_result->HasError()) {
				throw Exception(ExceptionType::EXECUTOR,
				                "IVM refresh of '" + vn + "' failed before data refresh: " + meta_result->GetError());
			}
			profiler.AddStep("metadata_pre_sql", meta_pre_start, "bytes=" + to_string(meta_pre_sql.size()));
		}
		auto start = std::chrono::steady_clock::now();
		auto result = exec_con.Query(sql);
		auto end = std::chrono::steady_clock::now();
		profiler.AddStep("execute_refresh_sql", start, "bytes=" + to_string(sql.size()));

		if (result->HasError()) {
			if (tx_open) {
				exec_con.Rollback();
				tx_open = false;
			}
			// Use a regular Exception (not InternalException) — a failed refresh is
			// recoverable (e.g. transaction-conflict on a shared delta table from two
			// parallel refreshes). InternalException causes DuckDB to flag the whole
			// database as invalidated, forcing a restart. We've already rolled back, so
			// the DB is in a clean state; the next refresh attempt should succeed.
			throw Exception(ExceptionType::EXECUTOR, "IVM refresh of '" + vn + "' failed: " + result->GetError());
		}
		if (tx_open) {
			exec_con.Commit();
			tx_open = false;
		} else if (cross_system && !meta_post_sql.empty()) {
			auto meta_post_start = std::chrono::steady_clock::now();
			if (meta_post_sql.find(DUCKLAKE_SNAPSHOT_PLACEHOLDER) != string::npos) {
				string dl_catalog = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
				// DuckLake can keep read snapshot state on the connection that compiled
				// the data refresh. Read the post-refresh watermark through a fresh
				// connection so we do not persist an old snapshot and replay deltas.
				Connection snap_con(*context.db.get());
				auto snap_result = snap_con.Query("SELECT id FROM " + OpenIVMUtils::QuoteIdentifier(dl_catalog) +
				                                  ".current_snapshot()");
				if (snap_result->HasError() || snap_result->RowCount() == 0 || snap_result->GetValue(0, 0).IsNull()) {
					throw Exception(ExceptionType::EXECUTOR,
					                "IVM refresh of '" + vn +
					                    "' failed: could not read DuckLake snapshot after data refresh");
				}
				meta_post_sql = StringUtil::Replace(meta_post_sql, DUCKLAKE_SNAPSHOT_PLACEHOLDER,
				                                    snap_result->GetValue(0, 0).ToString());
			}
			Connection meta_con(*context.db.get());
			auto meta_result = meta_con.Query(meta_post_sql);
			if (meta_result->HasError()) {
				throw Exception(ExceptionType::EXECUTOR,
				                "IVM refresh of '" + vn + "' failed after data refresh: " + meta_result->GetError());
			}
			profiler.AddStep("metadata_post_sql", meta_post_start, "bytes=" + to_string(meta_post_sql.size()));
		}

		// Record execution history for the learned cost model.
		if (record_history) {
			auto history_start = std::chrono::steady_clock::now();
			auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
			// Determine which method was used. Priority:
			//   1) `ivm_refresh_mode = 'full'` overrides everything → "full".
			//   2) `cost_estimate.strategy_label` if set (group_recompute, window_partition).
			//      For these fixed-strategy views the IVM-vs-full decision never fires.
			//   3) For "incremental" views, the adaptive cost model may have picked full recompute.
			string method = cost_estimate.strategy_label.empty() ? "incremental" : cost_estimate.strategy_label;
			if (method == "incremental" && cost_estimate.ShouldRecompute()) {
				method = "full";
			}
			Value mode_val;
			if (context.TryGetCurrentSetting("ivm_refresh_mode", mode_val) && !mode_val.IsNull()) {
				auto mode = StringUtil::Lower(mode_val.ToString());
				if (mode == "full") {
					method = "full";
				}
			}

			IVMMetadata(exec_con).RecordRefreshHistory(vn, method, cost_estimate.ivm_compute, cost_estimate.ivm_upsert,
			                                           cost_estimate.recompute_compute, cost_estimate.recompute_replace,
			                                           duration_ms);
			OPENIVM_DEBUG_PRINT("[HISTORY] Recorded: view=%s, method=%s, duration=%ldms\n", vn.c_str(), method.c_str(),
			                    (long)duration_ms);
			profiler.AddStep("record_refresh_history", history_start, method);
		}
		profiler.AddTotal();
		profiler.Flush(*context.db.get());
	} catch (...) {
		// Ensure the transaction is rolled back before we propagate the exception.
		// This covers the case where Query() itself threw (vs returning HasError) —
		// without this, the tx would stay open until exec_con's destructor runs,
		// possibly leaving dirty WAL entries.
		if (tx_open) {
			try {
				exec_con.Rollback();
			} catch (...) {
				// Rollback-inside-rollback failure is benign: the Connection destructor
				// will still clean up. Swallow so we don't mask the original error.
			}
			tx_open = false;
		}
		profiler.AddTotal();
		profiler.Flush(*context.db.get());
		throw;
	}
}

void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters) {
	OPENIVM_DEBUG_PRINT("[UPSERT] UpsertDeltaQueriesLocked START\n");
	// PRAGMA ivm refreshes relational state, not ordered output. Force the
	// OpenIVM entry point onto DuckDB's unordered execution mode even if this
	// connection previously had a local preserve_insertion_order=true setting.
	ClientConfig::GetConfig(context).user_settings.SetUserSetting(PreserveInsertionOrderSetting::SettingIndex,
	                                                             Value::BOOLEAN(false));
	string view_catalog_name;
	string view_schema_name;
	string attached_db_catalog_name;
	string attached_db_schema_name;
	string view_name;
	bool cross_system = false;

	Connection con(*context.db.get());

	if (parameters.values.size() == 3) {
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		view_name = StringValue::Get(parameters.values[2]);
	} else if (parameters.values.size() == 5) {
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		attached_db_catalog_name = StringValue::Get(parameters.values[2]);
		attached_db_schema_name = StringValue::Get(parameters.values[3]);
		view_name = StringValue::Get(parameters.values[4]);
		cross_system = true;
	} else {
		auto &search_path = ClientData::Get(context).catalog_search_path;
		auto default_entry = search_path->GetDefault();
		view_catalog_name = default_entry.catalog;
		view_schema_name = default_entry.schema;
		view_name = StringValue::Get(parameters.values[0]);

		OPENIVM_DEBUG_PRINT("[UPSERT] Default catalog=%s, schema=%s, view=%s\n", view_catalog_name.c_str(),
		                    view_schema_name.c_str(), view_name.c_str());
		// If the view doesn't exist in the default catalog, search attached catalogs.
		// This handles DuckLake MVs created as "dl.mv_name" where the view lives in "dl".
		{
			QueryErrorContext err_ctx;
			auto entry = Catalog::GetEntry(context, view_catalog_name, view_schema_name,
			                               EntryLookupInfo(CatalogType::VIEW_ENTRY, view_name, err_ctx),
			                               OnEntryNotFound::RETURN_NULL);
			OPENIVM_DEBUG_PRINT("[UPSERT] Default catalog entry: %s\n", entry ? "found" : "not found");
			if (!entry) {
				auto found_view =
				    con.Query("SELECT table_catalog, table_schema FROM information_schema.tables WHERE table_type = "
				              "'VIEW' AND table_name = '" +
				              OpenIVMUtils::EscapeValue(view_name) + "' ORDER BY table_catalog, table_schema LIMIT 1");
				if (!found_view->HasError() && found_view->RowCount() > 0) {
					view_catalog_name = found_view->GetValue(0, 0).ToString();
					view_schema_name = found_view->GetValue(1, 0).ToString();
					if (view_catalog_name != "memory") {
						cross_system = true;
					}
					OPENIVM_DEBUG_PRINT("[UPSERT] Found view via information_schema in '%s.%s'\n",
					                    view_catalog_name.c_str(), view_schema_name.c_str());
				}
			}
			OPENIVM_DEBUG_PRINT("[UPSERT] Resolved catalog='%s', schema='%s'\n", view_catalog_name.c_str(),
			                    view_schema_name.c_str());
		}
	}

	// cross_system detection: the view's catalog differs from the fresh connection's physical
	// default. Metadata tables (_duckdb_ivm_views etc.) live in the physical default; data/view
	// tables live in view_catalog_name. DuckDB forbids cross-catalog writes in one transaction,
	// so RefreshViewLocked must split the refresh SQL into data ops and metadata ops.
	if (!view_catalog_name.empty()) {
		Connection probe(*context.db.get());
		string probe_default;
		auto res = probe.Query("SELECT current_database()");
		if (!res->HasError() && res->RowCount() > 0) {
			probe_default = res->GetValue(0, 0).ToString();
		}
		if (!probe_default.empty() && view_catalog_name != probe_default) {
			cross_system = true;
		}
	}

	// Check cascade mode
	string cascade_mode = "downstream";
	Value cascade_val;
	if (context.TryGetCurrentSetting("ivm_cascade_refresh", cascade_val) && !cascade_val.IsNull()) {
		cascade_mode = StringUtil::Lower(cascade_val.ToString());
	}

	IVMMetadata metadata(con);

	// Each view is generated + executed under its own per-view lock.
	// This ensures cascaded views are also protected from concurrent refresh.

	// Upstream cascade: refresh ancestors first (this may populate our delta tables).
	if (cascade_mode == "upstream" || cascade_mode == "both") {
		auto upstream = metadata.GetUpstreamViews(view_name);
		for (auto &dep : upstream) {
			auto dep_location = ResolveViewLocation(con, dep, view_catalog_name, view_schema_name);
			RefreshViewLocked(context, dep_location.catalog_name, dep_location.schema_name, dep,
			                  dep_location.cross_system, attached_db_catalog_name, attached_db_schema_name);
		}
	}

	// Early exit: skip refresh if all delta tables are empty.
	// Placed AFTER upstream cascade so that upstream refreshes have a chance to populate
	// our delta tables before we check.
	Value skip_empty_val;
	bool skip_empty_enabled = true;
	if (context.TryGetCurrentSetting("ivm_skip_empty_deltas", skip_empty_val) && !skip_empty_val.IsNull()) {
		skip_empty_enabled = skip_empty_val.GetValue<bool>();
	}
	if (skip_empty_enabled) {
		auto view_type = metadata.GetViewType(view_name);
		if (view_type != IVMType::FULL_REFRESH) {
			auto delta_tables = metadata.GetDeltaTables(view_name);
			bool all_empty = true;

			for (auto &dt : delta_tables) {
				if (metadata.IsDuckLakeTable(view_name, dt)) {
					Connection snap_con(*context.db.get());
					auto loc =
					    ResolveDuckLakeSourceLocation(snap_con, view_name, dt, view_catalog_name, view_schema_name,
					                                  attached_db_catalog_name, attached_db_schema_name);
					auto cur_snap_result = snap_con.Query(
					    "SELECT id FROM " + OpenIVMUtils::QuoteIdentifier(loc.catalog_name) + ".current_snapshot()");
					if (cur_snap_result->HasError() || cur_snap_result->RowCount() == 0 ||
					    cur_snap_result->GetValue(0, 0).IsNull()) {
						all_empty = false;
						break;
					}
					auto ducklake_current_snap = cur_snap_result->GetValue(0, 0).GetValue<int64_t>();
					auto last_snap = metadata.GetLastSnapshotId(view_name, dt);
					if (last_snap == ducklake_current_snap) {
						continue;
					}
					auto has_changes =
					    snap_con.Query("SELECT EXISTS(SELECT 1 FROM ("
					                   "(SELECT 1 FROM ducklake_table_insertions('" +
					                   OpenIVMUtils::EscapeValue(loc.catalog_name) + "', '" +
					                   OpenIVMUtils::EscapeValue(loc.schema_name) + "', '" +
					                   OpenIVMUtils::EscapeValue(loc.table_name) + "', " + to_string(last_snap) + ", " +
					                   to_string(ducklake_current_snap) +
					                   ") LIMIT 1) "
					                   "UNION ALL "
					                   "(SELECT 1 FROM ducklake_table_deletions('" +
					                   OpenIVMUtils::EscapeValue(loc.catalog_name) + "', '" +
					                   OpenIVMUtils::EscapeValue(loc.schema_name) + "', '" +
					                   OpenIVMUtils::EscapeValue(loc.table_name) + "', " + to_string(last_snap) + ", " +
					                   to_string(ducklake_current_snap) + ") LIMIT 1)) _ivm_delta_probe LIMIT 1)");
					if (has_changes->HasError() || has_changes->RowCount() == 0 ||
					    has_changes->GetValue(0, 0).IsNull() || has_changes->GetValue(0, 0).GetValue<bool>()) {
						all_empty = false;
						break;
					}
					continue;
				}
				if (metadata.IsDeltaTable(view_name, dt)) {
					auto last_version = metadata.GetLastSnapshotId(view_name, dt);
					auto source_path = metadata.GetSourcePath(view_name, dt);
					auto version_result = con.Query(
					    "SELECT COALESCE(MAX(CAST(regexp_extract(file, '([0-9]+)\\.json$', 1) AS BIGINT)), -1) "
					    "FROM glob('" +
					    OpenIVMUtils::EscapeSingleQuotes(source_path) + "/_delta_log/*.json')");
					if (version_result->HasError() || version_result->RowCount() == 0 ||
					    version_result->GetValue(0, 0).IsNull()) {
						all_empty = false;
						break;
					}
					auto current_version = version_result->GetValue(0, 0).GetValue<int64_t>();
					if (last_version < current_version) {
						all_empty = false;
						break;
					}
					continue;
				}
				string delta_probe = OpenIVMUtils::QuoteIdentifier(dt);
				if (cross_system) {
					delta_probe = ResolveStandardDeltaQualifiedName(con, view_name, dt, "", "");
				} else if (!view_catalog_name.empty() && !view_schema_name.empty()) {
					delta_probe = QualifiedName(view_catalog_name, view_schema_name, dt);
				}
				auto last_update = metadata.GetLastUpdate(view_name, dt);
				if (last_update.empty()) {
					all_empty = false;
					break;
				}
				auto count_result =
				    con.Query("SELECT COUNT(*) FROM " + delta_probe + " WHERE " + string(ivm::TIMESTAMP_COL) + " >= '" +
				              OpenIVMUtils::EscapeValue(last_update) + "'::TIMESTAMP");
				if (!count_result->HasError() && count_result->GetValue(0, 0).GetValue<int64_t>() > 0) {
					all_empty = false;
					break;
				}
				if (count_result->HasError()) {
					all_empty = false;
					break;
				}
			}
			if (all_empty) {
				OPENIVM_DEBUG_PRINT("[UPSERT] All delta tables empty — skipping refresh for '%s'\n", view_name.c_str());
				return;
			}
		}
	} // skip_empty_enabled

	// Check for refresh hooks (custom SQL to run before/after/instead of IVM)
	string hook_sql;
	string hook_mode;
	{
		auto hook_r = con.Query("SELECT hook_sql, mode FROM _duckdb_ivm_refresh_hooks WHERE view_name = '" +
		                        OpenIVMUtils::EscapeValue(view_name) + "'");
		if (!hook_r->HasError() && hook_r->RowCount() > 0) {
			hook_sql = hook_r->GetValue(0, 0).ToString();
			hook_mode = StringUtil::Lower(hook_r->GetValue(1, 0).ToString());
		}
	}

	if (!hook_sql.empty() && hook_mode == "before") {
		auto hr = con.Query(hook_sql);
		if (hr->HasError()) {
			Printer::Print("Warning: before-hook for '" + view_name + "' failed: " + hr->GetError());
		}
	}

	if (hook_mode != "replace") {
		RefreshViewLocked(context, view_catalog_name, view_schema_name, view_name, cross_system,
		                  attached_db_catalog_name, attached_db_schema_name);
	}

	if (!hook_sql.empty() && (hook_mode == "after" || hook_mode == "replace")) {
		auto hr = con.Query(hook_sql);
		if (hr->HasError()) {
			Printer::Print("Warning: " + hook_mode + "-hook for '" + view_name + "' failed: " + hr->GetError());
		}
	}

	// Downstream cascade: refresh dependents after
	if (cascade_mode == "downstream" || cascade_mode == "both") {
		auto downstream = metadata.GetDownstreamViews(view_name);
		for (auto &dep : downstream) {
			auto dep_location = ResolveViewLocation(con, dep, view_catalog_name, view_schema_name);
			RefreshViewLocked(context, dep_location.catalog_name, dep_location.schema_name, dep,
			                  dep_location.cross_system, attached_db_catalog_name, attached_db_schema_name);
		}
	}
}

// When cross_system is true (DuckLake MVs), the refresh SQL touches two catalogs:
// data ops write to dl.main.*, metadata ops write to the physical default.
// DuckDB forbids cross-catalog writes in one transaction. We split the SQL into
// three parts: pre-metadata (set_in_progress), data ops, post-metadata (timestamps).
// out_pre_meta / out_post_meta receive those parts; the return value is data-only.
// When cross_system is false (or out_pre_meta is null), returns the combined SQL.
static string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name,
                                 const string &view_schema_name, const string &view_name, bool cross_system,
                                 const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                 string *out_pre_meta, string *out_post_meta) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();
	Connection con(*context.db.get());
	Value skip_empty_val;
	bool skip_empty_enabled = true;
	if (context.TryGetCurrentSetting("ivm_skip_empty_deltas", skip_empty_val) && !skip_empty_val.IsNull()) {
		skip_empty_enabled = skip_empty_val.GetValue<bool>();
	}
	// Catalog-qualified prefix for OpenIVM internal state. DuckLake-targeted MVs expose
	// a DuckLake view, but _ivm_data_* and delta_<mv> live in the physical default DB so
	// refresh does not need a cross-catalog write transaction.
	string default_db;
	string default_schema = "main";
	{
		auto db_res = con.Query("SELECT current_database()");
		if (!db_res->HasError() && db_res->RowCount() > 0 && !db_res->GetValue(0, 0).IsNull()) {
			default_db = db_res->GetValue(0, 0).ToString();
		}
		auto schema_res = con.Query("SELECT current_schema()");
		if (!schema_res->HasError() && schema_res->RowCount() > 0 && !schema_res->GetValue(0, 0).IsNull()) {
			default_schema = schema_res->GetValue(0, 0).ToString();
		}
	}
	string catalog_prefix;
	if (!view_catalog_name.empty() && view_catalog_name != "memory") {
		catalog_prefix = OpenIVMUtils::QuoteIdentifier(view_catalog_name) + "." +
		                 OpenIVMUtils::QuoteIdentifier(view_schema_name) + ".";
	}
	string internal_catalog_name = view_catalog_name;
	string internal_schema_name = view_schema_name;
	string internal_catalog_prefix = catalog_prefix;
	if (cross_system && !default_db.empty() && default_db != "memory" && view_catalog_name != default_db) {
		internal_catalog_name = default_db;
		internal_schema_name = default_schema;
		internal_catalog_prefix =
		    OpenIVMUtils::QuoteIdentifier(default_db) + "." + OpenIVMUtils::QuoteIdentifier(default_schema) + ".";
	}
	// Bare table names for catalog lookups; qualified names for SQL
	string data_table_bare = IVMTableNames::DataTableName(view_name);
	string data_table = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table_bare);

	// Look up delta view and index.
	OPENIVM_DEBUG_PRINT("[UPSERT] Looking up delta view '%s' in catalog '%s.%s'\n",
	                    OpenIVMUtils::DeltaName(view_name).c_str(), view_catalog_name.c_str(),
	                    view_schema_name.c_str());
	optional_ptr<TableCatalogEntry> delta_view_catalog_entry;
	optional_ptr<CatalogEntry> index_delta_view_catalog_entry;
	if (internal_catalog_prefix.empty() || internal_catalog_name == default_db) {
		// Standard catalog: use Catalog API directly
		con.BeginTransaction();
		delta_view_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    *con.context, internal_catalog_name, internal_schema_name, OpenIVMUtils::DeltaName(view_name),
		    OnEntryNotFound::THROW_EXCEPTION, error_context);
		index_delta_view_catalog_entry = Catalog::GetEntry(
		    *con.context, internal_catalog_name, internal_schema_name,
		    EntryLookupInfo(CatalogType::INDEX_ENTRY, data_table_bare + ivm::INDEX_SUFFIX, error_context),
		    OnEntryNotFound::RETURN_NULL);
		con.Rollback();
	}
	// DuckLake: skip Catalog API (requires DuckLake transaction). Column names and
	// group columns come from metadata instead. delta_view_catalog_entry stays null.

	// IVMMetadata uses auto-commit queries (no explicit transaction needed)
	IVMMetadata metadata(con);
	auto view_query_sql = metadata.GetViewQuery(view_name);
	if (view_query_sql.empty()) {
		throw ParserException("View not found! Please call IVM with a materialized view.");
	}
	IVMType view_query_type = metadata.GetViewType(view_name);
	OPENIVM_DEBUG_PRINT("[UPSERT] View: %s, Type: %d, Query: %s\n", view_name.c_str(), (int)view_query_type,
	                    view_query_sql.c_str());
	auto delta_table_names = metadata.GetDeltaTables(view_name);
	view_query_sql =
	    QualifyViewQuerySources(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
	                            view_schema_name, attached_db_catalog_name, attached_db_schema_name);

	// Crash recovery: if a previous refresh was interrupted (process died between MERGE and
	// last_update), the flag is still true. Recover via full recompute to avoid double-applying deltas.
	{
		auto flag_result = con.Query("SELECT refresh_in_progress FROM " + string(ivm::VIEWS_TABLE) +
		                             " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "'");
		if (!flag_result->HasError() && flag_result->RowCount() > 0 && !flag_result->GetValue(0, 0).IsNull() &&
		    flag_result->GetValue(0, 0).GetValue<bool>()) {
			Printer::Print("Warning: recovering '" + view_name + "' from interrupted refresh via full recompute.");
			metadata.SetRefreshInProgress(view_name, false);
			return BuildRecomputeQuery(metadata, con, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name, internal_catalog_prefix, out_post_meta);
		}
	}

	// AVG, MIN, MAX, HAVING use group-recompute strategy (not decomposable as simple deltas).
	// HAVING needs recompute because groups may enter/leave the result set after aggregate changes.
	// MIN, MAX use group-recompute. AVG is decomposed to SUM+COUNT by the parser (fully incremental).
	// HAVING needs recompute because groups may enter/leave the result set.
	// LEFT JOIN aggregates: group-recompute unless ivm_left_join_merge is on (Larson & Zhou).
	// FULL OUTER JOIN aggregates: group-recompute unless ivm_full_outer_merge is on (Zhang & Larson).
	bool source_has_left_join = metadata.HasLeftJoin(view_name);
	// Only query has_full_outer when the view has an outer join (avoids extra SQL for INNER-only views)
	bool source_has_full_outer = source_has_left_join && metadata.HasFullOuter(view_name);
	bool left_join_merge = false;
	Value lj_merge_val;
	if (source_has_left_join && !source_has_full_outer) {
		if (context.TryGetCurrentSetting("ivm_left_join_merge", lj_merge_val) && !lj_merge_val.IsNull()) {
			left_join_merge = lj_merge_val.GetValue<bool>();
		}
	}
	bool full_outer_merge = false;
	if (source_has_full_outer) {
		Value foj_merge_val;
		if (context.TryGetCurrentSetting("ivm_full_outer_merge", foj_merge_val) && !foj_merge_val.IsNull()) {
			full_outer_merge = foj_merge_val.GetValue<bool>();
		}
	}
	bool has_minmax = metadata.HasMinMax(view_name) || view_query_type == IVMType::AGGREGATE_HAVING ||
	                  (source_has_left_join && !source_has_full_outer && !left_join_merge) ||
	                  (source_has_full_outer && !full_outer_merge);

	// Check ivm_refresh_mode: 'full' forces full recompute, skipping the IVM pipeline.
	Value refresh_mode_val;
	bool force_full_refresh = false;
	if (context.TryGetCurrentSetting("ivm_refresh_mode", refresh_mode_val) && !refresh_mode_val.IsNull()) {
		auto mode = StringUtil::Lower(refresh_mode_val.ToString());
		if (mode == "full") {
			force_full_refresh = true;
		}
	}

	if (force_full_refresh || view_query_type == IVMType::FULL_REFRESH) {
		return BuildRecomputeQuery(metadata, con, view_name, view_query_sql, cross_system, attached_db_catalog_name,
		                           attached_db_schema_name, internal_catalog_prefix, out_post_meta);
	}

	// Adaptive cost model (experimental): estimate IVM vs full recompute cost.
	// Gated by ivm_adaptive_refresh setting (default off — always use IVM).
	Value ivm_adaptive_val;
	bool ivm_adaptive = false;
	if (context.TryGetCurrentSetting("ivm_adaptive_refresh", ivm_adaptive_val) && !ivm_adaptive_val.IsNull()) {
		ivm_adaptive = ivm_adaptive_val.GetValue<bool>();
	}
	if (ivm_adaptive) {
		con.BeginTransaction();
		Parser cost_parser;
		cost_parser.ParseQuery(view_query_sql);
		Planner cost_planner(*con.context);
		cost_planner.CreatePlan(cost_parser.statements[0]->Copy());
		Optimizer cost_optimizer(*cost_planner.binder, *con.context);
		auto cost_plan = cost_optimizer.Optimize(std::move(cost_planner.plan));

		auto cost_estimate = EstimateIVMCost(*con.context, *cost_plan, view_name);
		con.Rollback();
		if (cost_estimate.ShouldRecompute()) {
			OPENIVM_DEBUG_PRINT("[ADAPTIVE] Full recompute is cheaper — skipping IVM\n");
			return BuildRecomputeQuery(metadata, con, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name, internal_catalog_prefix, out_post_meta);
		}
	}

	// IVM path: proceed with incremental maintenance

	// Get column names from the delta view table.
	vector<string> column_names;
	vector<LogicalType> column_types;
	bool list_mode = false;
	if (delta_view_catalog_entry) {
		// Standard catalog: use catalog entry directly
		auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
		const ColumnList &delta_view_columns = delta_view_entry->GetColumns();
		column_names = delta_view_columns.GetColumnNames();
		for (auto &col : delta_view_columns.Logical()) {
			column_types.push_back(col.GetType());
			if (col.GetName() != ivm::MULTIPLICITY_COL && col.GetType().id() == LogicalTypeId::LIST) {
				list_mode = true;
			}
		}
	} else {
		// DuckLake: get column names + types via SQL
		auto col_result =
		    con.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = '" +
		              OpenIVMUtils::EscapeValue(internal_catalog_name) + "' AND table_schema = '" +
		              OpenIVMUtils::EscapeValue(internal_schema_name) + "' AND table_name = '" +
		              OpenIVMUtils::EscapeValue(OpenIVMUtils::DeltaName(view_name)) + "' ORDER BY ordinal_position");
		if (!col_result->HasError()) {
			for (idx_t i = 0; i < col_result->RowCount(); i++) {
				column_names.push_back(col_result->GetValue(0, i).ToString());
				try {
					column_types.push_back(
					    TransformStringToLogicalType(col_result->GetValue(1, i).ToString(), context));
				} catch (...) {
					column_types.push_back(LogicalType::VARCHAR);
				}
			}
		}
	}

	// Check if the delta view has a timestamp column (present when created via CREATE MATERIALIZED VIEW)
	bool has_ts_col =
	    std::find(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL)) != column_names.end();
	// Remove _duckdb_ivm_timestamp — it's auto-filled by DEFAULT (for chained MV support).
	// Keep column_names and column_types aligned: drop the type at the same position.
	auto it = std::find(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL));
	if (it != column_names.end()) {
		auto offset = it - column_names.begin();
		column_names.erase(it);
		if (offset < static_cast<decltype(offset)>(column_types.size())) {
			column_types.erase(column_types.begin() + offset);
		}
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] List mode: %s\n", list_mode ? "true" : "false");

	string upsert_query;

	// Build a timestamp filter for the delta_view reads in the upsert query.
	// This prevents double-counting when chained MVs accumulate delta_view rows
	// from multiple refresh rounds (because downstream views haven't consumed them yet).
	//
	// We use `last_refresh_ts` (wall-clock now() of the last refresh), NOT
	// `last_update` (MAX(base_ts)+1us, which can be earlier than the refresh's
	// wall-clock time). Companion rows written during a prior refresh have
	// ts = that refresh's now(); we need last_refresh_ts >= that now() so the
	// filter `ts > last_refresh_ts` correctly excludes them. Using last_update
	// here would under-filter and let prior companion rows get re-processed.
	string delta_ts_filter = BuildDeltaTimestampFilter(con, view_name, has_ts_col);

	// Detect LEFT JOIN: the parser adds _ivm_left_key as a hidden column for LEFT/RIGHT JOIN views.
	// If the MV has this column, use it for the partial recompute filter.
	bool has_left_join = std::find(column_names.begin(), column_names.end(), ivm::LEFT_KEY_COL) != column_names.end();
	// Detect FULL OUTER JOIN: the parser also adds _ivm_right_key for bidirectional recompute.
	bool has_full_outer = std::find(column_names.begin(), column_names.end(), ivm::RIGHT_KEY_COL) != column_names.end();
	OPENIVM_DEBUG_PRINT("[UPSERT] has_left_join=%d has_full_outer=%d\n", has_left_join, has_full_outer);

	// Detect insert-only deltas: when the delta view contains only insert rows,
	// we can skip the zero-row DELETE (aggregates) and the DELETE+consolidation (projections).
	//
	// Safety rules:
	// - Non-join views: safe if all base deltas are insert-only
	// - DuckLake join views: safe if all base deltas are insert-only (N-term telescoping
	//   has no XOR cross-terms, so insert-only base = insert-only delta view)
	// - Standard join views: safe only if exactly ONE table changed AND it's insert-only
	//   (no cross-terms fire when other deltas are empty, so no XOR)
	bool insert_only = false;
	{
		// A view has a join if it references more than one base table (delta_table_names.size() > 1)
		// OR if its SQL contains a JOIN keyword. The string search catches self-joins (same table
		// referenced twice, so only one delta table registered). Case-insensitive match without
		// space requirement handles mixed-case SQL and LPTS fallback queries.
		bool has_join = delta_table_names.size() > 1 || view_query_sql.find("JOIN") != string::npos ||
		                view_query_sql.find("join") != string::npos;

		// Per-table analysis: is each delta empty, insert-only, or has deletes?
		idx_t tables_with_changes = 0;
		bool any_has_deletes = false;
		bool all_ducklake = true;

		for (auto &dt : delta_table_names) {
			if (metadata.IsDuckLakeTable(view_name, dt)) {
				auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
				                                         attached_db_catalog_name, attached_db_schema_name);
				auto cur_snap_result = con.Query("SELECT id FROM " + OpenIVMUtils::QuoteIdentifier(loc.catalog_name) +
				                                 ".current_snapshot()");
				auto last_snap = metadata.GetLastSnapshotId(view_name, dt);
				if (cur_snap_result->HasError() || cur_snap_result->RowCount() == 0 ||
				    cur_snap_result->GetValue(0, 0).IsNull()) {
					any_has_deletes = true; // conservative
					tables_with_changes++;
					continue;
				}
				auto cur_snap = cur_snap_result->GetValue(0, 0).GetValue<int64_t>();
				if (last_snap == cur_snap) {
					continue; // no changes — empty delta
				}
				auto count_result = con.Query(
				    "SELECT "
				    "(SELECT COUNT(*) FROM ducklake_table_insertions('" +
				    OpenIVMUtils::EscapeValue(loc.catalog_name) + "', '" + OpenIVMUtils::EscapeValue(loc.schema_name) +
				    "', '" + OpenIVMUtils::EscapeValue(loc.table_name) + "', " + to_string(last_snap) + ", " +
				    to_string(cur_snap) +
				    ")), "
				    "(SELECT COUNT(*) FROM ducklake_table_deletions('" +
				    OpenIVMUtils::EscapeValue(loc.catalog_name) + "', '" + OpenIVMUtils::EscapeValue(loc.schema_name) +
				    "', '" + OpenIVMUtils::EscapeValue(loc.table_name) + "', " + to_string(last_snap) + ", " +
				    to_string(cur_snap) + "))");
				if (count_result->HasError()) {
					any_has_deletes = true;
					tables_with_changes++;
					continue;
				}
				auto insert_count = count_result->GetValue(0, 0).GetValue<int64_t>();
				auto delete_count = count_result->GetValue(1, 0).GetValue<int64_t>();
				if (insert_count == 0 && delete_count == 0) {
					continue;
				}
				tables_with_changes++;
				if (delete_count > 0) {
					any_has_deletes = true;
				}
			} else if (metadata.IsDeltaTable(view_name, dt)) {
				auto last_version = metadata.GetLastSnapshotId(view_name, dt);
				auto source_path = metadata.GetSourcePath(view_name, dt);
				auto version_result =
				    con.Query("SELECT COALESCE(MAX(CAST(regexp_extract(file, '([0-9]+)\\.json$', 1) AS BIGINT)), -1) "
				              "FROM glob('" +
				              OpenIVMUtils::EscapeSingleQuotes(source_path) + "/_delta_log/*.json')");
				if (version_result->HasError() || version_result->RowCount() == 0 ||
				    version_result->GetValue(0, 0).IsNull()) {
					any_has_deletes = true;
					tables_with_changes++;
					continue;
				}
				all_ducklake = false;
				auto current_version = version_result->GetValue(0, 0).GetValue<int64_t>();
				if (last_version < current_version) {
					tables_with_changes++;
				}
			} else {
				all_ducklake = false;
				auto ts_string = metadata.GetLastUpdate(view_name, dt);
				if (ts_string.empty()) {
					continue;
				}
				string delta_table_sql = cross_system ? ResolveStandardDeltaQualifiedName(con, view_name, dt, "", "")
				                                      : OpenIVMUtils::QuoteIdentifier(dt);
				// Check if delta has any rows at all
				auto total_result =
				    con.Query("SELECT COUNT(*) FROM " + delta_table_sql + " WHERE " + string(ivm::TIMESTAMP_COL) +
				              " >= '" + OpenIVMUtils::EscapeValue(ts_string) + "'::TIMESTAMP");
				if (total_result->HasError() || total_result->GetValue(0, 0).GetValue<int64_t>() == 0) {
					continue; // empty delta
				}
				tables_with_changes++;
				// Check for deletes
				auto del_result =
				    con.Query("SELECT COUNT(*) FROM " + delta_table_sql + " WHERE " + string(ivm::TIMESTAMP_COL) +
				              " >= '" + OpenIVMUtils::EscapeValue(ts_string) + "'::TIMESTAMP AND " +
				              string(ivm::MULTIPLICITY_COL) + " < 0");
				if (!del_result->HasError() && del_result->GetValue(0, 0).GetValue<int64_t>() > 0) {
					any_has_deletes = true;
				}
			}
		}

		if (any_has_deletes) {
			insert_only = false;
		} else if (!has_join) {
			// Non-join views: safe whenever all deltas are insert-only
			insert_only = true;
		} else if (all_ducklake) {
			// DuckLake join views: N-term telescoping has no XOR, always safe
			insert_only = true;
		} else if (tables_with_changes <= 1 && delta_table_names.size() > 1) {
			// Standard join views: safe when only one table changed AND there are
			// multiple distinct delta tables (rules out self-joins, where 1 delta
			// table maps to multiple join leaves that all change together).
			insert_only = true;
		}
	} // append_only_enabled
	// Read per-optimization flags to gate insert-only fast paths independently.
	Value skip_agg_del_val, skip_proj_del_val, minmax_incr_val;
	bool skip_agg_delete = insert_only;
	bool skip_proj_delete = insert_only;
	bool minmax_incremental = insert_only;
	if (context.TryGetCurrentSetting("ivm_skip_aggregate_delete", skip_agg_del_val) && !skip_agg_del_val.IsNull() &&
	    !skip_agg_del_val.GetValue<bool>()) {
		skip_agg_delete = false;
	}
	if (context.TryGetCurrentSetting("ivm_skip_projection_delete", skip_proj_del_val) && !skip_proj_del_val.IsNull() &&
	    !skip_proj_del_val.GetValue<bool>()) {
		skip_proj_delete = false;
	}
	if (context.TryGetCurrentSetting("ivm_minmax_incremental", minmax_incr_val) && !minmax_incr_val.IsNull() &&
	    !minmax_incr_val.GetValue<bool>()) {
		minmax_incremental = false;
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] insert_only=%d, skip_agg_delete=%d, skip_proj_delete=%d, minmax_incremental=%d\n",
	                    insert_only, skip_agg_delete, skip_proj_delete, minmax_incremental);

	// All DML (INSERT, DELETE, UPDATE, MERGE) targets the physical data table,
	// not the user-facing VIEW which excludes internal _ivm_* columns.
	// The compile functions receive view_name and compute data_table internally.
	// GROUP BY columns: from index (standard) or metadata (DuckLake fallback).
	auto group_cols = metadata.GetGroupColumns(view_name);
	auto agg_types = metadata.GetAggregateTypes(view_name);
	// ARG_MIN/ARG_MAX require group-recompute even for insert-only deltas — the
	// GREATEST/LEAST fast path used for MIN/MAX doesn't apply to these two-arg aggregates.
	bool has_argminmax = std::any_of(agg_types.begin(), agg_types.end(),
	                                 [](const string &t) { return t == "arg_min" || t == "arg_max"; });
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: %s\n", IVMTypeName(view_query_type));
	switch (view_query_type) {
	case IVMType::AGGREGATE_HAVING: {
		// When ivm_having_merge is enabled, the data table stores ALL groups (HAVING filter
		// is in the VIEW). Use standard MERGE — same as AGGREGATE_GROUP.
		// When disabled, fall back to group-recompute (has_minmax=true forces delete+re-insert).
		Value having_merge_val;
		bool having_merge = true;
		if (context.TryGetCurrentSetting("ivm_having_merge", having_merge_val) && !having_merge_val.IsNull()) {
			having_merge = having_merge_val.GetValue<bool>();
		}
		if (having_merge) {
			bool effective_insert_only = has_argminmax ? false : (has_minmax ? minmax_incremental : skip_agg_delete);
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, has_minmax, list_mode,
			    delta_ts_filter, group_cols, internal_catalog_prefix, effective_insert_only, agg_types, column_types);
		} else {
			upsert_query =
			    CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql,
			                           /*has_minmax=*/true, list_mode, delta_ts_filter, group_cols,
			                           internal_catalog_prefix, /*insert_only=*/false, agg_types, column_types);
		}
		break;
	}
	case IVMType::AGGREGATE_GROUP: {
		if (source_has_full_outer && !full_outer_merge) {
			// FULL OUTER JOIN aggregate group-recompute (Zhang & Larson):
			// Recompute only output groups touched by delta-view groups or changed FULL OUTER
			// join keys. Matching uses IS NOT DISTINCT FROM so NULL-padded groups are covered.
			upsert_query = BuildFullOuterAffectedGroupRefresh(metadata, view_name, delta_table_names, group_cols,
			                                                  data_table, view_query_sql, delta_ts_filter,
			                                                  internal_catalog_prefix, "_ivm_recompute");
		} else if (source_has_full_outer && full_outer_merge) {
			// Zhang & Larson MERGE for FULL OUTER JOIN aggregates:
			// Phase 1: MERGE handles matched-row changes via _ivm_match_count
			//          (same as LEFT JOIN MERGE — INNER-demoted delta view)
			// Phase 2: Recompute groups affected by UNMATCHED changes
			//          (left delta table + right→left join key mapping + NULL group)
			// ARG_MIN/ARG_MAX can't use the delta-sum MERGE template — force group-recompute.
			bool effective_insert_only = has_argminmax ? false : skip_agg_delete;
			upsert_query =
			    CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql,
			                           /*has_minmax=*/has_argminmax, list_mode, delta_ts_filter, group_cols,
			                           internal_catalog_prefix, effective_insert_only, agg_types, column_types);

			// Phase 2: recompute groups affected by unmatched/full-outer transfers.
			upsert_query += BuildFullOuterAffectedGroupRefresh(metadata, view_name, delta_table_names, group_cols,
			                                                   data_table, view_query_sql, delta_ts_filter,
			                                                   internal_catalog_prefix, "_ivm_unmatched");
		} else {
			// Standard path: MIN/MAX group-recompute or incremental MERGE
			bool effective_insert_only = has_argminmax ? false : (has_minmax ? minmax_incremental : skip_agg_delete);
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, has_minmax, list_mode,
			    delta_ts_filter, group_cols, internal_catalog_prefix, effective_insert_only, agg_types, column_types);
		}
		break;
	}
	case IVMType::SIMPLE_PROJECTION: {
		upsert_query = CompileProjectionRefresh(metadata, view_name, column_names, delta_table_names, data_table,
		                                        view_query_sql, delta_ts_filter, internal_catalog_prefix,
		                                        has_full_outer, has_left_join, skip_proj_delete);
		break;
	}

	case IVMType::SIMPLE_AGGREGATE: {
		// ARG_MIN/ARG_MAX can't be maintained by delta-sum UPDATE even for insert-only deltas.
		bool sa_insert_only = has_argminmax ? false : insert_only;
		upsert_query = CompileSimpleAggregates(view_name, column_names, view_query_sql, has_minmax, list_mode,
		                                       delta_ts_filter, internal_catalog_prefix, sa_insert_only, column_types);
		if (!has_minmax) {
			AppendSimpleAggregateEmptySourceNulling(metadata, upsert_query, view_name, column_names, data_table, con,
			                                        view_catalog_name, view_schema_name, attached_db_catalog_name,
			                                        attached_db_schema_name);
		}
		break;
	}
	case IVMType::WINDOW_PARTITION: {
		// Window functions: partition-level recompute — delete+re-insert affected partitions.
		auto partition_cols = metadata.GetGroupColumns(view_name); // reuses group_columns field
		vector<WindowPartitionDeltaSpec> partition_delta_specs;
		auto split_partition_spec = [](const string &raw) {
			auto pos = raw.find('=');
			if (pos == string::npos) {
				return std::make_pair(raw, raw);
			}
			return std::make_pair(raw.substr(0, pos), raw.substr(pos + 1));
		};
		auto delta_has_column = [&](const string &delta_table, const string &column_name) {
			auto col_result = con.Query("SELECT 1 FROM information_schema.columns WHERE table_name = '" +
			                            OpenIVMUtils::EscapeValue(delta_table) + "' AND lower(column_name) = lower('" +
			                            OpenIVMUtils::EscapeValue(column_name) + "') LIMIT 1");
			return !col_result->HasError() && col_result->RowCount() > 0;
		};
		for (auto &raw_partition_col : partition_cols) {
			auto parsed = split_partition_spec(raw_partition_col);
			for (auto &dt : delta_table_names) {
				if (metadata.IsDuckLakeTable(view_name, dt)) {
					continue;
				}
				if (delta_has_column(dt, parsed.second)) {
					string delta_table_sql = cross_system
					                             ? ResolveStandardDeltaQualifiedName(con, view_name, dt, "", "")
					                             : OpenIVMUtils::QuoteIdentifier(dt);
					partition_delta_specs.push_back({dt, delta_table_sql, parsed.first, parsed.second});
				}
			}
		}
		// For DuckLake base tables the delta source isn't a `delta_<T>` twin with
		// `_duckdb_ivm_timestamp` — the delta_table_names entry is the DuckLake base name
		// itself. Build the affected-partition filter via DuckLake snapshot diff instead:
		// symmetric difference between current and last_snapshot_id.
		bool any_ducklake = false;
		for (auto &dt : delta_table_names) {
			if (metadata.IsDuckLakeTable(view_name, dt)) {
				any_ducklake = true;
				break;
			}
		}
		// Only use snapshot-diff recompute when every partition col is in the MV's output
		// so the DELETE/INSERT can filter affected partitions. Single-source windows can
		// derive keys directly from the source table diff; multi-source windows compare the
		// old/new view's partition-key projection at DuckLake snapshots.
		bool safe_for_snapdiff = any_ducklake && !partition_cols.empty();
		if (safe_for_snapdiff) {
			for (auto &pc : partition_cols) {
				auto parsed = split_partition_spec(pc);
				if (std::find(column_names.begin(), column_names.end(), parsed.first) == column_names.end()) {
					safe_for_snapdiff = false;
					break;
				}
			}
		}
		if (safe_for_snapdiff && delta_table_names.size() == 1) {
			const string &base_name = delta_table_names[0];
			int64_t old_snap = metadata.GetLastSnapshotId(view_name, base_name);
			auto loc = ResolveDuckLakeSourceLocation(con, view_name, base_name, view_catalog_name, view_schema_name,
			                                         attached_db_catalog_name, attached_db_schema_name);
			auto snap_result =
			    con.Query("SELECT id FROM " + OpenIVMUtils::QuoteIdentifier(loc.catalog_name) + ".current_snapshot()");
			int64_t current_snap = old_snap;
			if (!snap_result->HasError() && snap_result->RowCount() > 0 && !snap_result->GetValue(0, 0).IsNull()) {
				current_snap = snap_result->GetValue(0, 0).GetValue<int64_t>();
			}

			string affected_cols;
			string affected_select;
			string affected_tuple;
			for (size_t i = 0; i < partition_cols.size(); i++) {
				if (i > 0) {
					affected_cols += ", ";
					affected_select += ", ";
					affected_tuple += ", ";
				}
				auto parsed = split_partition_spec(partition_cols[i]);
				affected_cols += KeywordHelper::WriteOptionallyQuoted(parsed.first);
				affected_select += KeywordHelper::WriteOptionallyQuoted(parsed.second) + " AS " +
				                   KeywordHelper::WriteOptionallyQuoted(parsed.first);
				affected_tuple += KeywordHelper::WriteOptionallyQuoted(parsed.first);
			}
			string temp_affected = string(ivm::TEMP_TABLE_PREFIX) + "affected_" + view_name;
			string qtemp_affected = KeywordHelper::WriteOptionallyQuoted(temp_affected);
			string insertions = "SELECT " + affected_select + " FROM ducklake_table_insertions('" +
			                    OpenIVMUtils::EscapeValue(loc.catalog_name) + "', '" +
			                    OpenIVMUtils::EscapeValue(loc.schema_name) + "', '" +
			                    OpenIVMUtils::EscapeValue(loc.table_name) + "', " + to_string(old_snap) + ", " +
			                    to_string(current_snap) + ")";
			string deletions = "SELECT " + affected_select + " FROM ducklake_table_deletions('" +
			                   OpenIVMUtils::EscapeValue(loc.catalog_name) + "', '" +
			                   OpenIVMUtils::EscapeValue(loc.schema_name) + "', '" +
			                   OpenIVMUtils::EscapeValue(loc.table_name) + "', " + to_string(old_snap) + ", " +
			                   to_string(current_snap) + ")";
			string affected_filter;
			if (partition_cols.size() == 1) {
				affected_filter = affected_tuple + " IN (SELECT " + affected_cols + " FROM " + qtemp_affected + ")";
			} else {
				affected_filter =
				    "(" + affected_tuple + ") IN (SELECT " + affected_cols + " FROM " + qtemp_affected + ")";
			}
			upsert_query = "CREATE TEMP TABLE " + qtemp_affected + " AS SELECT DISTINCT " + affected_cols + " FROM ((" +
			               insertions + ") UNION ALL (" + deletions + ")) _ivm_changed_partitions;\n";
			upsert_query += "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n";
			upsert_query += "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql +
			                ") _ivm_recompute\nWHERE " + affected_filter + ";\n";
			upsert_query += "DROP TABLE " + qtemp_affected + ";\n";
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake change-feed, %zu "
			                    "partition cols, old_snap=%ld, current_snap=%ld)\n",
			                    partition_cols.size(), (long)old_snap, (long)current_snap);
		} else if (safe_for_snapdiff && any_ducklake) {
			string old_view_query = BuildDuckLakeSnapshotQuery(metadata, con, view_name, view_query_sql,
			                                                   delta_table_names, view_catalog_name, view_schema_name,
			                                                   attached_db_catalog_name, attached_db_schema_name);
			string key_cols;
			string key_tuple;
			for (size_t i = 0; i < partition_cols.size(); i++) {
				if (i > 0) {
					key_cols += ", ";
					key_tuple += ", ";
				}
				auto parsed = split_partition_spec(partition_cols[i]);
				string output_col = KeywordHelper::WriteOptionallyQuoted(parsed.first);
				key_cols += output_col;
				key_tuple += output_col;
			}
			string current_rows = "SELECT * FROM (" + view_query_sql + ") _ivm_current_rows";
			string old_rows = "SELECT * FROM (" + old_view_query + ") _ivm_old_rows";
			string changed_rows = "((" + current_rows + ") EXCEPT ALL (" + old_rows + ")) UNION ALL ((" + old_rows +
			                      ") EXCEPT ALL (" + current_rows + "))";
			string affected_keys = "SELECT DISTINCT " + key_cols + " FROM (" + changed_rows + ") _ivm_changed_rows";
			string affected_filter;
			if (partition_cols.size() == 1) {
				affected_filter = key_tuple + " IN (" + affected_keys + ")";
			} else {
				affected_filter = "(" + key_tuple + ") IN (" + affected_keys + ")";
			}
			upsert_query = "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n" + "INSERT INTO " +
			               data_table + "\nSELECT * FROM (" + view_query_sql + ") _ivm_recompute\nWHERE " +
			               affected_filter + ";\n";
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake view-diff, %zu "
			                    "partition cols, %zu sources)\n",
			                    partition_cols.size(), delta_table_names.size());
		} else if (any_ducklake) {
			// DuckLake edge case: partition col not in MV output.
			// Full recompute — documented in docs/limitations.md.
			upsert_query =
			    "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
			OPENIVM_DEBUG_PRINT(
			    "[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake, full recompute fallback)\n");
		} else {
			upsert_query = CompileWindowRecompute(view_name, view_query_sql, delta_ts_filter, internal_catalog_prefix,
			                                      partition_cols, partition_delta_specs);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (%zu partition cols)\n",
			                    partition_cols.size());
		}
		break;
	}
	case IVMType::DISTINCT_INCREMENTAL: {
		// Aux-state DBSP-correct DISTINCT pipeline. Reads metadata captured at CREATE-MV
		// time (aux_table, distinct_cols, source/filter, single-SUM spec) and emits a
		// multi-statement batch:
		//   1) Δinput temp table (per-distinct-tuple net multiplicity from source delta)
		//   2) MERGE Δagg into _ivm_data_<view> (Δdistinct = ±1 transitions × sum_arg)
		//   3) DELETE rows whose _ivm_count_star fell to 0
		//   4) MERGE Δinput into the aux table; DELETE rows with _count ≤ 0
		// If metadata is missing (older view created before this column landed) we fall
		// through to GROUP_RECOMPUTE so the view still refreshes correctly.
		IVMMetadata::DistinctAuxMeta aux_meta;
		if (metadata.GetDistinctAuxMeta(view_name, aux_meta)) {
			auto group_columns = metadata.GetGroupColumns(view_name);
			string delta_source = OpenIVMUtils::DeltaName(aux_meta.source);
			string ts = metadata.GetLastUpdate(view_name, delta_source);
			upsert_query = CompileDistinctIncremental(
			    view_name, aux_meta.aux_table, aux_meta.cols, delta_source, ts, aux_meta.filter, group_columns,
			    aux_meta.sum_arg, aux_meta.sum_out, string(ivm::COUNT_STAR_COL), internal_catalog_prefix);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: DISTINCT_INCREMENTAL (%zu distinct cols, "
			                    "%zu group cols, sum_arg=%s, sum_out=%s)\n",
			                    aux_meta.cols.size(), group_columns.size(), aux_meta.sum_arg.c_str(),
			                    aux_meta.sum_out.c_str());
			break;
		}
		OPENIVM_DEBUG_PRINT("[UPSERT] DISTINCT_INCREMENTAL view has no aux meta — falling through to "
		                    "GROUP_RECOMPUTE\n");
		[[fallthrough]];
	}
	case IVMType::SEMI_ANTI_RECOMPUTE: {
		IVMMetadata::SemiAntiAuxMeta aux_meta;
		if (metadata.GetSemiAntiAuxMeta(view_name, aux_meta)) {
			auto resolve_delta_name = [&](const string &table_name) {
				string wanted = OpenIVMUtils::DeltaName(table_name);
				for (auto &dt : delta_table_names) {
					if (StringUtil::CIEquals(dt, wanted)) {
						return dt;
					}
				}
				return wanted;
			};
			string left_delta = resolve_delta_name(aux_meta.left_table);
			string right_delta = resolve_delta_name(aux_meta.right_table);
			string left_ts = metadata.GetLastUpdate(view_name, left_delta);
			string right_ts = metadata.GetLastUpdate(view_name, right_delta);
			if (left_ts.empty()) {
				left_delta.clear();
			}
			upsert_query = CompileSemiAntiRecompute(
			    view_name, aux_meta.aux_table, aux_meta.join_type, aux_meta.left_table, aux_meta.left_alias,
			    aux_meta.right_table, aux_meta.right_alias, aux_meta.predicate, aux_meta.post_filter,
			    aux_meta.left_cols, aux_meta.left_exprs, aux_meta.output_cols, left_delta, right_delta, left_ts,
			    right_ts, internal_catalog_prefix);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: SEMI_ANTI_RECOMPUTE (%s, %zu left cols)\n",
			                    aux_meta.join_type.c_str(), aux_meta.left_cols.size());
			break;
		}
		OPENIVM_DEBUG_PRINT("[UPSERT] SEMI_ANTI_RECOMPUTE view has no aux meta — falling through to "
		                    "GROUP_RECOMPUTE\n");
		[[fallthrough]];
	}
	case IVMType::GROUP_RECOMPUTE: {
		// Inner-DISTINCT-under-AGG: re-evaluate only the GROUP BY tuples touched by source
		// deltas. For each base table register a (base_name, last_update) spec; the compile
		// helper substitutes that table's reference in the LPTS view query with a
		// delta-filtered subselect, projects DISTINCT GROUP BY columns, unions across
		// sources, and uses the resulting key set to scope DELETE + INSERT.
		auto group_columns = metadata.GetGroupColumns(view_name);
		string recompute_ducklake_catalog =
		    ResolveDuckLakeCatalogName(con, view_catalog_name, attached_db_catalog_name);
		string recompute_ducklake_schema = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
		auto delta_specs = BuildGroupRecomputeDeltaSpecs(metadata, view_name, con, delta_table_names,
		                                                 recompute_ducklake_catalog, recompute_ducklake_schema);
		// LPTS emits fully-qualified `cat.schema.tbl` even when the catalog is the default
		// `memory` (where `catalog_prefix` for SQL emission is empty). Reconstruct the
		// always-qualified prefix so the source-table substitution finds the LPTS form
		// verbatim — DuckDB's defaults are `memory` / `main` when these come back empty.
		string lpts_table_prefix = BuildLptsTablePrefix(view_catalog_name, view_schema_name);
		upsert_query = CompileGroupRecompute(view_name, view_query_sql, group_columns, delta_specs,
		                                     internal_catalog_prefix, lpts_table_prefix);
		OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: GROUP_RECOMPUTE (%zu group cols, %zu sources)\n",
		                    group_columns.size(), delta_specs.size());
		break;
	}
	case IVMType::TOP_K:
		// Top-k is handled before this enum: the parser strips ORDER BY/LIMIT into
		// the user-facing view and classifies the maintained data query as
		// AGGREGATE_GROUP or SIMPLE_PROJECTION. The TOP_K enum is not assigned.
		[[fallthrough]];
	case IVMType::FULL_REFRESH: {
		// Should not reach here — full refresh is handled earlier via BuildRecomputeQuery.
		throw InternalException("FULL_REFRESH views should not reach incremental upsert compilation");
	}
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Upsert query:\n%s\n", upsert_query.c_str());
	// DoIVM is a table function (root of the tree)
	string ivm_query;
	string companion_query;
	string pre_companion;
	string post_companion;
	string compact_delta_view_query;
	string delete_from_view_query;
	string delta_view_name = internal_catalog_prefix + OpenIVMUtils::DeltaName(view_name);

	if (view_query_type == IVMType::WINDOW_PARTITION || view_query_type == IVMType::GROUP_RECOMPUTE ||
	    view_query_type == IVMType::DISTINCT_INCREMENTAL || view_query_type == IVMType::SEMI_ANTI_RECOMPUTE) {
		// These types use a recompute path (partition-scoped, group-scoped, or — once
		// the aux-state pipeline lands — distinct-tuple-transition-only) on the data
		// table directly. No delta-join plan needed, so skip the DoIVM/rewrite-rule path.
		OPENIVM_DEBUG_PRINT("[UPSERT] Skipping DoIVM for %s\n",
		                    view_query_type == IVMType::DISTINCT_INCREMENTAL  ? "DISTINCT_INCREMENTAL"
		                    : view_query_type == IVMType::SEMI_ANTI_RECOMPUTE ? "SEMI_ANTI_RECOMPUTE"
		                    : view_query_type == IVMType::GROUP_RECOMPUTE     ? "GROUP_RECOMPUTE"
		                                                                      : "WINDOW_PARTITION");
		ivm_query = "";
	} else {
		// splitting the query in two to make it easier to turn into string (insertions are the same)
		string do_ivm = "select * from DoIVM('" + OpenIVMUtils::EscapeValue(internal_catalog_name) + "','" +
		                OpenIVMUtils::EscapeValue(internal_schema_name) + "','" + OpenIVMUtils::EscapeValue(view_name) +
		                "');";

		// now we can plan the query
		OPENIVM_DEBUG_PRINT("[UPSERT] Planning DoIVM query: %s\n", do_ivm.c_str());
		Parser p;
		p.ParseQuery(do_ivm);

		con.BeginTransaction();
		auto &con_ctx = *con.context;
		OPENIVM_DEBUG_PRINT("[UPSERT] Creating planner...\n");
		Planner planner(con_ctx);
		OPENIVM_DEBUG_PRINT("[UPSERT] CreatePlan...\n");
		planner.CreatePlan(std::move(p.statements[0]));
		auto plan = std::move(planner.plan);
		OPENIVM_DEBUG_PRINT("[UPSERT] Plan created. Running optimizer...\n");
		Optimizer optimizer(*planner.binder, con_ctx);
		plan = optimizer.Optimize(std::move(plan)); // this transforms the plan into an incremental plan
		OPENIVM_DEBUG_PRINT("[UPSERT] Optimizer done.\n");
		con.Rollback();

		// Convert the rewritten plan to SQL via the AST pipeline.
		// LPTS does not cover every operator (table functions, certain WINDOW shapes,
		// nested subqueries, etc.). If serialization fails, fall back to a full
		// recompute of the MV — semantically correct, slower, but safe. Without
		// this fallback the whole PRAGMA ivm() call would surface "Invalid Error:
		// map::at" and the user gets a cryptic crash instead of a working refresh.
		string raw_ivm_sql;
		if (IsEmptyDeltaPlan(plan.get())) {
			raw_ivm_sql = BuildEmptyDeltaInsert(view_name, column_names, column_types);
			OPENIVM_DEBUG_PRINT("[UPSERT] Delta plan is empty; generated no-op delta insert for '%s'\n",
			                    view_name.c_str());
		} else {
			try {
				auto ast = LogicalPlanToAst(con_ctx, plan);
				auto cte_list = AstToCteList(*ast);
				raw_ivm_sql = cte_list->ToQuery(false);
				OPENIVM_DEBUG_PRINT("[UPSERT] ToQuery done. SQL:\n%s\n", raw_ivm_sql.c_str());
			} catch (const std::exception &e) {
				Printer::Print("Warning: materialized view '" + view_name +
				               "' uses constructs not supported by IVM's SQL serializer (" + e.what() +
				               "). Falling back to full recompute for this refresh.");
				OPENIVM_DEBUG_PRINT("[UPSERT] LPTS fallback (%s) for view '%s' → full recompute\n", e.what(),
				                    view_name.c_str());
				return BuildRecomputeQuery(metadata, con, view_name, view_query_sql, cross_system,
				                           attached_db_catalog_name, attached_db_schema_name, internal_catalog_prefix,
				                           out_post_meta);
			}
		}

		// Use explicit column list in INSERT INTO delta_view, excluding _duckdb_ivm_timestamp
		// so the DEFAULT now() fills it in (for chained MV support)
		string insert_target_bare = "INSERT INTO " + OpenIVMUtils::DeltaName(view_name);
		auto insert_pos = raw_ivm_sql.find(insert_target_bare);
		if (insert_pos != string::npos) {
			// Replace bare delta table name with catalog-qualified version
			if (!internal_catalog_prefix.empty()) {
				raw_ivm_sql.replace(insert_pos, insert_target_bare.size(), "INSERT INTO " + delta_view_name);
				insert_pos = raw_ivm_sql.find("INSERT INTO " + delta_view_name);
			}
			string col_list = "(";
			for (size_t i = 0; i < column_names.size(); i++) {
				if (i > 0) {
					col_list += ", ";
				}
				col_list += OpenIVMUtils::QuoteIdentifier(column_names[i]);
			}
			col_list += ") ";
			string full_insert = "INSERT INTO " + delta_view_name;
			raw_ivm_sql.insert(insert_pos + full_insert.size(), " " + col_list);
		}
		ivm_query += raw_ivm_sql;

		// Delete from delta view: timestamp-based if downstream views depend on it, unconditional otherwise
		auto downstream_check = con.Query("SELECT COUNT(*) FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                                  " WHERE table_name = '" + OpenIVMUtils::EscapeValue(delta_view_name) + "'");
		bool has_downstream = !downstream_check->HasError() && downstream_check->RowCount() > 0 &&
		                      downstream_check->GetValue(0, 0).GetValue<int64_t>() > 0;

		// Companion rows for downstream consumers.
		// When a view has downstream MVs that read its delta, those downstream views need
		// both the OLD and NEW state to correctly compute their own deltas.
		// The IVM query produces the NEW state (delta with mul=true).
		// The companion query records the OLD state (current MV rows with mul=false)
		// BEFORE the upsert modifies the MV.
		// For SIMPLE_AGGREGATE with downstream consumers:
		// The IVM delta represents the CHANGE (+5), but downstream projections need
		// the ABSOLUTE old and new values to compute their own deltas correctly.
		// Strategy: record old state (mul=false) BEFORE upsert, then record new state
		// (mul=true) AFTER upsert. The IVM delta in delta_view is replaced by these
		// absolute snapshots so downstream sees a clean old→new transition.

		if (has_downstream && view_query_type == IVMType::SIMPLE_AGGREGATE) {
			// Save old MV state to a temp table BEFORE the IVM+upsert modifies the MV.
			// After the upsert, clear IVM delta from delta_view and replace with
			// old(false) + new(true) absolute snapshots for downstream consumption.
			// SIMPLE_PROJECTION/FILTER views do not use this path: their IVM output is
			// already a row-level Z-set delta, while full old/new snapshots make downstream
			// DuckLake joins pay for unchanged rows before compaction can cancel them.
			string col_list;
			for (auto &col : column_names) {
				if (!col_list.empty()) {
					col_list += ", ";
				}
				col_list += OpenIVMUtils::QuoteIdentifier(col);
			}
			// Build "snapshot" select lists: for the multiplicity column we substitute
			// the literal Z-set weight (-1 for "old/retracted" snapshot, +1 for "new")
			// instead of the column reference.
			string select_old, select_new;
			bool first = true;
			for (auto &col : column_names) {
				if (!first) {
					select_old += ", ";
					select_new += ", ";
				}
				first = false;
				if (col == string(ivm::MULTIPLICITY_COL)) {
					select_old += "-1";
					select_new += "1";
				} else {
					select_old += OpenIVMUtils::QuoteIdentifier(col);
					select_new += OpenIVMUtils::QuoteIdentifier(col);
				}
			}
			// Pre: snapshot old state into temp table
			string temp_name = string(ivm::TEMP_TABLE_PREFIX) + view_name;
			string qt = KeywordHelper::WriteOptionallyQuoted(temp_name);
			string qdvn = KeywordHelper::WriteOptionallyQuoted(delta_view_name);
			const string &qdt2 = data_table;
			pre_companion = "CREATE TEMP TABLE " + qt + " AS SELECT * FROM " + qdt2 + ";\n";
			// Post: clear ALL IVM delta rows, replace with absolute -1/+1 snapshots
			post_companion = "DELETE FROM " + qdvn + " WHERE 1=1";
			if (!delta_ts_filter.empty()) {
				post_companion += " AND " + delta_ts_filter;
			}
			post_companion += ";\n";
			post_companion +=
			    "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_old + " FROM " + qt + ";\n";
			post_companion +=
			    "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_new + " FROM " + qdt2 + ";\n";
			post_companion += "DROP TABLE " + qt + ";\n";
			OPENIVM_DEBUG_PRINT("[UPSERT] Pre-companion: %s\n", pre_companion.c_str());
			OPENIVM_DEBUG_PRINT("[UPSERT] Post-companion: %s\n", post_companion.c_str());
		} else if ((view_query_type == IVMType::AGGREGATE_GROUP || view_query_type == IVMType::AGGREGATE_HAVING) &&
		           has_downstream && index_delta_view_catalog_entry) {
			auto *idx = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
			auto key_ids = idx->column_ids;
			vector<string> keys;
			unordered_set<string> keys_set;
			for (auto &kid : key_ids) {
				keys.push_back(column_names[kid]);
				keys_set.insert(column_names[kid]);
			}

			string col_list, val_list, join_cond;
			for (auto &col : column_names) {
				if (!col_list.empty()) {
					col_list += ", ";
					val_list += ", ";
				}
				col_list += col;
				if (keys_set.count(col)) {
					val_list += "d." + col;
				} else if (col == ivm::MULTIPLICITY_COL) {
					// Companion delta-view row that retracts the prior aggregate value:
					// emit weight -1 so downstream consumers subtract it.
					val_list += "-1";
				} else {
					val_list += "0";
				}
			}
			for (size_t i = 0; i < keys.size(); i++) {
				if (i > 0) {
					join_cond += " AND ";
				}
				join_cond += "d." + keys[i] + " IS NOT DISTINCT FROM m." + keys[i];
			}

			companion_query = "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + val_list + " FROM " +
			                  delta_view_name + " d WHERE d." + string(ivm::MULTIPLICITY_COL) + " > 0";
			if (!delta_ts_filter.empty()) {
				companion_query += " AND d." + delta_ts_filter;
			}
			companion_query += " AND EXISTS (SELECT 1 FROM " + data_table + " m WHERE " + join_cond + ");\n";
			OPENIVM_DEBUG_PRINT("[UPSERT] Companion query:\n%s\n", companion_query.c_str());
		}

		if (has_downstream) {
			if (skip_empty_enabled) {
				compact_delta_view_query =
				    BuildCompactDeltaViewSQL(view_name, delta_view_name, column_names, delta_ts_filter);
				OPENIVM_DEBUG_PRINT("[UPSERT] Compact delta-view query:\n%s\n", compact_delta_view_query.c_str());
			}
			delete_from_view_query = IVMMetadata::BuildDeltaCleanupSQL(delta_view_name, delta_view_name);
		} else {
			delete_from_view_query = "DELETE FROM " + delta_view_name + ";";
		}
	}

	// now we can also delete from the delta table, but only if all the dependent views have been refreshed
	// example: if two views A and B are on the same table T, we can only remove tuples from T
	// if both A and B have been refreshed (up to some timestamp)
	// to check this, we extract the minimum timestamp from _duckdb_ivm_delta_tables
	string delete_from_delta_table_query;
	// CONCURRENCY-SAFE TIMESTAMP ADVANCE:
	// The next refresh's delta-scan filter uses `ts >= last_update`. If we set
	// last_update = now() (which DuckDB evaluates at BEGIN TRANSACTION, BEFORE the
	// snapshot is taken at first-catalog-access), a concurrent DML that commits
	// between BEGIN and the snapshot-read would have ts > now() AND be visible in
	// THIS refresh's snapshot — so it's processed here, but the next refresh's
	// filter `ts >= now()` would still include it → double-apply drift.
	//
	// Fix: advance last_update to (max ts of rows visible in this tx's snapshot)
	// + 1us. Any row strictly newer than that was committed AFTER our snapshot,
	// so it wasn't processed here and will be caught by the next refresh. Any
	// row we DID process has ts <= max → excluded from the next refresh.
	//
	// Fallback: if this refresh saw zero rows (delta empty), use now() as before
	// so the cursor doesn't move backwards.
	// Two timestamps get bumped per (view, table) at end of refresh:
	//   - last_update     = MAX(base delta ts visible in this snapshot) + 1us
	//                       This is the "what base-delta rows have been processed"
	//                       cursor — always <= this refresh's snapshot time, so
	//                       the next refresh's filter `ts >= last_update` correctly
	//                       excludes everything we processed AND includes rows that
	//                       committed after our snapshot (race-safe).
	//   - last_refresh_ts = now() (transaction start wall clock)
	//                       This is the "when was the last refresh" marker used to
	//                       filter delta_<view> companion rows in chained MVs.
	//                       Separated from last_update because companion rows have
	//                       ts = refresh-time, which is typically later than
	//                       MAX(base_ts)+1us.
	string update_timestamp_query;
	for (auto &dt : delta_table_names) {
		if (metadata.IsExternallyVersionedTable(view_name, dt)) {
			// Externally versioned sources don't use _duckdb_ivm_timestamp; keep now() semantics.
			update_timestamp_query += "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
			                          " SET last_update = now(), last_refresh_ts = now() WHERE view_name = '" +
			                          OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
			                          OpenIVMUtils::EscapeValue(dt) + "';\n";
			continue;
		}
		string resolved = dt;
		if (cross_system) {
			resolved = ResolveStandardDeltaQualifiedName(con, view_name, dt, "", "");
		}
		update_timestamp_query += "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
		                          " SET last_update = COALESCE("
		                          "(SELECT MAX(" +
		                          string(ivm::TIMESTAMP_COL) + ") + INTERVAL '1 microsecond' FROM " + resolved +
		                          "), now()), last_refresh_ts = now()"
		                          " WHERE view_name = '" +
		                          OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
		                          OpenIVMUtils::EscapeValue(dt) + "';\n";
	}

	// Update DuckLake snapshot IDs so the next refresh only sees new changes.
	// For cross-system refresh (native MV that reads from dl.*), view_catalog_name
	// is the physical-default (no `current_snapshot()` function), so probe
	// `duckdb_databases()` once per refresh to find the attached DuckLake catalog.
	string dl_catalog_name = ResolveDuckLakeCatalogName(con, view_catalog_name, attached_db_catalog_name);
	string dl_snapshot_expr =
	    cross_system ? DUCKLAKE_SNAPSHOT_PLACEHOLDER : "(SELECT id FROM " + dl_catalog_name + ".current_snapshot())";
	string snapshot_update_query;
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			snapshot_update_query += "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
			                         " SET last_snapshot_id = " + dl_snapshot_expr + " WHERE view_name = '" +
			                         OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
			                         OpenIVMUtils::EscapeValue(dt) + "';\n";
		} else if (metadata.IsDeltaTable(view_name, dt)) {
			auto source_meta = con.Query("SELECT source_path FROM " + string(ivm::DELTA_TABLES_TABLE) +
			                             " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
			                             "' AND table_name = '" + OpenIVMUtils::EscapeValue(dt) + "'");
			if (source_meta->HasError() || source_meta->RowCount() == 0 || source_meta->GetValue(0, 0).IsNull()) {
				throw Exception(ExceptionType::CATALOG, "IVM: missing Delta Lake source path for table '" + dt +
				                                            "' in view '" + view_name + "'");
			}
			string delta_path = source_meta->GetValue(0, 0).ToString();
			auto version_result = con.Query(
			    "SELECT COALESCE(MAX(CAST(regexp_extract(file, '([0-9]+)\\.json$', 1) AS BIGINT)), -1) FROM glob('" +
			    OpenIVMUtils::EscapeSingleQuotes(delta_path) + "/_delta_log/*.json')");
			if (version_result->HasError() || version_result->RowCount() == 0 ||
			    version_result->GetValue(0, 0).IsNull()) {
				throw Exception(ExceptionType::CATALOG, "IVM: could not determine Delta Lake version for table '" + dt +
				                                            "' in view '" + view_name + "'");
			}
			snapshot_update_query += "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
			                         " SET last_snapshot_id = " + version_result->GetValue(0, 0).ToString() +
			                         " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
			                         "' AND table_name = '" + OpenIVMUtils::EscapeValue(dt) + "';\n";
		}
	}

	for (auto &dt : delta_table_names) {
		// Externally versioned sources have no OpenIVM delta tables to clean up.
		if (metadata.IsExternallyVersionedTable(view_name, dt)) {
			continue;
		}
		if (cross_system) {
			continue;
		}
		delete_from_delta_table_query += IVMMetadata::BuildDeltaCleanupSQL(dt, dt);
	}

	// Crash safety: set flag before the critical section, clear after last_update is set.
	// If the process crashes between these two points, the next refresh detects the flag
	// and recovers via full recompute (see check at the top of GenerateRefreshSQL).
	string set_in_progress = "UPDATE " + string(ivm::VIEWS_TABLE) +
	                         " SET refresh_in_progress = true WHERE view_name = '" +
	                         OpenIVMUtils::EscapeValue(view_name) + "';\n";
	string clear_in_progress = "UPDATE " + string(ivm::VIEWS_TABLE) +
	                           " SET refresh_in_progress = false WHERE view_name = '" +
	                           OpenIVMUtils::EscapeValue(view_name) + "';\n";

	// Assembly order:
	// 0. set_in_progress: mark refresh as in-flight (crash safety)
	// 1. pre_companion: snapshot old MV state into delta_view (for downstream old→new)
	// 2. ivm_query: compute delta, INSERT INTO delta_view
	// 3. companion_query: (AGGREGATE_GROUP) insert false/zero rows for existing groups
	// 4. upsert_query: apply delta to MV
	// 5. post_companion: replace IVM delta in delta_view with absolute new MV state
	// 6. update_timestamp: mark this refresh complete
	// 7. clear_in_progress: crash safety — flag cleared after timestamp is set
	// 8. delete_from_view: clean old delta_view rows
	// 9. delete_from_delta: clean old base delta rows
	//
	// For cross_system (DuckLake) MVs, steps 0 and 6-7 write to the physical-default catalog
	// (metadata tables) while steps 1-5 and 8-9 write to dl. DuckDB forbids cross-catalog
	// writes in one transaction. When out_pre_meta/out_post_meta are provided, we split them.
	string data_sql = pre_companion + ivm_query + "\n" + companion_query + "\n" + upsert_query + "\n" + post_companion +
	                  compact_delta_view_query + delete_from_view_query + "\n" + delete_from_delta_table_query;
	const string &meta_pre_sql = set_in_progress;
	string meta_post_sql = update_timestamp_query + snapshot_update_query + "\n" + clear_in_progress;

	string clean_query;
	if (cross_system && out_pre_meta != nullptr && out_post_meta != nullptr) {
		*out_pre_meta = meta_pre_sql;
		*out_post_meta = meta_post_sql;
		clean_query = data_sql;
	} else {
		clean_query = meta_pre_sql + data_sql + meta_post_sql;
	}

	// Write reference SQL to disk only if ivm_files_path is explicitly set
	Value files_path_val;
	if (context.TryGetCurrentSetting("ivm_files_path", files_path_val) && !files_path_val.IsNull()) {
		string ivm_file_path = files_path_val.ToString() + "/ivm_upsert_queries_" + view_name + ".sql";
		duckdb::OpenIVMUtils::WriteFile(ivm_file_path, false, clean_query);
	}

	OPENIVM_DEBUG_PRINT("[UPSERT] Generated query:\n%s\n", clean_query.c_str());

	return clean_query;
}

} // namespace duckdb
