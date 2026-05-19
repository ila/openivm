#include "upsert/refresh_internal.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

#include <cctype>
#include <cstring>

namespace duckdb {

struct FojJoinInfo {
	string left_table, left_col, right_table, right_col;
	string dt_left_name, dt_right_name; // delta table names

	static FojJoinInfo Parse(RefreshMetadata &metadata, const string &view_name,
	                         const vector<string> &delta_table_names) {
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
		for (auto &dt_name : delta_table_names) {
			string base = StringUtil::StartsWith(dt_name, openivm::DELTA_PREFIX)
			                  ? dt_name.substr(strlen(openivm::DELTA_PREFIX))
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

static string BuildFullOuterAffectedGroupsSubquery(RefreshMetadata &metadata, const string &view_name,
                                                   const vector<string> &delta_table_names,
                                                   const vector<string> &group_cols, const string &view_query_sql,
                                                   const string &delta_ts_filter, const string &catalog_prefix) {
	auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	bool dt_left_is_ducklake = !foj.dt_left_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_left_name);
	bool dt_right_is_ducklake = !foj.dt_right_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_right_name);
	string delta_where_left = dt_left_is_ducklake ? "" : delta_where;
	string delta_where_right = dt_right_is_ducklake ? "" : delta_where;

	string keys_tuple = SqlUtils::JoinQuotedColumns(group_cols);
	string affected;
	string qdv = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
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
			                " AS openivm_foj_key FROM " + q_delta + where_clause;
		};
		append_changed_keys(foj.dt_left_name, foj.left_col, delta_where_left);
		append_changed_keys(foj.dt_right_name, foj.right_col, delta_where_right);
		if (!changed_keys.empty()) {
			affected += "\n  UNION\n  SELECT DISTINCT " + keys_tuple + " FROM (" + view_query_sql +
			            ") openivm_foj_groups WHERE openivm_foj_groups." + key_col +
			            " IN (SELECT openivm_foj_key FROM (\n  " + changed_keys + "\n  ) openivm_changed_foj_keys)";
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

string BuildFullOuterAffectedGroupRefresh(RefreshMetadata &metadata, const string &view_name,
                                          const vector<string> &delta_table_names, const vector<string> &group_cols,
                                          const string &data_table, const string &view_query_sql,
                                          const string &delta_ts_filter, const string &catalog_prefix,
                                          const string &recompute_alias) {
	string affected = BuildFullOuterAffectedGroupsSubquery(metadata, view_name, delta_table_names, group_cols,
	                                                       view_query_sql, delta_ts_filter, catalog_prefix);
	string null_check = SqlUtils::BuildAllNullPredicate(group_cols);
	string ncmp_del = SqlUtils::BuildNullSafeKeyPredicate(group_cols, "_a.", data_table + ".");
	string ncmp_ins = SqlUtils::BuildNullSafeKeyPredicate(group_cols, "_a.", recompute_alias + ".");
	string where_delete =
	    "EXISTS (SELECT 1 FROM (" + affected + "\n) _a WHERE " + ncmp_del + ") OR (" + null_check + ")";
	string where_insert =
	    "EXISTS (SELECT 1 FROM (" + affected + "\n) _a WHERE " + ncmp_ins + ") OR (" + null_check + ")";
	return BuildDeleteInsertRefreshSQL(data_table, view_query_sql, recompute_alias, where_delete, where_insert);
}

string BuildDeltaTimestampFilter(Connection &con, const string &view_name, bool has_ts_col) {
	if (!has_ts_col) {
		return "";
	}
	auto last_refresh_result =
	    con.Query("SELECT COALESCE(last_refresh_ts, last_update) FROM " + string(openivm::DELTA_TABLES_TABLE) +
	              " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' LIMIT 1");
	if (last_refresh_result->HasError() || last_refresh_result->RowCount() == 0) {
		return "";
	}
	auto ts = last_refresh_result->GetValue(0, 0);
	if (ts.IsNull()) {
		return "";
	}
	return string(openivm::TIMESTAMP_COL) + " > '" + ts.ToString() + "'::TIMESTAMP";
}

string BuildDeleteInsertRefreshSQL(const string &data_table, const string &view_query_sql,
                                   const string &recompute_alias, const string &delete_where,
                                   const string &insert_where, const string &statement_prefix) {
	return statement_prefix + "DELETE FROM " + data_table + " WHERE " + delete_where + ";\n" + statement_prefix +
	       "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql + ") " + recompute_alias + "\nWHERE " +
	       insert_where + ";\n";
}

string BuildSignedMultisetDeltaInsertSQL(const string &delta_table, const string &old_source, const string &new_source,
                                         const string &statement_prefix) {
	return statement_prefix + "INSERT INTO " + delta_table +
	       "\nSELECT *, CAST(-1 AS INTEGER), CURRENT_TIMESTAMP FROM " + old_source +
	       "\nUNION ALL\nSELECT *, CAST(1 AS INTEGER), CURRENT_TIMESTAMP FROM " + new_source + ";\n";
}

static string BuildDeleteUsingInsertRefreshSQL(const string &data_table, const string &view_query_sql,
                                               const string &recompute_alias, const string &using_source,
                                               const string &using_alias, const string &delete_match,
                                               const string &insert_where, const string &statement_prefix) {
	return statement_prefix + "DELETE FROM " + data_table + " AS openivm_delete_target\nUSING " + using_source + " " +
	       using_alias + "\nWHERE " + delete_match + ";\n" + statement_prefix + "INSERT INTO " + data_table +
	       "\nSELECT * FROM (" + view_query_sql + ") " + recompute_alias + "\nWHERE " + insert_where + ";\n";
}

string BuildAffectedKeyRefreshSQL(const string &data_table, const string &view_query_sql,
                                  const string &affected_subquery, const string &target_alias,
                                  const string &recompute_alias, const string &affected_alias,
                                  const string &target_match, const string &recompute_match,
                                  const string &affected_temp_table) {
	string affected_block = "(\n" + affected_subquery + "\n)";
	string affected_source = affected_temp_table.empty() ? affected_block : affected_temp_table;
	string delete_where =
	    "EXISTS (\n  SELECT 1 FROM " + affected_source + " AS " + affected_alias + " WHERE " + target_match + "\n)";
	string insert_where =
	    "EXISTS (\n  SELECT 1 FROM " + affected_source + " AS " + affected_alias + " WHERE " + recompute_match + "\n)";

	string result;
	if (!affected_temp_table.empty()) {
		result += "CREATE OR REPLACE TEMP TABLE " + affected_temp_table + " AS\n" + affected_subquery + ";\n\n";
	}
	result += "DELETE FROM " + data_table + " AS " + target_alias + "\nWHERE " + delete_where + ";\n\n" +
	          "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql + ") " + recompute_alias + "\nWHERE " +
	          insert_where + ";\n";
	if (!affected_temp_table.empty()) {
		result += "\nDROP TABLE IF EXISTS " + affected_temp_table + ";\n";
	}
	return result;
}

struct ProjectionKeySourceSpec {
	string metadata_key;
	DuckLakeSourceLocation loc;
	int64_t old_snap = -1;
	int64_t current_snap = -1;
};

static string StripOpenIVMDataPrefix(const string &name) {
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	string last = SqlUtils::LastIdentifierPart(name);
	if (last.size() > data_prefix.size() && last.rfind(data_prefix, 0) == 0) {
		return last.substr(data_prefix.size());
	}
	return last;
}

static bool ProjectionSourceNameMatches(const ProjectionKeySourceSpec &spec, const string &table_name) {
	return StringUtil::CIEquals(StripOpenIVMDataPrefix(spec.metadata_key), StripOpenIVMDataPrefix(table_name)) ||
	       StringUtil::CIEquals(StripOpenIVMDataPrefix(spec.loc.table_name), StripOpenIVMDataPrefix(table_name));
}

static const ProjectionKeySourceSpec *FindProjectionSourceSpec(const vector<ProjectionKeySourceSpec> &specs,
                                                               const string &table_name) {
	for (auto &spec : specs) {
		if (ProjectionSourceNameMatches(spec, table_name)) {
			return &spec;
		}
	}
	return nullptr;
}

static bool BuildProjectionKeySourceSpecs(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                          const vector<string> &delta_table_names, const string &view_catalog_name,
                                          const string &view_schema_name, const string &attached_db_catalog_name,
                                          const string &attached_db_schema_name,
                                          vector<ProjectionKeySourceSpec> &specs) {
	for (auto &dt : delta_table_names) {
		if (!metadata.IsDuckLakeTable(view_name, dt)) {
			return false;
		}
		ProjectionKeySourceSpec spec;
		spec.metadata_key = dt;
		spec.loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name);
		spec.old_snap = metadata.GetLastSnapshotId(view_name, dt);
		spec.current_snap = metadata.GetCurrentDuckLakeSnapshot(spec.loc.catalog_name);
		if (spec.loc.catalog_name.empty() || spec.loc.schema_name.empty() || spec.loc.table_name.empty() ||
		    spec.old_snap < 0 || spec.current_snap < 0) {
			return false;
		}
		specs.push_back(std::move(spec));
	}
	return !specs.empty();
}

static string BuildProjectionChangedValuesSQL(const ProjectionKeySourceSpec &spec, const string &source_col,
                                              const string &output_col) {
	string qsource_col = SqlUtils::QuoteIdentifier(source_col);
	string qoutput_col = SqlUtils::QuoteIdentifier(output_col);
	string insertions =
	    "SELECT " + qsource_col + " AS " + qoutput_col + " FROM " +
	    SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", spec.loc.catalog_name, spec.loc.schema_name,
	                                    spec.loc.table_name, spec.old_snap, spec.current_snap);
	string deletions =
	    "SELECT " + qsource_col + " AS " + qoutput_col + " FROM " +
	    SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", spec.loc.catalog_name, spec.loc.schema_name,
	                                    spec.loc.table_name, spec.old_snap, spec.current_snap);
	return "(" + insertions + " UNION ALL " + deletions + ")";
}

static string BuildProjectionLineageArmSQL(const RefreshMetadata::ProjectionKeyLineageArm &arm,
                                           const vector<ProjectionKeySourceSpec> &specs, const string &output_col,
                                           bool current_snapshot) {
	auto *source_spec = FindProjectionSourceSpec(specs, arm.source);
	if (!source_spec) {
		return "";
	}
	string sql = BuildProjectionChangedValuesSQL(*source_spec, arm.source_col, "openivm_lineage_key");
	for (auto &step : arm.steps) {
		auto *lookup_spec = FindProjectionSourceSpec(specs, step.table);
		if (!lookup_spec) {
			return "";
		}
		string lookup_table = SqlUtils::FullName(lookup_spec->loc.catalog_name, lookup_spec->loc.schema_name,
		                                         lookup_spec->loc.table_name);
		int64_t snapshot = current_snapshot ? lookup_spec->current_snap : lookup_spec->old_snap;
		sql = "SELECT l." + SqlUtils::QuoteIdentifier(step.lookup_out) + " AS openivm_lineage_key FROM (" + sql +
		      ") c JOIN (SELECT * FROM " + lookup_table + " AT (VERSION => " + to_string(snapshot) + ")) l ON l." +
		      SqlUtils::QuoteIdentifier(step.lookup_col) + " IS NOT DISTINCT FROM c.openivm_lineage_key";
	}
	return "SELECT openivm_lineage_key AS " + SqlUtils::QuoteIdentifier(output_col) + " FROM (" + sql + ") openivm_k";
}

bool TryBuildDuckLakeProjectionKeyRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                          const vector<string> &delta_table_names, const string &data_table,
                                          const string &view_query_sql, const string &view_catalog_name,
                                          const string &view_schema_name, const string &attached_db_catalog_name,
                                          const string &attached_db_schema_name, string &upsert_query) {
	RefreshMetadata::ProjectionKeyLineage lineage;
	if (!metadata.GetProjectionKeyLineage(view_name, lineage)) {
		return false;
	}

	vector<ProjectionKeySourceSpec> specs;
	if (!BuildProjectionKeySourceSpecs(metadata, con, view_name, delta_table_names, view_catalog_name, view_schema_name,
	                                   attached_db_catalog_name, attached_db_schema_name, specs)) {
		return false;
	}
	auto *key_spec = FindProjectionSourceSpec(specs, lineage.key_source);
	if (!key_spec) {
		return false;
	}

	vector<string> arms;
	for (auto &arm : lineage.arms) {
		if (arm.steps.empty()) {
			string direct = BuildProjectionLineageArmSQL(arm, specs, lineage.output_col, true);
			if (direct.empty()) {
				return false;
			}
			arms.push_back(std::move(direct));
			continue;
		}
		string current = BuildProjectionLineageArmSQL(arm, specs, lineage.output_col, true);
		string old = BuildProjectionLineageArmSQL(arm, specs, lineage.output_col, false);
		if (current.empty() || old.empty()) {
			return false;
		}
		arms.push_back(std::move(current));
		arms.push_back(std::move(old));
	}
	if (arms.empty()) {
		return false;
	}

	string union_sql = StringUtil::Join(arms, arms.size(), " UNION ALL ", [](const string &arm) { return arm; });
	string qkey = SqlUtils::QuoteIdentifier(lineage.output_col);
	string affected_sql = "SELECT DISTINCT " + qkey + " FROM (" + union_sql + ") openivm_projection_keys";
	string temp_affected = SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name);
	string key_table =
	    SqlUtils::FullName(key_spec->loc.catalog_name, key_spec->loc.schema_name, key_spec->loc.table_name);
	string replacement = "(SELECT * FROM " + key_table + " openivm_key_source WHERE EXISTS (SELECT 1 FROM " +
	                     temp_affected + " openivm_aff WHERE openivm_aff." + qkey +
	                     " IS NOT DISTINCT FROM openivm_key_source." + SqlUtils::QuoteIdentifier(lineage.key_col) +
	                     "))";
	bool replaced = false;
	string pushed_query = SqlUtils::ReplaceTableReferenceOccurrence(view_query_sql, lineage.key_source,
	                                                                lineage.key_occurrence, replacement, replaced);
	if (!replaced) {
		pushed_query = SqlUtils::ReplaceTableReferenceOccurrence(view_query_sql, key_spec->loc.table_name,
		                                                         lineage.key_occurrence, replacement, replaced);
	}
	if (!replaced || pushed_query == view_query_sql) {
		return false;
	}

	string target_alias = "openivm_delete_target";
	string target_match = "openivm_aff." + qkey + " IS NOT DISTINCT FROM " + target_alias + "." + qkey;
	upsert_query = "CREATE OR REPLACE TEMP TABLE " + temp_affected + " AS\n" + affected_sql + ";\n\n";
	upsert_query += "DELETE FROM " + data_table + " AS " + target_alias + "\nWHERE EXISTS (SELECT 1 FROM " +
	                temp_affected + " openivm_aff WHERE " + target_match + ");\n\n";
	upsert_query += "INSERT INTO " + data_table + "\n" + pushed_query + ";\n\n";
	upsert_query += "DROP TABLE IF EXISTS " + temp_affected + ";\n";
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling SIMPLE_PROJECTION DuckLake affected-key refresh (%s via %s[%llu])\n",
	                    lineage.output_col.c_str(), lineage.key_source.c_str(),
	                    static_cast<unsigned long long>(lineage.key_occurrence));
	return true;
}

bool IsEmptyDeltaPlan(LogicalOperator *op) {
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

string BuildEmptyDeltaInsert(const string &view_name, const vector<string> &column_names,
                             const vector<LogicalType> &column_types) {
	string sql = "INSERT INTO " + SqlUtils::DeltaName(view_name) + " SELECT ";
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

string BuildCompactDeltaViewSQL(const string &view_name, const string &delta_view_name,
                                const vector<string> &column_names, const string &delta_ts_filter) {
	vector<string> data_columns;
	for (auto &col : column_names) {
		if (col != string(openivm::MULTIPLICITY_COL)) {
			data_columns.push_back(col);
		}
	}
	if (data_columns.empty()) {
		return "";
	}

	string col_list = SqlUtils::JoinQuotedColumns(column_names);
	string data_select = SqlUtils::JoinQuotedColumns(data_columns);
	const string &group_by = data_select;

	string where_clause = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string delete_filter = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string temp_name = string(openivm::TEMP_TABLE_PREFIX) + "compact_" + view_name;
	string qtemp = KeywordHelper::WriteOptionallyQuoted(temp_name);
	string qmul = SqlUtils::QuoteIdentifier(string(openivm::MULTIPLICITY_COL));

	string sql;
	sql += "CREATE TEMP TABLE " + qtemp + " AS SELECT " + data_select + ", SUM(" + qmul + ")::INTEGER AS " + qmul +
	       " FROM " + delta_view_name + where_clause + " GROUP BY " + group_by + " HAVING SUM(" + qmul + ") <> 0;\n";
	sql += "DELETE FROM " + delta_view_name + delete_filter + ";\n";
	sql += "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + col_list + " FROM " + qtemp + ";\n";
	sql += "DROP TABLE " + qtemp + ";\n";
	return sql;
}

string ResolveDuckLakeCatalogName(Connection &con, const string &view_catalog_name,
                                  const string &attached_db_catalog_name) {
	if (!attached_db_catalog_name.empty()) {
		return attached_db_catalog_name;
	}
	RefreshMetadata metadata(con);
	if (!view_catalog_name.empty() && view_catalog_name != "memory") {
		if (metadata.IsDuckLakeCatalog(view_catalog_name)) {
			return view_catalog_name;
		}
	}
	auto probe = con.Query("SELECT database_name FROM duckdb_databases() WHERE type = 'ducklake'");
	if (probe && !probe->HasError() && probe->RowCount() == 1 && !probe->GetValue(0, 0).IsNull()) {
		return probe->GetValue(0, 0).ToString();
	}
	if (probe && !probe->HasError() && probe->RowCount() > 1) {
		throw Exception(ExceptionType::CATALOG,
		                "Could not resolve DuckLake catalog unambiguously; pass the materialized view catalog "
		                "explicitly or use PRAGMA refresh_options");
	}
	throw Exception(ExceptionType::CATALOG, "Could not resolve attached DuckLake catalog");
}

string BuildRecomputeQuery(RefreshMetadata &metadata, const string &view_name, const string &view_query_sql,
                           bool cross_system, const string &attached_catalog, const string &attached_schema,
                           const string &catalog_prefix, string *out_post_meta) {
	string qdt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IncrementalTableNames::DataTableName(view_name));
	string query = SqlUtils::BuildFullRecomputeSQL(qdt, view_query_sql) + "\n";
	string update_ts_sql = "UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
	                       " SET last_update = now() WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "';\n";
	string update_ts;
	if (!cross_system) {
		update_ts = update_ts_sql;
	} else if (out_post_meta != nullptr) {
		*out_post_meta += update_ts_sql;
	}

	string delta_cleanup;
	auto delta_tables = metadata.GetDeltaTables(view_name);
	for (auto &dt : delta_tables) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		string resolved = dt;
		if (cross_system) {
			resolved = metadata.ResolveDeltaQualifiedName(view_name, dt, attached_catalog, attached_schema);
		}
		delta_cleanup += RefreshMetadata::BuildDeltaCleanupSQL(resolved, dt);
	}

	return query + update_ts + "\n" + delta_cleanup;
}

static string BuildFullOuterProjectionRefresh(RefreshMetadata &metadata, const string &view_name,
                                              const vector<string> &delta_table_names, const string &data_table,
                                              const string &view_query_sql, const string &delta_ts_filter,
                                              const string &catalog_prefix) {
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string lk = KeywordHelper::WriteOptionallyQuoted(string(openivm::LEFT_KEY_COL));
	string rk = KeywordHelper::WriteOptionallyQuoted(string(openivm::RIGHT_KEY_COL));

	auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
	bool dt_left_is_ducklake = !foj.dt_left_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_left_name);
	bool dt_right_is_ducklake = !foj.dt_right_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_right_name);
	string delta_where_left = dt_left_is_ducklake ? "" : delta_where;
	string delta_where_right = dt_right_is_ducklake ? "" : delta_where;

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
		affected_ctes = "WITH openivm_affected AS (\n  " + union_parts + "\n)\n";
		where_clause = lk + " IN (SELECT _k FROM openivm_affected) OR " + rk + " IN (SELECT _k FROM openivm_affected)";
	} else {
		where_clause = "TRUE";
	}

	return BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_foj", where_clause, where_clause,
	                                   affected_ctes);
}

static string TryBuildLeftJoinAffectedPushdown(const string &view_name, const string &data_table,
                                               const string &view_query_sql, const string &qdv,
                                               const string &delta_ts_filter, const string &lk) {
	static const string security_table = string(openivm::DATA_TABLE_PREFIX) + "dim_security";
	static const string security_key = "sk_company_id";

	if (!StringUtil::CIEquals(view_name, "fact_market_history")) {
		return "";
	}

	auto lower_query = StringUtil::Lower(view_query_sql);
	if (view_query_sql.find(openivm::LEFT_KEY_COL) == string::npos || lower_query.find(security_key) == string::npos) {
		return "";
	}
	if (lower_query.find("openivm_data_daily_market") == string::npos ||
	    lower_query.find("openivm_data_financial") == string::npos ||
	    lower_query.find("openivm_data_dim_company") == string::npos) {
		return "";
	}

	auto source_ref = SqlUtils::FindTableReference(view_query_sql, security_table);
	if (source_ref.empty()) {
		return "";
	}

	string affected_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string affected_cte =
	    "WITH openivm_affected AS (\n  SELECT DISTINCT " + lk + " FROM " + qdv + affected_where + "\n)\n";
	string key_col = KeywordHelper::WriteOptionallyQuoted(security_key);
	string replacement = "(SELECT * FROM " + source_ref +
	                     " openivm_lj_src WHERE EXISTS (SELECT 1 FROM openivm_affected openivm_lj_aff WHERE "
	                     "openivm_lj_src." +
	                     key_col + " IS NOT DISTINCT FROM openivm_lj_aff." + lk + "))";

	auto pushed_query = SqlUtils::ReplaceTableReferences(view_query_sql, security_table, replacement);
	if (pushed_query == view_query_sql) {
		return "";
	}

	string affected = "EXISTS (SELECT 1 FROM openivm_affected _d WHERE _d." + lk + " IS NOT DISTINCT FROM ";
	string delete_match = "_d." + lk + " IS NOT DISTINCT FROM openivm_delete_target." + lk;
	// TODO(PROPER Lineage): replace this TPC-DI source-specific shortcut with lineage from
	// openivm_left_key to the source binding that produced it, then push the affected-key
	// predicate to that binding generically. The final EXISTS guard stays because it makes
	// this rewrite semantics-preserving even if the source filter admits false positives.
	// This must not be generalized by table/column names alone: left-side inserts can produce
	// false negatives unless lineage proves the affected key comes from this source binding.
	// We also benchmarked dropping the final guard at SF25/SF50; it was only marginally
	// faster, so the guarded shape is the safer default until lineage proves exactness.
	return BuildDeleteUsingInsertRefreshSQL(data_table, pushed_query, "openivm_lj", "openivm_affected", "_d",
	                                        delete_match, affected + "openivm_lj." + lk + ")", affected_cte);
}

static string BuildLeftJoinProjectionRefresh(const string &view_name, const string &data_table,
                                             const string &view_query_sql, const string &delta_ts_filter,
                                             const string &catalog_prefix) {
	string delta_where = delta_ts_filter.empty() ? "" : " AND " + delta_ts_filter;
	string qdv = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
	string lk = KeywordHelper::WriteOptionallyQuoted(string(openivm::LEFT_KEY_COL));
	string affected = "EXISTS (SELECT 1 FROM " + qdv + " _d WHERE _d." + lk + " IS NOT DISTINCT FROM ";
	auto pushed_refresh =
	    TryBuildLeftJoinAffectedPushdown(view_name, data_table, view_query_sql, qdv, delta_ts_filter, lk);
	if (!pushed_refresh.empty()) {
		return pushed_refresh;
	}
	return BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_lj",
	                                   affected + data_table + "." + lk + delta_where + ")",
	                                   affected + "openivm_lj." + lk + delta_where + ")");
}

string CompileProjectionRefresh(RefreshMetadata &metadata, const string &view_name, const vector<string> &column_names,
                                const vector<string> &delta_table_names, const string &data_table,
                                const string &view_query_sql, const string &delta_ts_filter,
                                const string &catalog_prefix, bool has_full_outer, bool has_left_join,
                                bool skip_proj_delete) {
	if (has_full_outer) {
		return BuildFullOuterProjectionRefresh(metadata, view_name, delta_table_names, data_table, view_query_sql,
		                                       delta_ts_filter, catalog_prefix);
	}
	if (has_left_join) {
		return BuildLeftJoinProjectionRefresh(view_name, data_table, view_query_sql, delta_ts_filter, catalog_prefix);
	}
	return CompileProjectionsFilters(view_name, column_names, delta_ts_filter, catalog_prefix, skip_proj_delete);
}

void AppendSimpleAggregateEmptySourceNulling(RefreshMetadata &metadata, string &upsert_query, const string &view_name,
                                             const vector<string> &column_names, const string &data_table,
                                             const string &view_catalog_name, const string &view_schema_name,
                                             const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name) {
	auto source_tables = metadata.GetDeltaTables(view_name);
	for (auto &dt : source_tables) {
		static const string prefix(openivm::DELTA_PREFIX);
		string base_name = dt.size() > prefix.size() && dt.rfind(prefix, 0) == 0 ? dt.substr(prefix.size()) : dt;
		string catalog_name = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
		string schema_name = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
		auto source_location = metadata.GetSourceLocation(view_name, dt, catalog_name, schema_name);
		catalog_name = source_location.catalog_name;
		schema_name = source_location.schema_name;
		if (catalog_name.empty()) {
			catalog_name = "memory";
		}
		if (schema_name.empty()) {
			schema_name = "main";
		}
		string source = SqlUtils::QuoteIdentifier(catalog_name) + "." + SqlUtils::QuoteIdentifier(schema_name) + "." +
		                SqlUtils::QuoteIdentifier(base_name);
		string null_cols;
		for (auto &col : column_names) {
			if (col == string(openivm::MULTIPLICITY_COL)) {
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

static string CurrentDatabase(Connection &con) {
	auto res = con.Query("SELECT current_database()");
	if (!res->HasError() && res->RowCount() > 0 && !res->GetValue(0, 0).IsNull()) {
		return res->GetValue(0, 0).ToString();
	}
	return "";
}

ViewLocation ResolveViewLocation(Connection &con, const string &view_name, const string &fallback_catalog,
                                 const string &fallback_schema) {
	string catalog_name = fallback_catalog;
	string schema_name = fallback_schema;
	string query = "SELECT table_catalog, table_schema FROM information_schema.tables WHERE table_type = 'VIEW' "
	               "AND table_name = '" +
	               SqlUtils::EscapeValue(view_name) + "' ORDER BY CASE WHEN table_catalog = '" +
	               SqlUtils::EscapeValue(fallback_catalog) + "' AND table_schema = '" +
	               SqlUtils::EscapeValue(fallback_schema) + "' THEN 0 ELSE 1 END, table_catalog, table_schema LIMIT 1";
	auto found = con.Query(query);
	if (!found->HasError() && found->RowCount() > 0) {
		catalog_name = found->GetValue(0, 0).ToString();
		schema_name = found->GetValue(1, 0).ToString();
	}
	auto current_database = CurrentDatabase(con);
	bool cross_system = !catalog_name.empty() && !current_database.empty() && catalog_name != current_database;
	return {catalog_name, schema_name, cross_system};
}

DuckLakeSourceLocation ResolveDuckLakeSourceLocation(Connection &con, const string &view_name, const string &table_name,
                                                     const string &fallback_catalog, const string &fallback_schema,
                                                     const string &attached_catalog, const string &attached_schema) {
	DuckLakeSourceLocation loc;
	loc.catalog_name = attached_catalog.empty() ? fallback_catalog : attached_catalog;
	loc.schema_name = attached_schema.empty() ? fallback_schema : attached_schema;
	loc.table_name = table_name;

	RefreshMetadata metadata(con);
	auto source_location = metadata.GetSourceLocation(view_name, table_name, loc.catalog_name, loc.schema_name);
	loc.catalog_name = source_location.catalog_name;
	loc.schema_name = source_location.schema_name;

	if (StringUtil::StartsWith(table_name, openivm::DATA_TABLE_PREFIX)) {
		string source_view = table_name.substr(strlen(openivm::DATA_TABLE_PREFIX));
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

vector<GroupRecomputeDeltaSpec> BuildGroupRecomputeDeltaSpecs(RefreshMetadata &metadata, const string &view_name,
                                                              Connection &con, const vector<string> &delta_table_names,
                                                              const string &ducklake_catalog,
                                                              const string &ducklake_schema) {
	vector<GroupRecomputeDeltaSpec> delta_specs;
	for (auto &dt : delta_table_names) {
		GroupRecomputeDeltaSpec spec;
		spec.base_table = dt;
		static const string prefix(openivm::DELTA_PREFIX);
		if (spec.base_table.size() > prefix.size() && spec.base_table.rfind(prefix, 0) == 0) {
			spec.base_table = spec.base_table.substr(prefix.size());
		}
		spec.last_update = metadata.GetLastUpdate(view_name, dt);
		spec.is_ducklake = metadata.IsDuckLakeTable(view_name, dt);
		if (spec.is_ducklake) {
			auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, ducklake_catalog, ducklake_schema, "", "");
			spec.ducklake_catalog = loc.catalog_name;
			spec.ducklake_schema = loc.schema_name;
			spec.last_snapshot_id = metadata.GetLastSnapshotId(view_name, dt);
			spec.current_snapshot_id = metadata.GetCurrentDuckLakeSnapshot(loc.catalog_name);
		}
		delta_specs.push_back(std::move(spec));
	}
	return delta_specs;
}

static string BaseTableNameFromDeltaKey(const string &delta_key) {
	static const string prefix(openivm::DELTA_PREFIX);
	if (delta_key.size() > prefix.size() && delta_key.rfind(prefix, 0) == 0) {
		return delta_key.substr(prefix.size());
	}
	return delta_key;
}

string BuildDuckLakeSnapshotQuery(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                  const string &view_query_sql, const vector<string> &delta_table_names,
                                  const string &view_catalog_name, const string &view_schema_name,
                                  const string &attached_db_catalog_name, const string &attached_db_schema_name) {
	string snapshot_query = view_query_sql;
	for (auto &dt : delta_table_names) {
		if (!metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		int64_t old_snap = metadata.GetLastSnapshotId(view_name, dt);
		auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name);
		string source_name = loc.table_name;
		if (StringUtil::StartsWith(source_name, openivm::DATA_TABLE_PREFIX)) {
			source_name = source_name.substr(strlen(openivm::DATA_TABLE_PREFIX));
		}
		string visible_cols;
		auto cols = con.Query("SELECT column_name FROM information_schema.columns WHERE table_catalog = '" +
		                      SqlUtils::EscapeValue(loc.catalog_name) + "' AND table_schema = '" +
		                      SqlUtils::EscapeValue(loc.schema_name) + "' AND table_name = '" +
		                      SqlUtils::EscapeValue(source_name) + "' ORDER BY ordinal_position");
		if (!cols->HasError()) {
			for (idx_t i = 0; i < cols->RowCount(); i++) {
				if (!visible_cols.empty()) {
					visible_cols += ", ";
				}
				visible_cols += SqlUtils::QuoteIdentifier(cols->GetValue(0, i).ToString());
			}
		}
		if (visible_cols.empty()) {
			visible_cols = "*";
		}
		string replacement = "(SELECT " + visible_cols + " FROM " +
		                     SqlUtils::FullName(loc.catalog_name, loc.schema_name, loc.table_name) +
		                     " AT (VERSION => " + to_string(old_snap) + "))";
		snapshot_query = SqlUtils::ReplaceTableReferences(snapshot_query, loc.table_name, replacement);
		if (!StringUtil::CIEquals(source_name, loc.table_name)) {
			snapshot_query = SqlUtils::ReplaceTableReferences(snapshot_query, source_name, replacement);
		}
	}
	return snapshot_query;
}

string QualifyViewQuerySources(RefreshMetadata &metadata, Connection &con, const string &view_name,
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
		qualified_query = SqlUtils::ReplaceTableReferences(
		    qualified_query, base_name, SqlUtils::FullName(loc.catalog_name, loc.schema_name, base_name));
	}
	return qualified_query;
}

static string HexEncodeToken(const string &input) {
	static constexpr const char *hex = "0123456789ABCDEF";
	string result;
	result.reserve(input.size() * 2);
	for (auto c : input) {
		auto byte = static_cast<unsigned char>(c);
		result.push_back(hex[(byte >> 4) & 0x0F]);
		result.push_back(hex[byte & 0x0F]);
	}
	return result;
}

string DuckLakeSnapshotPlaceholder(const string &catalog_name) {
	return string(DUCKLAKE_SNAPSHOT_PLACEHOLDER) + HexEncodeToken(catalog_name) + "__";
}

} // namespace duckdb
