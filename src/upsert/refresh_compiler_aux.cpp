#include "upsert/refresh_compiler.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_internal.hpp"

namespace duckdb {

namespace {

static string DeltaSourceRef(const string &source, const string &catalog_prefix) {
	if (!source.empty() && (source[0] == '(' || source.find('.') != string::npos)) {
		return source;
	}
	return catalog_prefix + SqlUtils::QuoteIdentifier(source);
}

static string CreateAuxTablePrefix(const string &target_table, bool replace) {
	return string(replace ? "CREATE OR REPLACE TABLE " : "CREATE TABLE IF NOT EXISTS ") + target_table;
}

static string SourceExprForColumn(const vector<string> &cols, const vector<string> &source_exprs, idx_t column_idx,
                                  const string &fallback_alias = string()) {
	if (column_idx < source_exprs.size() && !source_exprs[column_idx].empty()) {
		return source_exprs[column_idx];
	}
	string col = SqlUtils::QuoteIdentifier(cols[column_idx]);
	if (!fallback_alias.empty()) {
		return fallback_alias + "." + col;
	}
	return cols[column_idx];
}

static void BuildAliasedSourceLists(const vector<string> &cols, const vector<string> &source_exprs, string &select_list,
                                    string &group_list, const string &fallback_alias = string()) {
	select_list.clear();
	group_list.clear();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i > 0) {
			select_list += ", ";
			group_list += ", ";
		}
		string expr = SourceExprForColumn(cols, source_exprs, i, fallback_alias);
		select_list += expr + " AS " + SqlUtils::QuoteIdentifier(cols[i]);
		group_list += expr;
	}
}

} // namespace

string BuildDistinctAuxStateCreateSQL(const string &target_table, const vector<string> &distinct_cols,
                                      const vector<string> &source_exprs, const string &source_relation,
                                      const string &filter_sql, bool replace) {
	string select_list;
	string group_list;
	BuildAliasedSourceLists(distinct_cols, source_exprs, select_list, group_list);
	string filter = filter_sql.empty() ? "" : " WHERE " + filter_sql;
	return CreateAuxTablePrefix(target_table, replace) + " AS SELECT " + select_list +
	       ", count(*)::BIGINT AS _count FROM " + source_relation + filter + " GROUP BY " + group_list;
}

string CompileDistinctIncremental(const string &view_name, const string &aux_table, const vector<string> &distinct_cols,
                                  const vector<string> &source_exprs, const string &delta_source,
                                  const string &last_update, const string &filter_sql,
                                  const vector<string> &group_columns, const string &sum_arg, const string &sum_out,
                                  const string &count_star_col, const string &catalog_prefix) {
	if (distinct_cols.empty() || group_columns.empty() || sum_arg.empty() || sum_out.empty()) {
		throw InternalException("CompileDistinctIncremental called with incomplete metadata for view '%s'", view_name);
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = DeltaSourceRef(delta_source, catalog_prefix);
	string sum_arg_q = SqlUtils::QuoteIdentifier(sum_arg);
	string sum_out_q = SqlUtils::QuoteIdentifier(sum_out);
	string count_q = SqlUtils::QuoteIdentifier(count_star_col);

	string distinct_cols_csv = SqlUtils::JoinQuotedColumns(distinct_cols);
	string distinct_cols_csv_i = SqlUtils::JoinQualifiedQuotedColumns(distinct_cols, "i");
	string group_cols_csv = SqlUtils::JoinQuotedColumns(group_columns);
	string mv_match = SqlUtils::BuildNullSafeMatch(group_columns, "v", "d");
	string source_select;
	string source_group;
	BuildAliasedSourceLists(distinct_cols, source_exprs, source_select, source_group);

	string dinput_table = "openivm_dinput_" + view_name;
	string ts_filter =
	    " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP";
	string filter_clause = filter_sql.empty() ? "" : " AND (" + filter_sql + ")";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dinput_table) + " AS\n  SELECT " +
	       source_select + ", SUM(" + string(openivm::MULTIPLICITY_COL) + ")::BIGINT AS dmult\n  FROM " + delta_q +
	       ts_filter + filter_clause + "\n  GROUP BY " + source_group + "\n  HAVING SUM(" +
	       string(openivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	string aux_match_aliased = SqlUtils::BuildNullSafeMatch(distinct_cols, "_aux", "i");
	string ddist_cte =
	    "WITH ddist AS (\n  SELECT " + distinct_cols_csv_i +
	    ", CASE WHEN COALESCE(_aux._count, 0) = 0 AND i.dmult > 0 THEN 1 "
	    "WHEN COALESCE(_aux._count, 0) > 0 AND COALESCE(_aux._count, 0) + i.dmult <= 0 THEN -1 ELSE 0 END AS dd\n"
	    "  FROM " +
	    SqlUtils::QuoteIdentifier(dinput_table) + " i LEFT JOIN " + aux_q + " _aux ON " + aux_match_aliased +
	    "\n),\ndagg AS (\n  SELECT " + group_cols_csv + ", SUM(" + sum_arg_q +
	    " * dd) AS d_sum, SUM(dd)::BIGINT AS d_count\n  FROM ddist WHERE dd <> 0\n  GROUP BY " + group_cols_csv +
	    "\n)\n";

	string insert_cols = group_cols_csv + ", " + sum_out_q + ", " + count_q;
	string insert_vals = SqlUtils::JoinQualifiedQuotedColumns(group_columns, "d") + ", d.d_sum, d.d_count";

	sql += ddist_cte + "MERGE INTO " + data_table + " v USING dagg d ON " + mv_match +
	       "\nWHEN MATCHED THEN UPDATE SET " + sum_out_q + " = COALESCE(v." + sum_out_q + ", 0) + d.d_sum, " + count_q +
	       " = v." + count_q + " + d.d_count\nWHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (" +
	       insert_vals + ");\n\n";

	sql += "DELETE FROM " + data_table + " WHERE " + count_q + " <= 0;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dinput_table) + " i ON " +
	       aux_match_aliased +
	       "\nWHEN MATCHED THEN UPDATE SET _count = _aux._count + i.dmult\nWHEN NOT MATCHED AND i.dmult > 0 "
	       "THEN INSERT (" +
	       distinct_cols_csv + ", _count) VALUES (" + distinct_cols_csv_i + ", i.dmult);\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE _count <= 0;\n\n";
	sql += "DROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(dinput_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileDistinctIncremental] %zu distinct cols, %zu group cols, sum %s(%s)→%s, aux=%s\n",
	                    distinct_cols.size(), group_columns.size(), "SUM", sum_arg.c_str(), sum_out.c_str(),
	                    aux_table.c_str());
	return sql;
}

string BuildSemiAntiAuxStateCreateSQL(const string &target_table, const string &left_source, const string &left_alias,
                                      const string &right_source, const string &right_alias, const string &predicate,
                                      const string &post_filter, const vector<string> &left_cols,
                                      const vector<string> &left_exprs, bool replace) {
	string left_cols_csv = SqlUtils::JoinQuotedColumns(left_cols);
	string left_cols_qualified = SqlUtils::JoinQualifiedQuotedColumns(left_cols, left_alias);
	string left_cols_lc = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "lc");
	string left_cols_mc = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "mc");
	string lc_mc_match = SqlUtils::BuildNullSafeMatch(left_cols, "lc", "mc");
	string left_source_select;
	string unused_group_list;
	BuildAliasedSourceLists(left_cols, left_exprs, left_source_select, unused_group_list, left_alias);
	string left_source_filter = post_filter.empty() ? "" : " WHERE " + post_filter;
	return CreateAuxTablePrefix(target_table, replace) + " AS WITH left_source AS (SELECT " + left_source_select +
	       " FROM " + left_source + " " + left_alias + left_source_filter + "), left_counts AS (SELECT " +
	       left_cols_csv + ", count(*)::BIGINT AS _left_count FROM left_source GROUP BY " + left_cols_csv +
	       "), match_counts AS (SELECT " + left_cols_qualified +
	       ", count(*)::BIGINT AS _match_count FROM (SELECT DISTINCT " + left_cols_csv + " FROM left_source) " +
	       left_alias + " JOIN " + right_source + " " + right_alias + " ON " + predicate + " GROUP BY " +
	       left_cols_qualified + ") SELECT " + left_cols_lc +
	       ", lc._left_count, coalesce(mc._match_count, 0)::BIGINT AS _match_count FROM left_counts lc LEFT JOIN "
	       "match_counts mc ON " +
	       lc_mc_match;
}

string CompileSemiAntiRecompute(const string &view_name, const string &aux_table, const string &join_type,
                                const string &left_table, const string &left_alias, const string &right_table,
                                const string &right_alias, const string &predicate, const string &post_filter,
                                const vector<string> &left_cols, const vector<string> &left_exprs,
                                const vector<string> &output_cols, const string &left_delta_source,
                                const string &right_delta_source, const string &left_last_update,
                                const string &right_last_update, const string &catalog_prefix) {
	if (left_cols.empty() || output_cols.empty() || aux_table.empty() || right_delta_source.empty() ||
	    right_last_update.empty()) {
		throw InternalException("CompileSemiAntiRecompute called with incomplete metadata for view '%s'", view_name);
	}

	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	bool has_left_delta = !left_delta_source.empty() && !left_last_update.empty();
	string left_delta_q = DeltaSourceRef(left_delta_source, catalog_prefix);
	string right_delta_q = DeltaSourceRef(right_delta_source, catalog_prefix);
	string dleft_table = "openivm_saj_dleft_" + view_name;
	string dright_table = "openivm_saj_dright_" + view_name;
	string old_table = "openivm_saj_old_" + view_name;
	string aff_table = "openivm_saj_aff_" + view_name;
	bool is_anti = StringUtil::Lower(join_type) == "anti";
	string visible = is_anti ? "_match_count = 0" : "_match_count > 0";
	string cur_visible = is_anti ? "_cur._match_count = 0" : "_cur._match_count > 0";
	string left_delta_filter = post_filter.empty() ? "" : " AND (" + post_filter + ")";

	string left_cols_csv = SqlUtils::JoinQuotedColumns(left_cols);
	string output_cols_csv = SqlUtils::JoinQuotedColumns(output_cols);
	string left_cols_i = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "i");
	string left_cols_l = SqlUtils::JoinQualifiedQuotedColumns(left_cols, left_alias);
	string left_cols_old = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "_old");
	string left_cols_cur = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "_cur");
	string output_old = SqlUtils::JoinQualifiedQuotedColumns(output_cols, "_old");
	string output_cur = SqlUtils::JoinQualifiedQuotedColumns(output_cols, "_cur");

	string aux_i_match = SqlUtils::BuildNullSafeMatch(left_cols, "_aux", "i");
	string old_cur_match = SqlUtils::BuildNullSafeMatch(left_cols, "_old", "_cur");
	string aff_old_match = SqlUtils::BuildNullSafeMatch(left_cols, "_aff", "_old");
	string aff_cur_match = SqlUtils::BuildNullSafeMatch(left_cols, "_aff", "_cur");
	string data_match = SqlUtils::BuildNullSafeMatch(output_cols, "_v", "_d");
	string left_delta_select;
	string left_delta_group;
	BuildAliasedSourceLists(left_cols, left_exprs, left_delta_select, left_delta_group, left_alias);

	string left_ts =
	    string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(left_last_update) + "'::TIMESTAMP";
	string right_ts = right_alias + "." + string(openivm::TIMESTAMP_COL) + " >= '" +
	                  SqlUtils::EscapeValue(right_last_update) + "'::TIMESTAMP";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(old_table) + " AS SELECT *, (" + visible +
	       ") AS _visible FROM " + aux_q + ";\n\n";

	if (has_left_delta) {
		sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dleft_table) + " AS\n  SELECT " +
		       left_delta_select + ", SUM(" + left_alias + "." + string(openivm::MULTIPLICITY_COL) +
		       ")::BIGINT AS dmult\n  FROM " + left_delta_q + " " + left_alias + "\n  WHERE " + left_alias + "." +
		       left_ts + left_delta_filter + "\n  GROUP BY " + left_delta_group + "\n  HAVING SUM(" + left_alias + "." +
		       string(openivm::MULTIPLICITY_COL) + ") <> 0;\n\n";
	} else {
		sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dleft_table) + " AS\n  SELECT " +
		       left_cols_csv + ", 0::BIGINT AS dmult FROM " + aux_q + " WHERE false;\n\n";
	}

	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dright_table) + " AS\n  SELECT " + left_cols_l +
	       ", SUM(" + right_alias + "." + string(openivm::MULTIPLICITY_COL) + ")::BIGINT AS dmatch\n  FROM " + aux_q +
	       " " + left_alias + " JOIN " + right_delta_q + " " + right_alias + " ON " + predicate + "\n  WHERE " +
	       right_ts + "\n  GROUP BY " + left_cols_l + "\n  HAVING SUM(" + right_alias + "." +
	       string(openivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dright_table) + " _d ON " +
	       SqlUtils::BuildNullSafeMatch(left_cols, "_aux", "_d") +
	       "\nWHEN MATCHED THEN UPDATE SET _match_count = _aux._match_count + _d.dmatch;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dleft_table) + " i ON " + aux_i_match +
	       "\nWHEN MATCHED THEN UPDATE SET _left_count = _aux._left_count + i.dmult;\n\n";

	sql += "INSERT INTO " + aux_q + " (" + left_cols_csv + ", _left_count, _match_count)\nSELECT " + left_cols_i +
	       ", i.dmult, COALESCE(mc._match_count, 0)::BIGINT\nFROM " + SqlUtils::QuoteIdentifier(dleft_table) +
	       " i\nLEFT JOIN " + aux_q + " _aux ON " + aux_i_match + "\nLEFT JOIN (\n  SELECT " + left_cols_l +
	       ", COUNT(*)::BIGINT AS _match_count\n  FROM " + SqlUtils::QuoteIdentifier(dleft_table) + " " + left_alias +
	       " JOIN " + right_table + " " + right_alias + " ON " + predicate + "\n  GROUP BY " + left_cols_l +
	       "\n) mc ON " + SqlUtils::BuildNullSafeMatch(left_cols, "mc", "i") +
	       "\nWHERE _aux._left_count IS NULL AND i.dmult > 0;\n\n";

	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(aff_table) + " AS\nSELECT " + left_cols_old +
	       " FROM " + SqlUtils::QuoteIdentifier(old_table) + " _old LEFT JOIN " + aux_q + " _cur ON " + old_cur_match +
	       "\nWHERE _cur._left_count IS NULL OR _old._left_count IS DISTINCT FROM _cur._left_count OR "
	       "_old._visible IS DISTINCT FROM (" +
	       cur_visible + ")\nUNION\nSELECT " + left_cols_cur + " FROM " + aux_q + " _cur LEFT JOIN " +
	       SqlUtils::QuoteIdentifier(old_table) + " _old ON " + old_cur_match +
	       "\nWHERE _old._left_count IS NULL OR _old._left_count IS DISTINCT FROM _cur._left_count OR "
	       "_old._visible IS DISTINCT FROM (" +
	       cur_visible + ");\n\n";

	sql += "WITH _old_rows AS (\n  SELECT " + output_old + " FROM " + SqlUtils::QuoteIdentifier(old_table) +
	       " _old JOIN " + SqlUtils::QuoteIdentifier(aff_table) + " _aff ON " + aff_old_match +
	       ", generate_series(1, _old._left_count::BIGINT)\n  WHERE _old._visible AND _old._left_count > 0" +
	       "\n), "
	       "_net AS (\n  SELECT " +
	       output_cols_csv + ", COUNT(*)::BIGINT AS _cnt FROM _old_rows GROUP BY " + output_cols_csv +
	       "\n)\nDELETE FROM " + data_table + " WHERE rowid IN (\n  SELECT _v.rowid FROM (\n    SELECT rowid, " +
	       output_cols_csv + ", ROW_NUMBER() OVER (PARTITION BY " + output_cols_csv + " ORDER BY rowid) AS _rn FROM " +
	       data_table + "\n  ) _v JOIN _net _d ON " + data_match + " WHERE _v._rn <= _d._cnt\n);\n\n";

	sql += "INSERT INTO " + data_table + " SELECT " + output_cur + "\nFROM " + aux_q + " _cur JOIN " +
	       SqlUtils::QuoteIdentifier(aff_table) + " _aff ON " + aff_cur_match +
	       ", generate_series(1, _cur._left_count::BIGINT)\nWHERE " + cur_visible + " AND _cur._left_count > 0;\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE _left_count <= 0;\n";
	sql += "DROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(old_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(dleft_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(dright_table) + ";\nDROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(aff_table) +
	       ";\n";

	OPENIVM_DEBUG_PRINT("[CompileSemiAntiRecompute] %s join, %zu left cols, aux=%s\n", join_type.c_str(),
	                    left_cols.size(), aux_table.c_str());
	return sql;
}

string BuildFilteredGroupCountAuxStateCreateSQL(const string &target_table, const string &source_table,
                                                const string &group_col, const string &sum_col,
                                                const string &source_group_expr, const string &source_sum_expr,
                                                bool replace) {
	string group_q = SqlUtils::QuoteIdentifier(group_col);
	string sum_q = SqlUtils::QuoteIdentifier(sum_col);
	string group_expr = source_group_expr.empty() ? group_q : source_group_expr;
	string sum_expr = source_sum_expr.empty() ? sum_q : source_sum_expr;
	return CreateAuxTablePrefix(target_table, replace) + " AS SELECT " + group_expr + " AS " + group_q + ", sum(" +
	       sum_expr + ") AS openivm_sum FROM " + source_table + " GROUP BY " + group_expr;
}

string CompileFilteredGroupCount(const string &view_name, const string &aux_table, const string &delta_source,
                                 const string &last_update, const string &group_col, const string &sum_col,
                                 const string &source_group_expr, const string &source_sum_expr,
                                 const string &output_col, const string &comparison_op, const string &threshold_sql,
                                 const string &catalog_prefix) {
	if (aux_table.empty() || delta_source.empty() || last_update.empty() || group_col.empty() || sum_col.empty() ||
	    output_col.empty() || comparison_op.empty() || threshold_sql.empty()) {
		throw InternalException("CompileFilteredGroupCount called with incomplete metadata for view '%s'", view_name);
	}

	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = DeltaSourceRef(delta_source, catalog_prefix);
	string dsum_table = "openivm_fgc_delta_" + view_name;
	string group_q = SqlUtils::QuoteIdentifier(group_col);
	string sum_q = SqlUtils::QuoteIdentifier(sum_col);
	string source_group = source_group_expr.empty() ? group_q : source_group_expr;
	string source_sum = source_sum_expr.empty() ? sum_q : source_sum_expr;
	string output_q = SqlUtils::QuoteIdentifier(output_col);
	string dsum_expr = "SUM(" + string(openivm::MULTIPLICITY_COL) + " * " + source_sum + ")";
	string old_sum = "COALESCE(_aux.openivm_sum, 0)";
	string new_sum = "(" + old_sum + " + d.openivm_delta_sum)";
	string old_visible = "CASE WHEN " + old_sum + " " + comparison_op + " " + threshold_sql + " THEN 1 ELSE 0 END";
	string new_visible = "CASE WHEN " + new_sum + " " + comparison_op + " " + threshold_sql + " THEN 1 ELSE 0 END";
	string aux_match = SqlUtils::BuildNullSafeMatch(vector<string> {group_col}, "_aux", "d");

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dsum_table) + " AS\n  SELECT " + source_group +
	       " AS " + group_q + ", " + dsum_expr + " AS openivm_delta_sum\n  FROM " + delta_q + "\n  WHERE " +
	       string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP\n  GROUP BY " +
	       source_group + "\n  HAVING " + dsum_expr + " <> 0;\n\n";

	sql += "WITH openivm_transition AS (\n  SELECT SUM((" + new_visible + ") - (" + old_visible +
	       ")) AS openivm_delta_count\n  FROM " + SqlUtils::QuoteIdentifier(dsum_table) + " d LEFT JOIN " + aux_q +
	       " _aux ON " + aux_match + "\n)\nUPDATE " + data_table + " SET " + output_q + " = COALESCE(" + output_q +
	       ", 0) + COALESCE((SELECT openivm_delta_count FROM openivm_transition), 0);\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dsum_table) + " d ON " + aux_match +
	       "\nWHEN MATCHED THEN UPDATE SET openivm_sum = COALESCE(_aux.openivm_sum, 0) + d.openivm_delta_sum\n"
	       "WHEN NOT MATCHED THEN INSERT (" +
	       group_q + ", openivm_sum) VALUES (d." + group_q + ", d.openivm_delta_sum);\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE openivm_sum = 0;\n";
	sql += "DROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(dsum_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileFilteredGroupCount] group=%s, sum=%s, op=%s, aux=%s\n", group_col.c_str(),
	                    sum_col.c_str(), comparison_op.c_str(), aux_table.c_str());
	return sql;
}

string CompileWindowRecompute(const string &view_name, const string &view_query_sql, const string &delta_ts_filter,
                              const string &catalog_prefix, const vector<string> &partition_columns,
                              const vector<WindowPartitionDeltaSpec> &partition_delta_specs, bool emit_cascade_delta,
                              const string &affected_keys_sql, const string &affected_key_cols,
                              const string &affected_key_tuple) {
	bool have_affected_keys = !affected_keys_sql.empty() && !affected_key_cols.empty() && !affected_key_tuple.empty();
	if (!have_affected_keys && (partition_columns.empty() || partition_delta_specs.empty())) {
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string affected_temp_table = SqlUtils::QuoteIdentifier("openivm_affected_" + view_name);

	string affected_filter;
	if (have_affected_keys) {
		affected_filter =
		    affected_key_tuple + " IN (SELECT " + affected_key_cols + " FROM " + affected_temp_table + ")";
	} else {
		for (size_t i = 0; i < partition_delta_specs.size(); i++) {
			if (i > 0) {
				affected_filter += " OR ";
			}
			const auto &spec = partition_delta_specs[i];
			string output_col = SqlUtils::QuoteIdentifier(spec.output_column);
			string source_col = SqlUtils::QuoteIdentifier(spec.source_column);
			string delta_table =
			    spec.delta_table_sql.empty() ? SqlUtils::QuoteIdentifier(spec.delta_table) : spec.delta_table_sql;
			affected_filter +=
			    output_col + " IN (SELECT DISTINCT " + source_col + " FROM " + delta_table + delta_where + ")";
		}
	}

	OPENIVM_DEBUG_PRINT(
	    "[CompileWindowRecompute] Partition columns: %zu, delta specs: %zu, lineage keys: %s, cascade delta: %s\n",
	    partition_columns.size(), partition_delta_specs.size(), have_affected_keys ? "yes" : "no",
	    emit_cascade_delta ? "enabled" : "disabled");
	if (!emit_cascade_delta) {
		if (!have_affected_keys) {
			return BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_recompute", affected_filter,
			                                   affected_filter);
		}
		string sql;
		sql += "CREATE OR REPLACE TEMP TABLE " + affected_temp_table + " AS\n" + affected_keys_sql + ";\n\n";
		sql += BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_recompute", affected_filter,
		                                   affected_filter);
		sql += "DROP TABLE IF EXISTS " + affected_temp_table + ";\n";
		return sql;
	}

	string delta_table = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string old_temp_table = SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + view_name);
	string new_temp_table = SqlUtils::QuoteIdentifier(string("openivm_new_") + view_name);
	string sql;
	if (have_affected_keys) {
		sql += "CREATE OR REPLACE TEMP TABLE " + affected_temp_table + " AS\n" + affected_keys_sql + ";\n\n";
	}
	sql += "CREATE OR REPLACE TEMP TABLE " + old_temp_table + " AS\nSELECT * FROM " + data_table + "\nWHERE " +
	       affected_filter + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + new_temp_table + " AS\nSELECT * FROM (" + view_query_sql +
	       ") openivm_recompute\nWHERE " + affected_filter + ";\n\n";
	sql += "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n";
	sql += "INSERT INTO " + data_table + "\nSELECT * FROM " + new_temp_table + ";\n";
	sql += "\n" + BuildSignedMultisetDeltaInsertSQL(delta_table, old_temp_table, new_temp_table);
	if (have_affected_keys) {
		sql += "\nDROP TABLE IF EXISTS " + affected_temp_table + ";\n";
	}
	sql += "DROP TABLE IF EXISTS " + old_temp_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + new_temp_table + ";\n";
	return sql;
}

} // namespace duckdb
