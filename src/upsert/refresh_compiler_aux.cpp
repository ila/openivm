#include "upsert/refresh_compiler.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_internal.hpp"

namespace duckdb {

namespace {

static string DeltaSourceRef(const string &source, const string &catalog_prefix) {
	if (!source.empty() && source[0] == '(') {
		return source;
	}
	return catalog_prefix + SqlUtils::QuoteIdentifier(source);
}

} // namespace

string CompileDistinctIncremental(const string &view_name, const string &aux_table, const vector<string> &distinct_cols,
                                  const string &delta_source, const string &last_update, const string &filter_sql,
                                  const vector<string> &group_columns, const string &sum_arg, const string &sum_out,
                                  const string &count_star_col, const string &catalog_prefix) {
	if (distinct_cols.empty() || group_columns.empty() || sum_arg.empty() || sum_out.empty()) {
		throw InternalException("CompileDistinctIncremental called with incomplete metadata for view '%s'", view_name);
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = catalog_prefix + SqlUtils::QuoteIdentifier(delta_source);
	string sum_arg_q = SqlUtils::QuoteIdentifier(sum_arg);
	string sum_out_q = SqlUtils::QuoteIdentifier(sum_out);
	string count_q = SqlUtils::QuoteIdentifier(count_star_col);

	string distinct_cols_csv = SqlUtils::JoinQuotedColumns(distinct_cols);
	string distinct_cols_csv_i = SqlUtils::JoinQualifiedQuotedColumns(distinct_cols, "i");
	string group_cols_csv = SqlUtils::JoinQuotedColumns(group_columns);
	string mv_match = SqlUtils::BuildNullSafeMatch(group_columns, "v", "d");

	string dinput_table = "openivm_dinput_" + view_name;
	string ts_filter =
	    " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP";
	string filter_clause = filter_sql.empty() ? "" : " AND (" + filter_sql + ")";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dinput_table) + " AS\n  SELECT " +
	       distinct_cols_csv + ", SUM(" + string(openivm::MULTIPLICITY_COL) + ")::BIGINT AS dmult\n  FROM " + delta_q +
	       ts_filter + filter_clause + "\n  GROUP BY " + distinct_cols_csv + "\n  HAVING SUM(" +
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
	for (size_t i = 0; i < left_cols.size(); i++) {
		if (i > 0) {
			left_delta_select += ", ";
			left_delta_group += ", ";
		}
		string expr = i < left_exprs.size() && !left_exprs[i].empty()
		                  ? left_exprs[i]
		                  : left_alias + "." + SqlUtils::QuoteIdentifier(left_cols[i]);
		left_delta_select += expr + " AS " + SqlUtils::QuoteIdentifier(left_cols[i]);
		left_delta_group += expr;
	}

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

string CompileFilteredGroupCount(const string &view_name, const string &aux_table, const string &delta_source,
                                 const string &last_update, const string &group_col, const string &sum_col,
                                 const string &output_col, const string &comparison_op, const string &threshold_sql,
                                 const string &catalog_prefix) {
	if (aux_table.empty() || delta_source.empty() || last_update.empty() || group_col.empty() || sum_col.empty() ||
	    output_col.empty() || comparison_op.empty() || threshold_sql.empty()) {
		throw InternalException("CompileFilteredGroupCount called with incomplete metadata for view '%s'", view_name);
	}

	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = catalog_prefix + SqlUtils::QuoteIdentifier(delta_source);
	string dsum_table = "openivm_fgc_delta_" + view_name;
	string group_q = SqlUtils::QuoteIdentifier(group_col);
	string sum_q = SqlUtils::QuoteIdentifier(sum_col);
	string output_q = SqlUtils::QuoteIdentifier(output_col);
	string dsum_expr = "SUM(" + string(openivm::MULTIPLICITY_COL) + " * " + sum_q + ")";
	string old_sum = "COALESCE(_aux.openivm_sum, 0)";
	string new_sum = "(" + old_sum + " + d.openivm_delta_sum)";
	string old_visible = "CASE WHEN " + old_sum + " " + comparison_op + " " + threshold_sql + " THEN 1 ELSE 0 END";
	string new_visible = "CASE WHEN " + new_sum + " " + comparison_op + " " + threshold_sql + " THEN 1 ELSE 0 END";
	string aux_match = SqlUtils::BuildNullSafeMatch(vector<string> {group_col}, "_aux", "d");

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dsum_table) + " AS\n  SELECT " + group_q + ", " +
	       dsum_expr + " AS openivm_delta_sum\n  FROM " + delta_q + "\n  WHERE " + string(openivm::TIMESTAMP_COL) +
	       " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP\n  GROUP BY " + group_q + "\n  HAVING " +
	       dsum_expr + " <> 0;\n\n";

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
                              const vector<WindowPartitionDeltaSpec> &partition_delta_specs) {
	if (partition_columns.empty() || partition_delta_specs.empty()) {
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	string affected_filter;
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

	OPENIVM_DEBUG_PRINT("[CompileWindowRecompute] Partition columns: %zu\n", partition_columns.size());
	return BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_recompute", affected_filter,
	                                   affected_filter);
}

} // namespace duckdb
