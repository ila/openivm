#ifndef OPENIVM_PARSER_SQL_EXTRACTORS_HPP
#define OPENIVM_PARSER_SQL_EXTRACTORS_HPP

#include "duckdb.hpp"
#include "core/refresh_metadata.hpp"

namespace duckdb {

struct SemiAntiExtract {
	// The caller adds the aux table and its columns.
	RefreshMetadata::SemiAntiAuxMeta meta;
	// The left side is a derived table of the membership query rather than a relation.
	bool left_is_subquery = false;
	string left_key_col;
	string left_key_expr;
	string right_key_expr;
	string null_aware_left_expr;
	// Parallel to meta.output_cols; a column reference's name parts, otherwise empty.
	vector<string> output_exprs;
	vector<vector<string>> output_refs;
};

struct FilteredGroupCountExtract {
	string source;
	string group_col;
	string sum_col;
	string sum_alias;
	string output_col;
	string comparison_op;
	string threshold_sql;
	string filter;
};

struct CountDistinctExtract {
	string source;
	vector<string> group_exprs;
	string distinct_expr;
	string distinct_col;
	string output_col;
	string filter;
};

bool ExtractInnerDistinct(const string &original_sql, vector<string> &out_cols, string &out_input_sql,
                          string &out_source, string &out_filter_sql);
bool ExtractCountDistinctAggregate(const string &original_sql, const vector<string> &group_columns,
                                   const vector<string> &output_names, CountDistinctExtract &out);
bool ExtractFilteredGroupCount(const string &original_sql, const vector<string> &output_names,
                               FilteredGroupCountExtract &out);
bool ExtractSemiAntiQuery(const string &original_sql, SemiAntiExtract &out);

} // namespace duckdb

#endif // OPENIVM_PARSER_SQL_EXTRACTORS_HPP
