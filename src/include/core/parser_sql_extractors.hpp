#ifndef OPENIVM_PARSER_SQL_EXTRACTORS_HPP
#define OPENIVM_PARSER_SQL_EXTRACTORS_HPP

#include "duckdb.hpp"

namespace duckdb {

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

struct FilteredGroupCountExtract {
	string source;
	string group_col;
	string sum_col;
	string sum_alias;
	string output_col;
	string comparison_op;
	string threshold_sql;
};

bool ExtractInnerDistinct(const string &original_sql, vector<string> &out_cols, string &out_input_sql,
                          string &out_source, string &out_filter_sql);
bool ExtractFilteredGroupCount(const string &original_sql, const vector<string> &output_names,
                               FilteredGroupCountExtract &out);
bool ExtractSemiAntiQuery(const string &original_sql, SemiAntiExtract &out);

} // namespace duckdb

#endif // OPENIVM_PARSER_SQL_EXTRACTORS_HPP
