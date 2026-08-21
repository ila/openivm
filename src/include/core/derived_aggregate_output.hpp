#ifndef DERIVED_AGGREGATE_OUTPUT_HPP
#define DERIVED_AGGREGATE_OUTPUT_HPP

#include "duckdb.hpp"

namespace duckdb {

struct DerivedAggregateOutput {
	string output_column;
	string expression_sql;
};

struct DerivedAggregateOutputInfo {
	bool complete = false;
	vector<DerivedAggregateOutput> outputs;
};

} // namespace duckdb

#endif // DERIVED_AGGREGATE_OUTPUT_HPP
