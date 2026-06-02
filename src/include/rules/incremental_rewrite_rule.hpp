#ifndef INCREMENTAL_REWRITE_RULE_HPP
#define INCREMENTAL_REWRITE_RULE_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

class IncrementalRewriteRule : public OptimizerExtension {
public:
	IncrementalRewriteRule() {
		optimize_function = IncrementalRewriteRuleFunction;
	}

	static void AddInsertNode(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
	                          string &view_name, string &view_catalog_name, string &view_schema_name);

	static void IncrementalRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // INCREMENTAL_REWRITE_RULE_HPP
