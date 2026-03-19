#ifndef OPENIVM_INSERT_RULE_HPP
#define OPENIVM_INSERT_RULE_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class IVMInsertRule : public OptimizerExtension {
public:
	IVMInsertRule();

	struct IVMInsertOptimizerInfo : OptimizerExtensionInfo {
		explicit IVMInsertOptimizerInfo() {
		}
		// Guard against re-entrant optimizer calls from con.Query() inside the insert rule.
		// When active is true, the rule is already processing a DML statement and any
		// nested optimizer invocations (from delta table inserts) should be skipped.
		bool active = false;
	};

	static void IVMInsertRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // OPENIVM_INSERT_RULE_HPP
