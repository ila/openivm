#ifndef REFRESH_INSERT_RULE_HPP
#define REFRESH_INSERT_RULE_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

namespace duckdb {

class IVMInsertRule : public OptimizerExtension {
public:
	IVMInsertRule();

	struct IVMInsertOptimizerInfo : OptimizerExtensionInfo {
		explicit IVMInsertOptimizerInfo() {
		}
	};

	static void IVMInsertRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // REFRESH_INSERT_RULE_HPP
