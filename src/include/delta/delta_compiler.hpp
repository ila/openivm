#ifndef OPENIVM_DELTA_COMPILER_HPP
#define OPENIVM_DELTA_COMPILER_HPP

#include "core/ivm_view_classifier.hpp"
#include "delta/delta_plan_fragment.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include <unordered_map>

namespace duckdb {

struct DeltaCompileContext {
	DeltaCompileContext(OptimizerExtensionInput &input, Connection &metadata_con, const string &view,
	                    const DeltaViewModel &model);

	OptimizerExtensionInput &input;
	Connection &metadata_con;
	const string &view;
	const DeltaViewModel &model;
};

class DeltaCompiler {
public:
	DeltaCompiler(OptimizerExtensionInput &input, Connection &metadata_con, const string &view,
	              const DeltaViewModel &model);
	explicit DeltaCompiler(DeltaCompileContext context);

	DeltaPlanFragment Compile(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root);
	DeltaPlanFragment CompileCopiedSubtree(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root);

private:
	const DeltaModelNode *FindNode(LogicalOperator *op) const;
	DeltaPlanFragment CompileInternal(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root, bool copied_subtree);

	DeltaCompileContext context;
	unordered_map<const LogicalOperator *, idx_t> node_by_plan;
};

// Refresh-time model: node kinds, max table index and a refresh type whose only consumer is the SIMPLE_PROJECTION
// check in the join rule. Aux-state metadata and lineage are not loaded, so aux-dependent types are not refined.
DeltaViewModel BuildRefreshDeltaViewModel(OptimizerExtensionInput &input, LogicalOperator *optimized_plan,
                                          const vector<string> &output_names);

void LogDeltaModelSummary(const DeltaViewModel &model);

} // namespace duckdb

#endif // OPENIVM_DELTA_COMPILER_HPP
