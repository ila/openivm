#ifndef OPENIVM_DELTA_COMPILER_HPP
#define OPENIVM_DELTA_COMPILER_HPP

#include "core/ivm_view_classifier.hpp"
#include "delta/delta_plan_fragment.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include <unordered_map>

namespace duckdb {

struct DeltaCompileAssumptions {
	bool all_sources_are_ducklake = false;
	bool keep_window_join_partitions = true;
	bool has_unsupported_incremental_construct = false;
};

struct DeltaCompileContext {
	DeltaCompileContext(OptimizerExtensionInput &input, Connection &metadata_con, string &view,
	                    const DeltaViewModel &model, DeltaCompileAssumptions assumptions);

	OptimizerExtensionInput &input;
	Connection &metadata_con;
	string &view;
	const DeltaViewModel &model;
	DeltaCompileAssumptions assumptions;
};

class DeltaCompiler {
public:
	DeltaCompiler(OptimizerExtensionInput &input, Connection &metadata_con, string &view, const DeltaViewModel &model,
	              DeltaCompileAssumptions assumptions = {});
	explicit DeltaCompiler(DeltaCompileContext context);

	DeltaPlanFragment Compile(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root);
	DeltaPlanFragment CompileCopiedSubtree(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root);

private:
	const DeltaModelNode *FindNode(LogicalOperator *op) const;
	DeltaPlanFragment CompileInternal(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root, bool copied_subtree);

	DeltaCompileContext context;
	unordered_map<const LogicalOperator *, idx_t> node_by_plan;
};

DeltaViewModel BuildRefreshDeltaViewModel(OptimizerExtensionInput &input, Connection &metadata_con,
                                          const string &view_name, LogicalOperator *optimized_plan,
                                          const vector<string> &output_names,
                                          DeltaCompileAssumptions *out_assumptions = nullptr);

void LogDeltaModelSummary(const DeltaViewModel &model);

} // namespace duckdb

#endif // OPENIVM_DELTA_COMPILER_HPP
