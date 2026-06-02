#include "rules/incremental_rewrite_rule.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/sql_utils.hpp"
#include "delta/delta_compiler.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

/// Walk a plan tree and return the highest table_index found in any column binding.
static idx_t FindMaxTableIndex(LogicalOperator *node) {
	idx_t max_idx = 0;
	for (auto &b : node->GetColumnBindings()) {
		max_idx = std::max(max_idx, b.table_index);
	}
	for (auto &child : node->children) {
		max_idx = std::max(max_idx, FindMaxTableIndex(child.get()));
	}
	return max_idx;
}

void IncrementalRewriteRule::AddInsertNode(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                                           string &view_name, string &view_catalog_name, string &view_schema_name) {
#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("\nAdd the insert node to the plan...\n");
	OPENIVM_DEBUG_PRINT("Plan:\n%s\nParameters:", plan->ToString().c_str());
	for (const auto &i_param : plan->ParamsToString()) {
		OPENIVM_DEBUG_PRINT("%s", i_param.second.c_str());
	}
	OPENIVM_DEBUG_PRINT("\n---end of insert node output---\n");
#endif

	auto table = Catalog::GetEntry<TableCatalogEntry>(context, view_catalog_name, view_schema_name,
	                                                  SqlUtils::DeltaName(view_name), OnEntryNotFound::THROW_EXCEPTION,
	                                                  QueryErrorContext());
	auto insert_node = make_uniq<LogicalInsert>(*table, binder.GenerateTableIndex());

	Value value;
	unique_ptr<BoundConstantExpression> exp;
	for (size_t i = 0; i < plan->expressions.size(); i++) {
		insert_node->expected_types.emplace_back(plan->expressions[i]->return_type);
		value = Value(plan->expressions[i]->return_type);
		exp = make_uniq<BoundConstantExpression>(std::move(value));
		insert_node->bound_defaults.emplace_back(std::move(exp));
	}

	insert_node->children.emplace_back(std::move(plan));
	plan = std::move(insert_node);
}

void IncrementalRewriteRule::IncrementalRewriteRuleFunction(OptimizerExtensionInput &input,
                                                            duckdb::unique_ptr<LogicalOperator> &plan) {
	if (plan->children.empty()) {
		return;
	}

	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (!StringUtil::StartsWith(child->GetName(), "COMPUTEDELTA")) {
		return;
	}

#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Activating the rewrite rule\n");
#endif

	auto child_get = dynamic_cast<LogicalGet *>(child);
	if (!child_get) {
		throw InternalException("ComputeDelta marker must be represented as a logical get");
	}
	auto view = child_get->named_parameters["view_name"].ToString();
	auto view_catalog = child_get->named_parameters["view_catalog_name"].ToString();
	auto view_schema = child_get->named_parameters["view_schema_name"].ToString();

	Connection con(*input.context.db);
	con.Query("SET disabled_optimizers='" + string(openivm::DISABLED_OPTIMIZERS) + "';");

	auto v = con.Query("select sql_string from " + string(openivm::VIEWS_TABLE) + " where view_name = '" +
	                   SqlUtils::EscapeValue(view) + "';");
	if (v->HasError()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: cannot read view definition for '" + view + "': " + v->GetError());
	}
	if (v->RowCount() == 0 || v->GetValue(0, 0).IsNull()) {
		throw Exception(ExceptionType::CATALOG, "IVM: missing view definition for '" + view + "'");
	}
	string view_query = v->GetValue(0, 0).ToString();

	Parser parser;
	Planner planner(input.context);

	parser.ParseQuery(view_query);
	if (parser.statements.empty()) {
		throw Exception(ExceptionType::PARSER, "IVM: empty view definition for '" + view + "'");
	}
	auto statement = parser.statements[0].get();

	OPENIVM_DEBUG_PRINT("[REWRITE] About to CreatePlan for view query\n");
	planner.CreatePlan(statement->Copy());
	OPENIVM_DEBUG_PRINT("[REWRITE] CreatePlan done\n");
#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Unoptimized plan: \n%s\n", planner.plan->ToString().c_str());
#endif
	Optimizer optimizer(*planner.binder, input.context);
	auto optimized_plan = optimizer.Optimize(std::move(planner.plan));

	// Reset disabled_optimizers to avoid polluting the session
	con.Query("RESET disabled_optimizers;");

#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Optimized plan: \n%s\n", optimized_plan->ToString().c_str());
#endif

	if (optimized_plan->children.empty()) {
		throw NotImplementedException("Plan contains single node, this is not supported");
	}

	auto output_names = PrepareOutputNames(optimized_plan.get(), planner.names);
	DeltaCompileAssumptions assumptions;
	auto delta_model = BuildRefreshDeltaViewModel(input, con, view, optimized_plan.get(), output_names, &assumptions);
	LogDeltaModelSummary(delta_model);

	// Advance the main binder past all table indices in the plan to prevent collisions.
	// Join delta compilation uses input.optimizer.binder which may not have been advanced by the
	// local optimizer. Walk the plan to find the highest table index used.
	{
		idx_t max_idx = FindMaxTableIndex(optimized_plan.get());
		while (input.optimizer.binder.GenerateTableIndex() <= max_idx) {
		}
	}

	DeltaCompiler compiler(input, con, view, delta_model, assumptions);

	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === Starting IR delta compilation ===\n");
	auto root = optimized_plan.get();
	DeltaPlanFragment fragment = compiler.Compile(optimized_plan, root);
	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === Delta compilation done, running AddInsertNode ===\n");
	AddInsertNode(input.context, input.optimizer.binder, fragment.op, view, view_catalog, view_schema);
	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === FINAL PLAN ===\n%s\n", fragment.op->ToString().c_str());
	plan = std::move(fragment.op);
	return;
}

} // namespace duckdb
