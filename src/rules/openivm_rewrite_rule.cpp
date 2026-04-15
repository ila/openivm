#include "rules/openivm_rewrite_rule.hpp"

#include "rules/aggregate.hpp"
#include "rules/distinct.hpp"
#include "rules/filter.hpp"
#include "rules/join.hpp"
#include "rules/projection.hpp"
#include "rules/scan.hpp"
#include "rules/union.hpp"
#include "rules/window.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "upsert/openivm_index_regen.hpp"

#include "lpts_pipeline.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/parser.hpp"

#include <iostream>

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

/// Update CTE_REF nodes matching the given CTE table_index with the multiplicity column.
static void UpdateCteRefsWithMul(LogicalOperator *node, idx_t cte_table_index, const LogicalType &mul_type) {
	if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = node->Cast<LogicalCTERef>();
		if (ref.cte_index == cte_table_index &&
		    (ref.bound_columns.empty() || ref.bound_columns.back() != ivm::MULTIPLICITY_COL)) {
			ref.chunk_types.push_back(mul_type);
			ref.bound_columns.push_back(ivm::MULTIPLICITY_COL);
			OPENIVM_DEBUG_PRINT("[CTE]   Updated CTE_REF cte_index=%lu with mul column\n",
			                    (unsigned long)cte_table_index);
		}
	}
	for (auto &child : node->children) {
		UpdateCteRefsWithMul(child.get(), cte_table_index, mul_type);
	}
}

void IVMRewriteRule::AddInsertNode(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
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
	                                                  OpenIVMUtils::DeltaName(view_name),
	                                                  OnEntryNotFound::THROW_EXCEPTION, QueryErrorContext());
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

ModifiedPlan IVMRewriteRule::RewritePlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan,
                                         string &view, LogicalOperator *&root) {
	PlanWrapper pw(input, plan, view, root);

	OPENIVM_DEBUG_PRINT("[RewritePlan] Visiting node: %s\n", LogicalOperatorToString(plan->type).c_str());
	OPENIVM_DEBUG_PRINT("[RewritePlan] Node detail: %s\n", plan->ToString().c_str());

	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		IvmScanRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		IvmJoinRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		IvmProjectionRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		IvmAggregateRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		IvmFilterRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_UNION: {
		IvmUnionRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_DISTINCT: {
		IvmDistinctRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_WINDOW: {
		IvmWindowRule rule;
		return rule.Rewrite(pw);
	}
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE: {
		// CTE wrapper: children[0] = CTE definition, children[1] = consumer.
		auto &cte = pw.plan->Cast<LogicalCTE>();
		idx_t cte_table_index = cte.table_index;

		// 1. Rewrite the CTE definition (contains the actual operators like JOINs).
		auto def_mul = RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
		pw.plan->children[0] = std::move(def_mul.op);

		// 2. Update the CTE's column_count to include the new multiplicity column.
		cte.column_count = pw.plan->children[0]->GetColumnBindings().size();

		// 3. Update all CTE_REF nodes that reference this CTE (by cte_index == table_index).
		//    Add the multiplicity type and column name so downstream operators see the extra column.
		OPENIVM_DEBUG_PRINT("[CTE] Looking for CTE_REFs with cte_index=%lu\n", (unsigned long)cte_table_index);
		// Search the entire tree from this node down
		UpdateCteRefsWithMul(pw.plan.get(), cte_table_index, pw.mul_type);

		// 4. Rewrite the consumer (which reads from CTE_REFs).
		auto cons_mul = RewritePlan(pw.input, pw.plan->children[1], pw.view, pw.root);
		pw.plan->children[1] = std::move(cons_mul.op);
		pw.plan->ResolveOperatorTypes();
		return {std::move(pw.plan), cons_mul.mul_binding};
	}
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		// CTE reference: leaf that reads from a rewritten CTE definition.
		// The multiplicity column was added by the MATERIALIZED_CTE handler above.
		auto bindings = pw.plan->GetColumnBindings();
		ColumnBinding mul_binding = bindings.back();
		return {std::move(pw.plan), mul_binding};
	}
	default:
		throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(plan->type));
	}
}

void IVMRewriteRule::IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
	if (plan->children.empty()) {
		return;
	}

	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (!StringUtil::StartsWith(child->GetName(), "DOIVM")) {
		return;
	}

#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Activating the rewrite rule\n");
#endif

	auto child_get = dynamic_cast<LogicalGet *>(child);
	auto view = child_get->named_parameters["view_name"].ToString();
	auto view_catalog = child_get->named_parameters["view_catalog_name"].ToString();
	auto view_schema = child_get->named_parameters["view_schema_name"].ToString();

	Connection con(*input.context.db);
	con.Query("SET disabled_optimizers='" + string(ivm::DISABLED_OPTIMIZERS) + "';");

	auto v = con.Query("select sql_string from " + string(ivm::VIEWS_TABLE) + " where view_name = '" +
	                   OpenIVMUtils::EscapeValue(view) + "';");
	if (v->HasError()) {
		throw InternalException("Error while querying view definition");
	}
	string view_query = v->GetValue(0, 0).ToString();

	Parser parser;
	Planner planner(input.context);

	parser.ParseQuery(view_query);
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

	// Advance the main binder past all table indices in the plan to prevent collisions.
	// IvmJoinRule uses input.optimizer.binder which may not have been advanced by the
	// local optimizer. Walk the plan to find the highest table index used.
	{
		idx_t max_idx = FindMaxTableIndex(optimized_plan.get());
		while (input.optimizer.binder.GenerateTableIndex() <= max_idx) {
		}
	}

	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === Starting RewritePlan ===\n");
	auto root = optimized_plan.get();
	ModifiedPlan modified_plan = RewritePlan(input, optimized_plan, view, root);
	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === RewritePlan done, running AddInsertNode ===\n");
	AddInsertNode(input.context, input.optimizer.binder, modified_plan.op, view, view_catalog, view_schema);
	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === FINAL PLAN ===\n%s\n", modified_plan.op->ToString().c_str());
	plan = std::move(modified_plan.op);
	return;
}
} // namespace duckdb
