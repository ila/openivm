#include "rules/openivm_rewrite_rule.hpp"

#include "rules/ivm_aggregate_rule.hpp"
#include "rules/ivm_filter_rule.hpp"
#include "rules/ivm_join_rule.hpp"
#include "rules/ivm_projection_rule.hpp"
#include "rules/ivm_scan_rule.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_index_regen.hpp"

#include "logical_plan_to_sql.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/parser/parser.hpp"

#include <iostream>

namespace duckdb {

void IVMRewriteRule::AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &view_name,
                                   string &view_catalog_name, string &view_schema_name) {
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
	auto insert_node = make_uniq<LogicalInsert>(*table, 999);

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
	case LogicalOperatorType::LOGICAL_JOIN: {
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
	if (child->GetName().substr(0, 5) != "DOIVM") {
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

	con.BeginTransaction();
	con.Query("SET disabled_optimizers='" + string(ivm::DISABLED_OPTIMIZERS) + "';");
	con.Commit();

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

	planner.CreatePlan(statement->Copy());
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

	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === Starting RewritePlan ===\n");
	auto root = optimized_plan.get();
	ModifiedPlan modified_plan = RewritePlan(input, optimized_plan, view, root);
	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === RewritePlan done, running AddInsertNode ===\n");
	AddInsertNode(input.context, modified_plan.op, view, view_catalog, view_schema);
	OPENIVM_DEBUG_PRINT("[IVM Rewrite] === FINAL PLAN ===\n%s\n", modified_plan.op->ToString().c_str());
	plan = std::move(modified_plan.op);
	return;
}
} // namespace duckdb
