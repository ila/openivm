#include "delta/delta_operator.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "delta/operators/join.hpp"
#include "upsert/refresh_index_regen.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_delim_get.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

namespace {

struct BaseLeafInfo {
	vector<size_t> path;
	LogicalGet *get;
	LogicalOperator *node;
};

static bool IsJoinNode(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || type == LogicalOperatorType::LOGICAL_JOIN ||
	       type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT || type == LogicalOperatorType::LOGICAL_ANY_JOIN ||
	       type == LogicalOperatorType::LOGICAL_DELIM_JOIN || type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
}

static void CollectBaseLeaves(LogicalOperator *node, vector<size_t> path, vector<BaseLeafInfo> &leaves) {
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return;
	}
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		auto *get = dynamic_cast<LogicalGet *>(node);
		if (get && get->GetTable().get() != nullptr) {
			leaves.push_back({std::move(path), get, node});
		}
		return;
	}
	for (idx_t child_idx = 0; child_idx < node->children.size(); child_idx++) {
		path.push_back(child_idx);
		CollectBaseLeaves(node->children[child_idx].get(), path, leaves);
		path.pop_back();
	}
}

static void VerifyDelimJoinTypes(LogicalOperator *node) {
	if (IsJoinNode(node->type)) {
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (join && join->join_type != JoinType::INNER && join->join_type != JoinType::LEFT &&
		    join->join_type != JoinType::RIGHT && join->join_type != JoinType::OUTER &&
		    join->join_type != JoinType::MARK && join->join_type != JoinType::SEMI &&
		    join->join_type != JoinType::ANTI && join->join_type != JoinType::RIGHT_SEMI &&
		    join->join_type != JoinType::RIGHT_ANTI && join->join_type != JoinType::SINGLE) {
			throw Exception(ExceptionType::OPTIMIZER,
			                JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM DELIM_JOIN");
		}
	}
	for (auto &child : node->children) {
		VerifyDelimJoinTypes(child.get());
	}
}

static bool IsSemiAntiJoinType(JoinType join_type) {
	return join_type == JoinType::SEMI || join_type == JoinType::ANTI || join_type == JoinType::RIGHT_SEMI ||
	       join_type == JoinType::RIGHT_ANTI;
}

static bool IsSafeSemiAntiDelimJoin(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	if (!IsSemiAntiJoinType(join.join_type) || join.conditions.empty() || join.predicate) {
		return false;
	}
	for (auto &condition : join.conditions) {
		if (condition.comparison != ExpressionType::COMPARE_EQUAL &&
		    condition.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
	}
	for (auto &expr : join.duplicate_eliminated_columns) {
		if (!expr || expr->type != ExpressionType::BOUND_COLUMN_REF) {
			return false;
		}
	}
	return !join.duplicate_eliminated_columns.empty();
}

static void UpdateProjectionMapForLeaf(unique_ptr<LogicalOperator> &term, const BaseLeafInfo &leaf) {
	if (leaf.path.empty()) {
		return;
	}
	size_t child_side = leaf.path.back();
	unique_ptr<LogicalOperator> *parent = &term;
	for (size_t s = 0; s + 1 < leaf.path.size(); s++) {
		parent = &((*parent)->children[leaf.path[s]]);
	}
	if (!IsJoinNode((*parent)->type)) {
		return;
	}
	auto *join = dynamic_cast<LogicalComparisonJoin *>((*parent).get());
	if (!join) {
		return;
	}
	auto &proj_map = (child_side == 0) ? join->left_projection_map : join->right_projection_map;
	if (!proj_map.empty()) {
		idx_t mul_idx = leaf.node->GetColumnBindings().size();
		proj_map.push_back(mul_idx);
	}
}

static unique_ptr<LogicalOperator> BuildDelimKeySource(ClientContext &context, LogicalComparisonJoin &delim_join,
                                                       LogicalDelimGet &delim_get, bool allow_ordinal_fallback) {
	if (delim_join.children.size() != 2) {
		throw InternalException("DELIM_JOIN must have two children");
	}
	const idx_t source_child_idx = delim_join.delim_flipped ? 1 : 0;
	delim_join.children[source_child_idx]->ResolveOperatorTypes();
	auto source_bindings = delim_join.children[source_child_idx]->GetColumnBindings();
	auto source_copy = delim_join.children[source_child_idx]->Copy(context);
	source_copy->ResolveOperatorTypes();
	auto copy_bindings = source_copy->GetColumnBindings();
	if (source_bindings.size() != copy_bindings.size()) {
		throw InternalException("DELIM_JOIN source copy changed binding count");
	}

	vector<unique_ptr<Expression>> exprs;
	exprs.reserve(delim_join.duplicate_eliminated_columns.size());
	for (auto &expr : delim_join.duplicate_eliminated_columns) {
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			throw NotImplementedException("DELIM_JOIN duplicate-eliminated expression must be a column reference");
		}
		auto &col_ref = expr->Cast<BoundColumnRefExpression>();
		idx_t source_ordinal = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < source_bindings.size(); i++) {
			if (source_bindings[i] == col_ref.binding) {
				source_ordinal = i;
				break;
			}
		}
		if (source_ordinal == DConstants::INVALID_INDEX) {
			if (!allow_ordinal_fallback || col_ref.binding.column_index >= copy_bindings.size()) {
				throw InternalException("DELIM_JOIN duplicate-eliminated binding not found in source child");
			}
			source_ordinal = col_ref.binding.column_index;
		}
		exprs.push_back(make_uniq<BoundColumnRefExpression>(col_ref.return_type, copy_bindings[source_ordinal]));
	}
	if (exprs.empty()) {
		throw NotImplementedException("DELIM_JOIN without duplicate-eliminated columns is not supported");
	}

	auto projection = make_uniq<LogicalProjection>(delim_get.table_index, std::move(exprs));
	projection->children.push_back(std::move(source_copy));
	projection->ResolveOperatorTypes();

	vector<unique_ptr<Expression>> distinct_targets;
	auto bindings = projection->GetColumnBindings();
	for (idx_t i = 0; i < bindings.size(); i++) {
		distinct_targets.push_back(make_uniq<BoundColumnRefExpression>(projection->types[i], bindings[i]));
	}
	auto distinct = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
	distinct->children.push_back(std::move(projection));
	distinct->ResolveOperatorTypes();
	return std::move(distinct);
}

static void RebindAllExpressions(LogicalOperator &op, ColumnBindingReplacer &replacer);

static uint64_t BindingKey(const ColumnBinding &binding) {
	return (uint64_t)binding.table_index ^ ((uint64_t)binding.column_index * 0x9e3779b97f4a7c15ULL);
}

static void AddColumnBindingReplacements(const vector<ColumnBinding> &old_bindings,
                                         const vector<ColumnBinding> &new_bindings,
                                         vector<ReplacementBinding> &replacement_bindings,
                                         const ColumnBinding *stop_binding = nullptr) {
	const idx_t count = std::min(old_bindings.size(), new_bindings.size());
	for (idx_t i = 0; i < count; i++) {
		if (stop_binding && new_bindings[i] == *stop_binding) {
			break;
		}
		replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
	}
}

static void AddLeafBindingReplacements(const vector<ColumnBinding> &old_bindings,
                                       const vector<ColumnBinding> &new_bindings,
                                       vector<ReplacementBinding> &replacement_bindings,
                                       const ColumnBinding &mul_binding) {
	for (auto &old_binding : old_bindings) {
		for (auto &new_binding : new_bindings) {
			if (new_binding == mul_binding) {
				continue;
			}
			if (old_binding.column_index != new_binding.column_index) {
				continue;
			}
			replacement_bindings.emplace_back(old_binding, new_binding);
			break;
		}
	}
}

static ColumnBinding MapTermBinding(ColumnBinding binding, const unordered_map<old_idx, new_idx> &idx_map,
                                    const vector<ReplacementBinding> &replacement_bindings) {
	auto idx_entry = idx_map.find(binding.table_index);
	if (idx_entry != idx_map.end()) {
		binding.table_index = idx_entry->second;
	}

	for (idx_t pass = 0; pass <= replacement_bindings.size(); pass++) {
		bool replaced = false;
		for (auto &replacement : replacement_bindings) {
			if (binding == replacement.old_binding) {
				binding = replacement.new_binding;
				replaced = true;
				break;
			}
		}
		if (!replaced) {
			return binding;
		}
	}
	return binding;
}

static ColumnBinding FindOutputBinding(const vector<ColumnBinding> &term_bindings, ColumnBinding target) {
	for (auto &binding : term_bindings) {
		if (binding == target) {
			return binding;
		}
	}
	throw InternalException("DeltaDelimJoin: original output binding not found in rewritten term");
}

static bool ReplaceDelimGets(ClientContext &context, unique_ptr<LogicalOperator> &node,
                             vector<ReplacementBinding> &replacement_bindings,
                             LogicalComparisonJoin *active_delim_join = nullptr) {
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		if (!active_delim_join) {
			throw NotImplementedException("LOGICAL_DELIM_GET without enclosing DELIM_JOIN is not supported");
		}
		auto &delim_get = node->Cast<LogicalDelimGet>();
		auto old_bindings = delim_get.GetColumnBindings();
		node = BuildDelimKeySource(context, *active_delim_join, delim_get, true);
		auto new_bindings = node->GetColumnBindings();
		if (old_bindings.size() != new_bindings.size()) {
			throw InternalException("DELIM_GET replacement changed binding count");
		}
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			replacement_bindings.emplace_back(old_bindings[i], new_bindings[i]);
		}
		return true;
	}

	LogicalComparisonJoin *next_delim = active_delim_join;
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		next_delim = &node->Cast<LogicalComparisonJoin>();
	}

	bool replaced = false;
	for (auto &child : node->children) {
		if (!child) {
			continue;
		}
		replaced = ReplaceDelimGets(context, child, replacement_bindings, next_delim) || replaced;
	}
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN && replaced && !replacement_bindings.empty()) {
		ColumnBindingReplacer replacer;
		replacer.replacement_bindings = replacement_bindings;
		RebindAllExpressions(*node, replacer);
	}
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		node->Cast<LogicalComparisonJoin>().duplicate_eliminated_columns.clear();
		node->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
	}
	return replaced;
}

static void RebindAllExpressions(LogicalOperator &op, ColumnBindingReplacer &replacer) {
	for (auto &expr : op.expressions) {
		if (!expr) {
			continue;
		}
		replacer.VisitExpression(&expr);
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &condition : join.conditions) {
			if (condition.left) {
				replacer.VisitExpression(&condition.left);
			}
			if (condition.right) {
				replacer.VisitExpression(&condition.right);
			}
		}
		for (auto &expr : join.duplicate_eliminated_columns) {
			if (!expr) {
				continue;
			}
			replacer.VisitExpression(&expr);
		}
		if (join.predicate) {
			replacer.VisitExpression(&join.predicate);
		}
	}
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &aggregate = op.Cast<LogicalAggregate>();
		for (auto &expr : aggregate.groups) {
			if (!expr) {
				continue;
			}
			replacer.VisitExpression(&expr);
		}
	}
	for (auto &child : op.children) {
		if (!child) {
			continue;
		}
		RebindAllExpressions(*child, replacer);
	}
}

static void CollectMulBindings(const vector<ColumnBinding> &mul_bindings, unordered_set<uint64_t> &mul_set) {
	for (auto &mb : mul_bindings) {
		mul_set.insert(BindingKey(mb));
	}
}

static unique_ptr<Expression> BuildMultiplicityProduct(Binder &binder, const LogicalType &mul_type,
                                                       const vector<ColumnBinding> &mul_bindings) {
	FunctionBinder fbinder(binder);
	unique_ptr<Expression> product = make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[0]);
	for (size_t i = 1; i < mul_bindings.size(); i++) {
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(product));
		args.push_back(make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[i]));
		ErrorData err;
		product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true);
		if (!product) {
			throw InternalException("DeltaDelimJoin: failed to bind multiplicity product: %s", err.RawMessage());
		}
	}
	if (mul_bindings.size() % 2 == 0) {
		vector<unique_ptr<Expression>> args;
		args.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(-1)));
		args.push_back(std::move(product));
		ErrorData err;
		product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true);
		if (!product) {
			throw InternalException("DeltaDelimJoin: failed to bind multiplicity sign: %s", err.RawMessage());
		}
	}
	return product;
}

static unique_ptr<LogicalOperator> AssembleUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                    const vector<LogicalType> &types, Binder &binder) {
	auto result = std::move(terms[0]);
	for (size_t i = 1; i < terms.size(); i++) {
		result = make_uniq<LogicalSetOperation>(binder.GenerateTableIndex(), types.size(), std::move(result),
		                                        std::move(terms[i]), LogicalOperatorType::LOGICAL_UNION, true);
		result->types = types;
	}

	auto bindings = result->GetColumnBindings();
	vector<unique_ptr<Expression>> exprs;
	for (idx_t i = 0; i < bindings.size(); i++) {
		exprs.push_back(make_uniq<BoundColumnRefExpression>(types[i], bindings[i]));
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(exprs));
	projection->children.push_back(std::move(result));
	projection->types = types;
	return std::move(projection);
}

static ColumnBinding ReplaceOutputBindings(const vector<ColumnBinding> &original_bindings,
                                           unique_ptr<LogicalOperator> &result, LogicalOperator &root) {
	auto new_bindings = result->GetColumnBindings();
	ColumnBindingReplacer replacer;
	idx_t map_count = std::min(original_bindings.size(), new_bindings.size() - 1);
	for (idx_t col_idx = 0; col_idx < map_count; col_idx++) {
		replacer.replacement_bindings.emplace_back(original_bindings[col_idx], new_bindings[col_idx]);
	}
	replacer.stop_operator = result;
	replacer.VisitOperator(root);
	return new_bindings.back();
}

} // namespace

static bool ReplaceSafeSemiAntiDelimGets(ClientContext &context, unique_ptr<LogicalOperator> &node,
                                         vector<ReplacementBinding> *active_replacements = nullptr,
                                         LogicalComparisonJoin *active_delim_join = nullptr) {
	if (!node) {
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		if (!active_delim_join || !active_replacements) {
			return false;
		}
		auto &delim_get = node->Cast<LogicalDelimGet>();
		auto old_bindings = delim_get.GetColumnBindings();
		try {
			node = BuildDelimKeySource(context, *active_delim_join, delim_get, false);
		} catch (Exception &) {
			return false;
		}
		auto new_bindings = node->GetColumnBindings();
		if (old_bindings.size() != new_bindings.size()) {
			throw InternalException("DELIM_GET replacement changed binding count");
		}
		for (idx_t i = 0; i < old_bindings.size(); i++) {
			active_replacements->emplace_back(old_bindings[i], new_bindings[i]);
		}
		return true;
	}

	if (IsSafeSemiAntiDelimJoin(*node)) {
		vector<ReplacementBinding> local_replacements;
		auto &join = node->Cast<LogicalComparisonJoin>();
		bool replaced = false;
		for (auto &child : node->children) {
			replaced = ReplaceSafeSemiAntiDelimGets(context, child, &local_replacements, &join) || replaced;
		}
		if (replaced && !local_replacements.empty()) {
			ColumnBindingReplacer replacer;
			replacer.replacement_bindings = local_replacements;
			RebindAllExpressions(*node, replacer);
			join.duplicate_eliminated_columns.clear();
			node->type = LogicalOperatorType::LOGICAL_COMPARISON_JOIN;
			node->ResolveOperatorTypes();
			OPENIVM_DEBUG_PRINT(
			    "[PlanRewrite] Rewrote equality SEMI/ANTI DELIM_JOIN to distinct-key comparison join\n");
		}
		return replaced;
	}

	bool replaced = false;
	for (auto &child : node->children) {
		replaced = ReplaceSafeSemiAntiDelimGets(context, child, active_replacements, active_delim_join) || replaced;
	}
	return replaced;
}

bool RewriteSafeSemiAntiDelimGets(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	return ReplaceSafeSemiAntiDelimGets(context, plan);
}

DeltaPlanFragment CompileDelimJoinDelta(DeltaOperatorInput input) {
	ClientContext &context = input.context.input.context;
	Binder &binder = input.context.input.optimizer.binder;
	const vector<ColumnBinding> original_bindings = input.plan->GetColumnBindings();

	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::DELIM_JOIN_INCLUSION_EXCLUSION);
	VerifyDelimJoinTypes(input.plan.get());
	vector<BaseLeafInfo> leaves;
	CollectBaseLeaves(input.plan.get(), {}, leaves);
	if (leaves.empty()) {
		throw InternalException("DeltaDelimJoin: no mutable base leaves found");
	}
	if (leaves.size() > openivm::MAX_JOIN_TABLES) {
		throw NotImplementedException("DELIM_JOIN IVM not supported for joins with more than 16 base tables");
	}

	input.plan->ResolveOperatorTypes();
	auto output_types = input.plan->types;
	auto types = output_types;
	types.emplace_back(input.mul_type);

	vector<unique_ptr<LogicalOperator>> terms;
	const uint64_t term_count = (1ULL << leaves.size()) - 1;
	OPENIVM_DEBUG_PRINT("[DeltaDelimJoin] Rewriting DELIM_JOIN with %zu base leaves (%llu terms)\n", leaves.size(),
	                    (unsigned long long)term_count);
	for (uint64_t mask = 1; mask <= term_count; mask++) {
		auto term = input.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		vector<ColumnBinding> mul_bindings;
		vector<ReplacementBinding> output_replacements;

		for (size_t i = 0; i < leaves.size(); i++) {
			if (!(mask & (1ULL << i))) {
				continue;
			}
			auto &leaf_ref = GetNodeAtPath(term, leaves[i].path);
			auto leaf_bindings = leaf_ref->GetColumnBindings();
			DeltaGetResult delta_i = CreateDeltaGetNode(context, binder, leaves[i].get, input.context.view);
			auto delta_bindings = delta_i.node->GetColumnBindings();
			AddLeafBindingReplacements(leaf_bindings, delta_bindings, output_replacements, delta_i.mul_binding);
			mul_bindings.push_back(delta_i.mul_binding);
			leaf_ref = std::move(delta_i.node);
			UpdateProjectionMapForLeaf(term, leaves[i]);
		}

		vector<ReplacementBinding> delim_replacements;
		ReplaceDelimGets(context, term, delim_replacements);
		output_replacements.insert(output_replacements.end(), delim_replacements.begin(), delim_replacements.end());

		auto term_bindings = term->GetColumnBindings();
		vector<unique_ptr<Expression>> exprs;
		unordered_set<uint64_t> mul_set;
		CollectMulBindings(mul_bindings, mul_set);
		for (idx_t output_idx = 0; output_idx < original_bindings.size(); output_idx++) {
			auto mapped_binding =
			    MapTermBinding(original_bindings[output_idx], renumbered.idx_map, output_replacements);
			if (mul_set.count(BindingKey(mapped_binding))) {
				throw InternalException("DeltaDelimJoin: original output remapped to multiplicity binding");
			}
			auto output_binding = FindOutputBinding(term_bindings, mapped_binding);
			exprs.push_back(make_uniq<BoundColumnRefExpression>(output_types[output_idx], output_binding));
		}
		exprs.push_back(BuildMultiplicityProduct(binder, input.mul_type, mul_bindings));

		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(exprs));
		projection->children.push_back(std::move(term));
		projection->types = types;
		terms.push_back(std::move(projection));
	}

	auto result = AssembleUnionAll(terms, types, binder);
	ColumnBinding new_mul_binding = ReplaceOutputBindings(original_bindings, result, *input.root);
	input.plan = std::move(result);
	return {std::move(input.plan), new_mul_binding};
}

} // namespace duckdb
