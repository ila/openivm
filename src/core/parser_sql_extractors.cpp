#include "core/parser_sql_extractors.hpp"
#include "core/sql_utils.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/operator_expression.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/expression/star_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"

namespace duckdb {
namespace {

static unique_ptr<SelectStatement> ParseSelect(const string &sql) {
	auto parser = Parser::GetBuiltinParser();
	try {
		parser.ParseQuery(sql);
	} catch (const ParserException &) {
		return nullptr;
	}
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

static SelectNode *AsSelect(QueryNode &node) {
	return node.type == QueryNodeType::SELECT_NODE ? &node.Cast<SelectNode>() : nullptr;
}

static string ExprSQL(optional_ptr<ParsedExpression> expression) {
	return expression ? expression->ToString() : "";
}

static bool ReadSource(TableRef &ref, string &table, string &alias) {
	if (ref.type != TableReferenceType::BASE_TABLE) {
		return false;
	}
	auto &base = ref.Cast<BaseTableRef>();
	if (base.at_clause || !base.column_name_alias.empty()) {
		return false;
	}
	auto unaliased = base.Copy();
	unaliased->alias.clear();
	table = unaliased->ToString();
	alias = base.alias.empty() ? base.Table().GetIdentifierName() : base.alias.GetIdentifierName();
	return true;
}

static bool ReadOutputs(SelectNode &select, vector<string> &names, vector<string> &expressions,
                        vector<vector<string>> &column_refs) {
	for (auto &expression : select.select_list) {
		if (expression->GetExpressionClass() == ExpressionClass::STAR) {
			auto &star = expression->Cast<StarExpression>();
			// The caller obtains the bound left-source columns for an unmodified star.
			return select.select_list.size() == 1 && star.ExcludeList().empty() && star.ReplaceList().empty() &&
			       star.RenameList().empty() && !star.Expression() && !star.IsColumns();
		}
		names.push_back(expression->GetName().GetIdentifierName());
		expressions.push_back(expression->ToString());
		column_refs.push_back(expression->GetExpressionClass() == ExpressionClass::COLUMN_REF
		                          ? IdentifiersToStrings(expression->Cast<ColumnRefExpression>().ColumnNames())
		                          : vector<string>());
	}
	return !names.empty();
}

static void CollectSelects(QueryNode &node, vector<SelectNode *> &result);

static void CollectSourceSelects(TableRef &ref, vector<SelectNode *> &result) {
	if (ref.type == TableReferenceType::SUBQUERY) {
		CollectSelects(*ref.Cast<SubqueryRef>().subquery->node, result);
	} else if (ref.type == TableReferenceType::JOIN) {
		auto &join = ref.Cast<JoinRef>();
		CollectSourceSelects(*join.left, result);
		CollectSourceSelects(*join.right, result);
	}
}

static void CollectSelects(QueryNode &node, vector<SelectNode *> &result) {
	if (auto select = AsSelect(node)) {
		result.push_back(select);
		CollectSourceSelects(*select->from_table, result);
	} else if (node.type == QueryNodeType::SET_OPERATION_NODE) {
		for (auto &child : node.Cast<SetOperationNode>().children) {
			CollectSelects(*child, result);
		}
	}
	for (auto &entry : node.cte_map.map) {
		CollectSelects(*entry.second->query_node, result);
	}
}

static vector<SelectNode *> Selects(QueryNode &root) {
	vector<SelectNode *> result;
	CollectSelects(root, result);
	return result;
}

static optional_ptr<FunctionExpression> MatchFunction(ParsedExpression &expression, const string &name) {
	if (expression.GetExpressionClass() != ExpressionClass::FUNCTION) {
		return nullptr;
	}
	auto &function = expression.Cast<FunctionExpression>();
	return StringUtil::CIEquals(function.FunctionName().GetIdentifierName(), name) ? &function : nullptr;
}

static string ColumnName(const ParsedExpression &expression) {
	return expression.GetExpressionClass() == ExpressionClass::COLUMN_REF
	           ? expression.Cast<ColumnRefExpression>().GetColumnName().GetIdentifierName()
	           : "";
}

static void Conjuncts(ParsedExpression &expression, vector<ParsedExpression *> &result) {
	if (expression.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expression.Cast<ConjunctionExpression>().GetChildren()) {
			Conjuncts(*child, result);
		}
	} else {
		result.push_back(&expression);
	}
}

static string RenameQualifier(ParsedExpression &expression, const string &table, const string &alias,
                              const string &replacement) {
	auto copy = expression.Copy();
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(*copy, [&](ColumnRefExpression &column) {
		if (column.ColumnNames().size() > 1 &&
		    (StringUtil::CIEquals(column.ColumnNames()[column.ColumnNames().size() - 2].GetIdentifierName(), alias) ||
		     StringUtil::CIEquals(column.ColumnNames()[column.ColumnNames().size() - 2].GetIdentifierName(),
		                          SqlUtils::LastIdentifierPart(table)))) {
			column.ColumnNamesMutable() = {Identifier(replacement), column.GetColumnName()};
		}
	});
	return copy->ToString();
}

} // namespace

bool ExtractInnerDistinct(const string &sql, vector<string> &out_cols, string &out_input_sql, string &out_source,
                          string &out_filter_sql) {
	auto statement = ParseSelect(sql);
	if (!statement) {
		return false;
	}
	SelectNode *distinct = nullptr;
	for (auto select : Selects(*statement->node)) {
		for (auto &modifier : select->modifiers) {
			if (modifier->type == ResultModifierType::DISTINCT_MODIFIER) {
				if (distinct || !modifier->Cast<DistinctModifier>().distinct_on_targets.empty()) {
					return false;
				}
				distinct = select;
			}
		}
	}
	string alias;
	if (!distinct || !ReadSource(*distinct->from_table, out_source, alias) ||
	    !distinct->groups.group_expressions.empty() || distinct->having) {
		return false;
	}
	for (auto &expression : distinct->select_list) {
		if (expression->GetExpressionClass() == ExpressionClass::STAR) {
			return false;
		}
		out_cols.push_back(expression->ToString());
	}
	out_filter_sql = ExprSQL(distinct->where_clause);
	auto input = distinct->Copy();
	input->modifiers.clear();
	out_input_sql = input->ToString();
	OPENIVM_DEBUG_PRINT("[EXTRACT] Parsed inner DISTINCT from %s\n", out_source.c_str());
	return !out_cols.empty();
}

bool ExtractCountDistinctAggregate(const string &sql, const vector<string> &group_columns,
                                   const vector<string> &output_names, CountDistinctExtract &out) {
	auto statement = ParseSelect(sql);
	auto select = statement ? AsSelect(*statement->node) : nullptr;
	string alias;
	if (!select || group_columns.empty() || select->groups.group_expressions.size() != group_columns.size() ||
	    !ReadSource(*select->from_table, out.source, alias)) {
		return false;
	}
	idx_t count_index = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < select->select_list.size(); i++) {
		auto count = MatchFunction(*select->select_list[i], "count");
		if (!count) {
			continue;
		}
		if (count_index != DConstants::INVALID_INDEX || !count->Distinct() || count->GetArguments().size() != 1 ||
		    count->Filter()) {
			return false;
		}
		count_index = i;
		out.distinct_expr = count->GetArguments()[0].GetExpression().ToString();
		out.distinct_col = ColumnName(count->GetArguments()[0].GetExpression());
		if (out.distinct_col.empty()) {
			out.distinct_col = "openivm_distinct_value";
		}
	}
	if (count_index == DConstants::INVALID_INDEX || count_index >= output_names.size()) {
		return false;
	}
	out.output_col = output_names[count_index];
	out.filter = ExprSQL(select->where_clause);
	for (auto &group : select->groups.group_expressions) {
		out.group_exprs.push_back(group->ToString());
	}
	return true;
}

bool ExtractFilteredGroupCount(const string &sql, const vector<string> &output_names, FilteredGroupCountExtract &out) {
	auto statement = ParseSelect(sql);
	if (!statement || output_names.size() != 1) {
		return false;
	}
	auto selects = Selects(*statement->node);
	bool has_count = false;
	for (auto select : selects) {
		for (auto &expression : select->select_list) {
			has_count |= bool(MatchFunction(*expression, "count_star"));
		}
	}
	if (!has_count) {
		return false;
	}
	for (auto select : selects) {
		if (select->select_list.size() != 2 || select->groups.group_expressions.size() != 1) {
			continue;
		}
		string alias;
		if (!ReadSource(*select->from_table, out.source, alias)) {
			continue;
		}
		for (idx_t i = 0; i < 2; i++) {
			auto sum = MatchFunction(*select->select_list[i], "sum");
			if (!sum || sum->GetArguments().size() != 1 || sum->Distinct() || sum->Filter()) {
				continue;
			}
			out.group_col = ColumnName(*select->select_list[1 - i]);
			out.sum_col = ColumnName(sum->GetArguments()[0].GetExpression());
			out.sum_alias = select->select_list[i]->GetName().GetIdentifierName();
			if (out.group_col.empty() || out.sum_col.empty() ||
			    !StringUtil::CIEquals(ColumnName(*select->groups.group_expressions[0]), out.group_col)) {
				return false;
			}
			for (auto outer : selects) {
				if (!outer->where_clause || outer->where_clause->GetExpressionClass() != ExpressionClass::COMPARISON) {
					continue;
				}
				auto &comparison = outer->where_clause->Cast<ComparisonExpression>();
				if (!StringUtil::CIEquals(ColumnName(*comparison.LeftMutable()), out.sum_alias) ||
				    (comparison.GetExpressionType() != ExpressionType::COMPARE_LESSTHAN &&
				     comparison.GetExpressionType() != ExpressionType::COMPARE_GREATERTHAN) ||
				    comparison.Right().GetExpressionClass() != ExpressionClass::CONSTANT) {
					continue;
				}
				auto value = comparison.Right().Cast<ConstantExpression>().GetLiteral().ToValue();
				if (value.IsNull() || !value.type().IsNumeric() || value.GetValue<double>() != 0) {
					return false;
				}
				out.comparison_op = ExpressionTypeToOperator(comparison.GetExpressionType());
				out.threshold_sql = comparison.Right().ToString();
				out.output_col = output_names[0];
				return true;
			}
		}
	}
	return false;
}

bool ExtractSemiAntiQuery(const string &sql, SemiAntiExtract &out) {
	auto &meta = out.meta;
	auto statement = ParseSelect(sql);
	auto select = statement ? AsSelect(*statement->node) : nullptr;
	if (!select || !ReadOutputs(*select, meta.output_cols, out.output_exprs, out.output_refs)) {
		return false;
	}
	if (select->from_table->type == TableReferenceType::JOIN) {
		auto &join = select->from_table->Cast<JoinRef>();
		if (join.type == JoinType::SEMI || join.type == JoinType::ANTI) {
			if ((!join.condition && join.using_columns.empty()) ||
			    !ReadSource(*join.left, meta.left_table, meta.left_alias) ||
			    !ReadSource(*join.right, meta.right_table, meta.right_alias)) {
				return false;
			}
			meta.join_type = join.type == JoinType::SEMI ? SemiAntiJoinType::SEMI : SemiAntiJoinType::ANTI;
			if (join.condition) {
				meta.predicate = join.condition->ToString();
			} else {
				vector<string> equalities;
				for (const auto &column : join.using_columns) {
					auto quoted = SqlUtils::QuoteIdentifier(column);
					equalities.push_back(SqlUtils::QuoteIdentifier(meta.left_alias) + "." + quoted + " = " +
					                     SqlUtils::QuoteIdentifier(meta.right_alias) + "." + quoted);
				}
				meta.predicate = StringUtil::Join(equalities, " AND ");
				OPENIVM_DEBUG_PRINT("[SEMI/ANTI] Expanded USING predicate: %s\n", meta.predicate.c_str());
			}
			meta.post_filter = ExprSQL(select->where_clause);
			return true;
		}
	}
	if (!select->where_clause) {
		return false;
	}
	vector<ParsedExpression *> conjuncts;
	Conjuncts(*select->where_clause, conjuncts);
	SubqueryExpression *subquery = nullptr;
	bool anti = false;
	vector<string> filters;
	for (auto expression : conjuncts) {
		bool negated = expression->GetExpressionType() == ExpressionType::OPERATOR_NOT;
		auto candidate = negated ? expression->Cast<OperatorExpression>().GetChildren()[0].get() : expression;
		if (candidate->GetExpressionClass() != ExpressionClass::SUBQUERY) {
			filters.push_back("(" + expression->ToString() + ")");
			continue;
		}
		if (subquery) {
			return false;
		}
		subquery = &candidate->Cast<SubqueryExpression>();
		anti = negated;
	}
	if (!subquery || (subquery->GetSubqueryType() != SubqueryType::EXISTS &&
	                  (subquery->GetSubqueryType() != SubqueryType::ANY ||
	                   subquery->GetComparisonType() != ExpressionType::COMPARE_EQUAL))) {
		return false;
	}
	auto right = AsSelect(*subquery->Subquery()->node);
	if (!right || !ReadSource(*right->from_table, meta.right_table, meta.right_alias)) {
		return false;
	}
	meta.join_type = anti ? SemiAntiJoinType::ANTI : SemiAntiJoinType::SEMI;
	meta.post_filter = StringUtil::Join(filters, " AND ");
	bool simple_left = ReadSource(*select->from_table, meta.left_table, meta.left_alias);
	if (subquery->GetSubqueryType() == SubqueryType::EXISTS) {
		if (!simple_left) {
			return false;
		}
		meta.predicate = right->where_clause ? right->where_clause->ToString() : "true";
		return true;
	}
	if (!subquery->GetChild() || right->select_list.size() != 1) {
		return false;
	}
	if (!simple_left) {
		auto left = select->Copy();
		left->Cast<SelectNode>().where_clause.reset();
		left->modifiers.clear();
		meta.left_table = "(" + left->ToString() + ")";
		meta.left_alias = "openivm_left";
		out.left_is_subquery = true;
	}
	auto original_right_alias = meta.right_alias;
	meta.right_alias = "openivm_right";
	out.left_key_col = "openivm_saj_key";
	out.left_key_expr = simple_left
	                        ? RenameQualifier(*subquery->GetChild(), meta.left_table, meta.left_alias, meta.left_alias)
	                        : SqlUtils::QuoteIdentifier(meta.left_alias) + "." +
	                              SqlUtils::QuoteIdentifier(subquery->GetChild()->GetName());
	out.right_key_expr =
	    RenameQualifier(*right->select_list[0], meta.right_table, original_right_alias, meta.right_alias);
	meta.predicate = out.left_key_expr + " = " + out.right_key_expr;
	if (right->where_clause) {
		meta.right_filter =
		    RenameQualifier(*right->where_clause, meta.right_table, original_right_alias, meta.right_alias);
	}
	if (anti) {
		meta.null_aware = true;
		meta.null_aware_left_col = "openivm_not_in_lhs_is_null";
		out.null_aware_left_expr = "(" + out.left_key_expr + " IS NULL)";
		meta.null_aware_right_expr = out.right_key_expr;
	}
	OPENIVM_DEBUG_PRINT("[EXTRACT] Parsed %s membership query\n",
	                    SEMI_ANTI_JOIN_TYPE_NAMES[static_cast<idx_t>(meta.join_type)]);
	return true;
}

} // namespace duckdb
