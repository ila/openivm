#include "core/time_travel_pins.hpp"

#include "core/openivm_debug.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/expression/subquery_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/query_node/cte_node.hpp"
#include "duckdb/parser/query_node/recursive_cte_node.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/insert_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/pivotref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "lpts_helpers.hpp"
#include "lpts_sql_scanner.hpp"

#include <cctype>

namespace duckdb {
namespace openivm {

using BaseTableRefCallback = std::function<void(BaseTableRef &)>;

// A `BaseTableRef` whose unqualified name matches a CTE visible at that point is a reference to
// the CTE, not a scan of a same-named relation, so it must never be treated as an unpinned scan.
struct RefVisitor {
	const BaseTableRefCallback &callback;
	case_insensitive_set_t cte_names;
};

static void VisitQueryNode(QueryNode &node, const RefVisitor &visitor);

static void VisitExpression(ParsedExpression &expression, const RefVisitor &visitor) {
	if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>();
		if (subquery.subquery && subquery.subquery->node) {
			VisitQueryNode(*subquery.subquery->node, visitor);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(expression,
	                                            [&](ParsedExpression &child) { VisitExpression(child, visitor); });
}

static void VisitTableRef(TableRef &ref, const RefVisitor &visitor) {
	switch (ref.type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base_table = ref.Cast<BaseTableRef>();
		bool unqualified = base_table.catalog_name.empty() && base_table.schema_name.empty();
		if (unqualified && visitor.cte_names.find(base_table.table_name) != visitor.cte_names.end()) {
			break;
		}
		visitor.callback(base_table);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			VisitTableRef(*join.left, visitor);
		}
		if (join.right) {
			VisitTableRef(*join.right, visitor);
		}
		if (join.condition) {
			VisitExpression(*join.condition, visitor);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref.Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			VisitQueryNode(*subquery.subquery->node, visitor);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref.Cast<PivotRef>();
		if (pivot.source) {
			VisitTableRef(*pivot.source, visitor);
		}
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &table_function = ref.Cast<TableFunctionRef>();
		if (table_function.function) {
			VisitExpression(*table_function.function, visitor);
		}
		break;
	}
	default:
		break;
	}
}

static void VisitQueryNode(QueryNode &node, const RefVisitor &visitor) {
	// Each CTE body sees preceding siblings, but not its own name.
	RefVisitor scoped {visitor.callback, visitor.cte_names};
	for (auto &cte : node.cte_map.map) {
		if (cte.second->query && cte.second->query->node) {
			VisitQueryNode(*cte.second->query->node, scoped);
		}
		scoped.cte_names.insert(cte.first);
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			VisitTableRef(*select.from_table, scoped);
		}
		auto visit = [&](unique_ptr<ParsedExpression> &expression) {
			if (expression) {
				VisitExpression(*expression, scoped);
			}
		};
		for (auto &expression : select.select_list) {
			visit(expression);
		}
		for (auto &expression : select.groups.group_expressions) {
			visit(expression);
		}
		visit(select.where_clause);
		visit(select.having);
		visit(select.qualify);
		break;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &set_operation = node.Cast<SetOperationNode>();
		for (auto &child : set_operation.children) {
			if (child) {
				VisitQueryNode(*child, scoped);
			}
		}
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &recursive_cte = node.Cast<RecursiveCTENode>();
		RefVisitor recursive_scope {visitor.callback, scoped.cte_names};
		recursive_scope.cte_names.insert(recursive_cte.ctename);
		if (recursive_cte.left) {
			VisitQueryNode(*recursive_cte.left, recursive_scope);
		}
		if (recursive_cte.right) {
			VisitQueryNode(*recursive_cte.right, recursive_scope);
		}
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte = node.Cast<CTENode>();
		if (cte.query) {
			VisitQueryNode(*cte.query, scoped);
		}
		if (cte.child) {
			RefVisitor child_scope {visitor.callback, scoped.cte_names};
			child_scope.cte_names.insert(cte.ctename);
			VisitQueryNode(*cte.child, child_scope);
		}
		break;
	}
	default:
		break;
	}
	ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, [&](unique_ptr<ParsedExpression> &expression) {
		if (expression) {
			VisitExpression(*expression, scoped);
		}
	});
}

static void VisitStatement(SQLStatement &statement, const RefVisitor &visitor) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT: {
		auto &select = statement.Cast<SelectStatement>();
		if (select.node) {
			VisitQueryNode(*select.node, visitor);
		}
		break;
	}
	case StatementType::CREATE_STATEMENT: {
		auto &create = statement.Cast<CreateStatement>();
		if (create.info && create.info->type == CatalogType::TABLE_ENTRY) {
			auto &table_info = create.info->Cast<CreateTableInfo>();
			if (table_info.query && table_info.query->node) {
				VisitQueryNode(*table_info.query->node, visitor);
			}
		}
		break;
	}
	case StatementType::INSERT_STATEMENT: {
		auto &insert = statement.Cast<InsertStatement>();
		if (insert.select_statement && insert.select_statement->node) {
			VisitQueryNode(*insert.select_statement->node, visitor);
		}
		break;
	}
	default:
		break;
	}
}

// Resolve without the AT clause; DuckDB owns lookup errors for unresolved relations.
static optional_ptr<CatalogEntry> LookupPinRelation(ClientContext &context, BaseTableRef &ref) {
	QueryErrorContext error_context;
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, ref.table_name, error_context);
	try {
		auto catalog = ref.catalog_name;
		auto schema = ref.schema_name;
		Binder::BindSchemaOrCatalog(context, catalog, schema);
		return Catalog::GetEntry(context, catalog, schema, lookup, OnEntryNotFound::RETURN_NULL);
	} catch (const std::exception &) {
		return nullptr;
	}
}

[[noreturn]] static void ThrowAmbiguousPin(const string &table_name, const string &reason) {
	throw NotImplementedException(
	    "OpenIVM cannot compile a materialized view that pins relation '%s' ambiguously: %s. DuckDB resolves a "
	    "time-travel qualifier during catalog lookup, so the bound plan keeps no per-scan record of it and "
	    "re-attaching one can only be keyed by relation — conflating scans that must read different snapshots is "
	    "not something OpenIVM will do silently. Give every scan of the relation the same pin, or split them into "
	    "separate views.",
	    table_name, reason);
}

TimeTravelPins TimeTravelPins::Peel(ClientContext &context, SQLStatement &statement,
                                    const BaseTableRefCallback &qualify_source) {
	TimeTravelPins result;
	case_insensitive_set_t unpinned;
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		if (qualify_source) {
			qualify_source(ref);
		}
		if (!ref.at_clause) {
			// Track native scans too: the pin map is keyed by relation name, so mixed pinned and
			// unpinned uses of that name cannot be represented safely.
			unpinned.insert(ref.table_name);
			return;
		}
		Pin pin;
		pin.catalog = ref.catalog_name;
		pin.schema = ref.schema_name;
		auto relation = LookupPinRelation(context, ref);
		pin.binds_natively = !relation || relation->ParentCatalog().SupportsTimeTravel();
		if (relation) {
			pin.catalog = relation->ParentCatalog().GetName();
			pin.schema = relation->ParentSchema().name;
		}
		// Binding loses the explicit AT clause even when it names the current native snapshot.
		// Preserve it for LPTS, but only remove foreign pins from the locally executable query.
		pin.snapshot = pin.binds_natively ? ref.at_clause->Copy() : std::move(ref.at_clause);
		auto existing = result.pins.find(ref.table_name);
		if (existing != result.pins.end()) {
			if (!AtClause::Equals(existing->second.snapshot, pin.snapshot)) {
				ThrowAmbiguousPin(ref.table_name, "it is pinned to both '" + existing->second.snapshot->ToString() +
				                                      "' and '" + pin.snapshot->ToString() + "'");
			}
			if (existing->second.catalog != pin.catalog || existing->second.schema != pin.schema) {
				ThrowAmbiguousPin(ref.table_name, "the same pin names two differently qualified relations");
			}
		}
		OPENIVM_DEBUG_PRINT("[TIME TRAVEL] Recorded pin '%s' on relation '%s' (native=%d)\n",
		                    pin.snapshot->ToString().c_str(), ref.table_name.c_str(), pin.binds_natively);
		result.pins[ref.table_name] = std::move(pin);
	};
	RefVisitor visitor {callback, case_insensitive_set_t()};
	VisitStatement(statement, visitor);
	for (auto &entry : result.pins) {
		if (unpinned.find(entry.first) != unpinned.end()) {
			ThrowAmbiguousPin(entry.first, "it is scanned both pinned and unpinned");
		}
	}
	return result;
}

SnapshotResolver TimeTravelPins::Resolver() const {
	if (pins.empty()) {
		return {};
	}
	return [this](const TableCatalogEntry &table) -> unique_ptr<AtClause> {
		auto entry = pins.find(table.name);
		if (entry == pins.end()) {
			return nullptr;
		}
		auto &pin = entry->second;
		if ((!pin.catalog.empty() && !StringUtil::CIEquals(pin.catalog, table.ParentCatalog().GetName())) ||
		    (!pin.schema.empty() && !StringUtil::CIEquals(pin.schema, table.schema.name))) {
			return nullptr;
		}
		return pin.snapshot->Copy();
	};
}

// Copy the quoted run starting at `sql[start]` (whose delimiter is `sql[start]`) into `result`,
// returning the index just past the closing delimiter. Doubled delimiters escape.
static idx_t CopyQuotedRun(const string &sql, idx_t start, string &result) {
	char quote = sql[start];
	result += quote;
	idx_t i = start + 1;
	while (i < sql.size()) {
		if (sql[i] == quote) {
			if (i + 1 < sql.size() && sql[i + 1] == quote) {
				result += quote;
				result += quote;
				i += 2;
				continue;
			}
			result += quote;
			return i + 1;
		}
		result += sql[i];
		i++;
	}
	return i;
}

string TimeTravelPins::StripFrom(ClientContext &context, const string &sql) const {
	if (pins.empty()) {
		return sql;
	}
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("Expected one view query while stripping time-travel pins");
	}
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		auto entry = pins.find(ref.table_name);
		if (entry == pins.end() || entry->second.binds_natively) {
			return;
		}
		auto &pin = entry->second;
		auto relation = LookupPinRelation(context, ref);
		if (relation && StringUtil::CIEquals(pin.catalog, relation->ParentCatalog().GetName()) &&
		    StringUtil::CIEquals(pin.schema, relation->ParentSchema().name)) {
			ref.at_clause.reset();
		}
	};
	VisitStatement(*parser.statements[0], RefVisitor {callback, {}});
	return parser.statements[0]->ToString();
}

// Copy the `[catalog.][schema.]relation` chain starting at `sql[start]` into `result`, honouring
// quoted components. Reports the unquoted final component — the relation name pins are keyed by —
// and whether the chain carried a catalog/schema prefix, which a CTE reference never does.
static idx_t CopyQualifiedIdentifierChain(const string &sql, idx_t start, string &result, string &final_component,
                                          bool &qualified) {
	idx_t i = start;
	qualified = false;
	while (true) {
		if (i < sql.size() && (sql[i] == '"' || sql[i] == '`')) {
			idx_t quoted_start = result.size();
			i = CopyQuotedRun(sql, i, result);
			final_component = result.substr(quoted_start + 1, result.size() - quoted_start - 2);
		} else if (i < sql.size() && IsIdentStart(sql[i])) {
			idx_t end = i;
			while (end < sql.size() && (IsIdentPart(sql[end]) || sql[end] == '$')) {
				end++;
			}
			final_component = sql.substr(i, end - i);
			result.append(sql, i, end - i);
			i = end;
		} else {
			break;
		}
		if (i < sql.size() && sql[i] == '.') {
			result += '.';
			i++;
			qualified = true;
			continue;
		}
		break;
	}
	return i;
}

// Whether `sql` already carries `qualifier` at `pos` (ignoring how its whitespace is spelled), so an
// AST-rendered scan is never given a second copy of its own pin.
static bool CarriesQualifierAt(const string &sql, idx_t pos, const string &qualifier) {
	idx_t sql_pos = pos;
	idx_t qualifier_pos = 0;
	while (qualifier_pos < qualifier.size()) {
		if (std::isspace(static_cast<unsigned char>(qualifier[qualifier_pos]))) {
			bool sql_has_space = sql_pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[sql_pos]));
			while (qualifier_pos < qualifier.size() &&
			       std::isspace(static_cast<unsigned char>(qualifier[qualifier_pos]))) {
				qualifier_pos++;
			}
			while (sql_pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[sql_pos]))) {
				sql_pos++;
			}
			if (!sql_has_space) {
				return false;
			}
			continue;
		}
		if (sql_pos >= sql.size() || std::tolower(static_cast<unsigned char>(sql[sql_pos])) !=
		                                 std::tolower(static_cast<unsigned char>(qualifier[qualifier_pos]))) {
			return false;
		}
		sql_pos++;
		qualifier_pos++;
	}
	return true;
}

// Advance past whitespace and comments so a qualifier written behind either is still found.
static idx_t SkipIgnorableSpan(const string &sql, idx_t pos) {
	while (pos < sql.size()) {
		pos = SkipWhitespace(sql, pos);
		idx_t end;
		if (pos == sql.size() || (sql[pos] != '-' && sql[pos] != '/') || !TryReadSkippableSqlSpan(sql, pos, end)) {
			break;
		}
		pos = end;
	}
	return pos;
}

// Words that close a FROM list, so a comma past them separates something other than relations.
static bool EndsFromList(const string &token) {
	static const char *const TERMINATORS[] = {"where",  "group",     "having", "qualify", "window",    "order",
	                                          "limit",  "offset",    "union",  "except",  "intersect", "select",
	                                          "values", "returning", "set",    "insert",  "update",    "delete"};
	for (auto terminator : TERMINATORS) {
		if (StringUtil::CIEquals(token, terminator)) {
			return true;
		}
	}
	return false;
}

// Whether the parenthesis that ends at `pos` opens a derived table rather than a parenthesized join
// list. `(SELECT ...)`, `(WITH ...)`, `(VALUES ...)`, `(TABLE t)` and DuckDB's `(FROM t ...)` all
// start a query of their own; anything else in table position is a relation, and a nested
// parenthesis just defers the question one level.
static bool OpensDerivedTable(const string &sql, idx_t pos) {
	idx_t cursor = SkipIgnorableSpan(sql, pos);
	while (cursor < sql.size() && sql[cursor] == '(') {
		cursor = SkipIgnorableSpan(sql, cursor + 1);
	}
	idx_t token_end;
	string token;
	if (!TryReadIdentifierToken(sql, cursor, token_end, token)) {
		return false;
	}
	return StringUtil::CIEquals(token, "select") || StringUtil::CIEquals(token, "with") ||
	       StringUtil::CIEquals(token, "values") || StringUtil::CIEquals(token, "table") ||
	       StringUtil::CIEquals(token, "from");
}

string TimeTravelPins::RestoreIntoSql(const string &sql, SqlDialect dialect) const {
	if (pins.empty()) {
		return sql;
	}
	string result;
	result.reserve(sql.size());
	// Only a relation directly behind FROM, JOIN or a FROM-list comma is a scan; anything else naming
	// the relation is a column reference, a delta/metadata table or a literal, none of which take a
	// pin. Each parenthesis nests its own FROM list so a subquery never leaks the enclosing one.
	bool expect_relation = false;
	vector<bool> from_list_open;
	from_list_open.push_back(false);
	idx_t i = 0;
	while (i < sql.size()) {
		char c = sql[i];
		idx_t end;
		// Quoted identifiers must reach relation matching below; only literals/comments are skipped.
		if (c != '"' && TryReadSkippableSqlSpan(sql, i, end)) {
			result.append(sql, i, end - i);
			i = end;
			if (c == '\'') {
				expect_relation = false;
			}
			continue;
		}
		if (IsIdentStart(c) || c == '"' || c == '`') {
			string final_component;
			bool qualified;
			i = CopyQualifiedIdentifierChain(sql, i, result, final_component, qualified);
			if (expect_relation) {
				expect_relation = false;
				auto entry = pins.find(final_component);
				// PrepareViewQuerySources qualifies base scans in the parsed tree, not CTE references.
				// Plan-rendered scans already carry their pins, even with output qualification overrides.
				if (!qualified || entry == pins.end() || entry->second.binds_natively) {
					continue;
				}
				auto dialect_suffix = RenderSnapshotSuffix(entry->second.snapshot.get(), dialect);
				if (CarriesQualifierAt(sql, i, dialect_suffix)) {
					continue;
				}
				result += dialect_suffix;
				OPENIVM_DEBUG_PRINT("[TIME TRAVEL] Restored pin '%s' onto rendered scan of '%s'\n",
				                    dialect_suffix.c_str(), final_component.c_str());
				continue;
			}
			if (StringUtil::CIEquals(final_component, "from")) {
				expect_relation = true;
				from_list_open.back() = true;
			} else if (StringUtil::CIEquals(final_component, "join")) {
				expect_relation = true;
			} else if (EndsFromList(final_component)) {
				from_list_open.back() = false;
			}
			continue;
		}
		result += c;
		i++;
		if (c == '(') {
			// In table position a parenthesis opens either a derived table, which starts its own
			// query, or a parenthesized join list, whose first element is still a scan that needs its
			// pin. Only the query keywords tell the two apart; a table function's argument list never
			// reaches here because its own name already consumed the table position.
			bool table_list = expect_relation && !OpensDerivedTable(sql, i);
			from_list_open.push_back(table_list);
			expect_relation = table_list;
		} else if (c == ')') {
			if (from_list_open.size() > 1) {
				from_list_open.pop_back();
			}
			expect_relation = false;
		} else if (c == ',') {
			// An implicit cross join: the next relation is a scan of its own and needs its own pin.
			expect_relation = from_list_open.back();
		} else if (c == ';') {
			from_list_open.assign(1, false);
			expect_relation = false;
		} else if (!std::isspace(static_cast<unsigned char>(c))) {
			expect_relation = false;
		}
	}
	return result;
}

} // namespace openivm
} // namespace duckdb
