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
#include "lpts_helpers.hpp"
#include "lpts_sql_scanner.hpp"

#include <cstring>
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

// Whether the catalog backing `ref` implements time travel, so its pin binds natively and must be
// left in place. An unresolvable relation is left alone as well: DuckDB owns that error message.
static bool CatalogHonoursPin(ClientContext &context, BaseTableRef &ref) {
	QueryErrorContext error_context;
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, ref.table_name, error_context);
	optional_ptr<CatalogEntry> entry;
	try {
		entry = Catalog::GetEntry(context, ref.catalog_name, ref.schema_name, lookup, OnEntryNotFound::RETURN_NULL);
	} catch (const std::exception &) {
		return true;
	}
	if (!entry) {
		return true;
	}
	return entry->ParentCatalog().SupportsTimeTravel();
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

TimeTravelPins TimeTravelPins::Peel(ClientContext &context, SQLStatement &statement) {
	TimeTravelPins result;
	case_insensitive_set_t unpinned;
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		if (!ref.at_clause) {
			// Every unpinned scan is recorded, including one in a catalog that honours pins
			// natively: re-attachment is keyed by relation name and an unqualified pin matches any
			// catalog, so a pin peeled off one relation would otherwise land on a same-named
			// relation that was deliberately read unpinned.
			unpinned.insert(ref.table_name);
			return;
		}
		if (CatalogHonoursPin(context, ref)) {
			return;
		}
		Pin pin;
		// INVALID_CATALOG / INVALID_SCHEMA are the empty string, so an unqualified reference already
		// stores the "" this map treats as "matches any qualifier".
		pin.catalog = ref.catalog_name;
		pin.schema = ref.schema_name;
		pin.snapshot = std::move(ref.at_clause);
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
		OPENIVM_DEBUG_PRINT("[TIME TRAVEL] Peeled pin '%s' off relation '%s'\n", pin.snapshot->ToString().c_str(),
		                    ref.table_name.c_str());
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

void TimeTravelPins::PeelForLocalBinding(ClientContext &context, SQLStatement &statement) {
	Peel(context, statement);
}

TimeTravelPins TimeTravelPins::FromViewSql(ClientContext &context, const string &view_query_sql) {
	Parser parser(context.GetParserOptions());
	parser.ParseQuery(view_query_sql);
	if (parser.statements.empty()) {
		return TimeTravelPins();
	}
	return Peel(context, *parser.statements[0]);
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

static bool IsIdentifierStart(char c) {
	return std::isalpha(static_cast<unsigned char>(c)) || c == '_';
}

static bool IsIdentifierPart(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '$';
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

// Index just past the `)` matching the `(` at `sql[open]`, skipping quoted runs.
static idx_t MatchingParen(const string &sql, idx_t open) {
	idx_t depth = 0;
	for (idx_t i = open; i < sql.size(); i++) {
		char c = sql[i];
		if (c == '\'' || c == '"' || c == '`') {
			string ignored;
			i = CopyQuotedRun(sql, i, ignored) - 1;
			continue;
		}
		if (c == '(') {
			depth++;
		} else if (c == ')') {
			depth--;
			if (depth == 0) {
				return i + 1;
			}
		}
	}
	return DConstants::INVALID_INDEX;
}

string TimeTravelPins::StripFrom(const string &sql) const {
	if (pins.empty()) {
		return sql;
	}
	Parser parser;
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("Expected one view query while stripping time-travel pins");
	}
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		auto entry = pins.find(ref.table_name);
		if (entry == pins.end()) {
			return;
		}
		auto &pin = entry->second;
		if ((pin.catalog.empty() || StringUtil::CIEquals(pin.catalog, ref.catalog_name)) &&
		    (pin.schema.empty() || StringUtil::CIEquals(pin.schema, ref.schema_name))) {
			ref.at_clause.reset();
		}
	};
	VisitStatement(*parser.statements[0], RefVisitor {callback, {}});
	return parser.statements[0]->ToString();
}

// Words that may legally follow a table reference; none of them can be a bare alias.
static bool CanBeBareAlias(const string &token) {
	// DuckDB's `alias_clause` takes a `ColId`: plain identifiers plus the unreserved and column-name
	// keywords, but not the reserved or type/function ones. So `WHERE`, `JOIN` and `NATURAL` end the
	// relation instead of naming it.
	auto category = Parser::IsKeyword(StringUtil::Lower(token));
	return category != KeywordCategory::KEYWORD_RESERVED && category != KeywordCategory::KEYWORD_TYPE_FUNC;
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
		} else if (i < sql.size() && IsIdentifierStart(sql[i])) {
			idx_t end = i;
			while (end < sql.size() && IsIdentifierPart(sql[end])) {
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
		if (std::isspace(static_cast<unsigned char>(sql[pos]))) {
			pos++;
			continue;
		}
		if (sql[pos] == '-' && pos + 1 < sql.size() && sql[pos + 1] == '-') {
			auto newline = sql.find('\n', pos);
			pos = newline == string::npos ? sql.size() : newline + 1;
			continue;
		}
		if (sql[pos] == '/' && pos + 1 < sql.size() && sql[pos + 1] == '*') {
			auto close = sql.find("*/", pos + 2);
			pos = close == string::npos ? sql.size() : close + 2;
			continue;
		}
		break;
	}
	return pos;
}

// Read a raw DuckDB `AT (...)` qualifier belonging to the relation that ends at `pos`. Bodies that
// never reach the AST keep their pin in normalized text, where DuckDB spells it *after* the alias
// (`t AS p AT (VERSION => 366)`); every other dialect wants it directly behind the relation. The
// alias text in between is handed back verbatim so it can be re-emitted after the translated pin,
// and `end` reports where the raw clause stops so the caller drops it instead of keeping both.
static bool TryReadRawPinAfterRelation(const string &sql, idx_t pos, string &alias_text, idx_t &end) {
	string buffer;
	idx_t cursor = pos;
	// `[AS] alias` is at most two tokens, so the qualifier has to appear within three.
	for (idx_t token_index = 0; token_index < 3; token_index++) {
		idx_t token_start = SkipIgnorableSpan(sql, cursor);
		idx_t token_end;
		string token;
		if (token_start < sql.size() && (sql[token_start] == '"' || sql[token_start] == '`')) {
			token_end = CopyQuotedRun(sql, token_start, token);
		} else if (!TryReadIdentifierToken(sql, token_start, token_end, token)) {
			return false;
		}
		if (StringUtil::CIEquals(token, "at")) {
			idx_t paren = SkipIgnorableSpan(sql, token_end);
			if (paren >= sql.size() || sql[paren] != '(') {
				return false;
			}
			auto close = MatchingParen(sql, paren);
			if (close == DConstants::INVALID_INDEX) {
				return false;
			}
			alias_text = buffer;
			end = close;
			return true;
		}
		if (!StringUtil::CIEquals(token, "as") && !CanBeBareAlias(token)) {
			return false;
		}
		buffer.append(sql, cursor, token_end - cursor);
		cursor = token_end;
	}
	return false;
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

// Names a `WITH` clause binds in `sql`. A CTE reference is a name, not a scan, so it must never be
// handed a snapshot qualifier even when it shadows a pinned relation.
static case_insensitive_set_t CollectCteNames(const string &sql) {
	case_insensitive_set_t names;
	string candidate;
	idx_t i = 0;
	while (i < sql.size()) {
		char c = sql[i];
		if (c == '\'') {
			string ignored;
			i = CopyQuotedRun(sql, i, ignored);
			candidate.clear();
			continue;
		}
		if (c == '"' || c == '`') {
			string quoted;
			i = CopyQuotedRun(sql, i, quoted);
			candidate = quoted.size() >= 2 ? quoted.substr(1, quoted.size() - 2) : quoted;
			continue;
		}
		if (IsIdentifierStart(c)) {
			idx_t end = i;
			while (end < sql.size() && IsIdentifierPart(sql[end])) {
				end++;
			}
			auto token = sql.substr(i, end - i);
			i = end;
			if (!StringUtil::CIEquals(token, "as")) {
				candidate = token;
				continue;
			}
			// `name AS (`, `name (columns) AS (` and `name AS [NOT] MATERIALIZED (` all define a CTE;
			// nothing else puts a parenthesis directly behind `AS`.
			idx_t cursor = SkipIgnorableSpan(sql, i);
			for (idx_t modifiers = 0; modifiers < 2; modifiers++) {
				idx_t keyword_end;
				string keyword;
				if (!TryReadIdentifierToken(sql, cursor, keyword_end, keyword) ||
				    (!StringUtil::CIEquals(keyword, "not") && !StringUtil::CIEquals(keyword, "materialized"))) {
					break;
				}
				cursor = SkipIgnorableSpan(sql, keyword_end);
			}
			if (cursor < sql.size() && sql[cursor] == '(' && !candidate.empty()) {
				names.insert(candidate);
			}
			continue;
		}
		// A column-alias list sits between the CTE name and its `AS`, so parentheses keep the name.
		if (c != '(' && c != ')' && !std::isspace(static_cast<unsigned char>(c))) {
			candidate.clear();
		}
		i++;
	}
	return names;
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
	auto cte_names = CollectCteNames(sql);
	idx_t i = 0;
	while (i < sql.size()) {
		char c = sql[i];
		if (c == '\'') {
			i = CopyQuotedRun(sql, i, result);
			expect_relation = false;
			continue;
		}
		if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
			while (i < sql.size() && sql[i] != '\n') {
				result += sql[i++];
			}
			continue;
		}
		if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
			auto close = sql.find("*/", i + 2);
			auto end = close == string::npos ? sql.size() : close + 2;
			result.append(sql, i, end - i);
			i = end;
			continue;
		}
		if (IsIdentifierStart(c) || c == '"' || c == '`') {
			string final_component;
			bool qualified;
			i = CopyQualifiedIdentifierChain(sql, i, result, final_component, qualified);
			if (expect_relation) {
				expect_relation = false;
				auto entry = pins.find(final_component);
				if (entry == pins.end()) {
					continue;
				}
				auto dialect_suffix = RenderSnapshotSuffix(entry->second.snapshot.get(), dialect);
				if (CarriesQualifierAt(sql, i, dialect_suffix)) {
					continue;
				}
				string alias_text;
				idx_t raw_pin_end;
				bool carries_raw_pin = TryReadRawPinAfterRelation(sql, i, alias_text, raw_pin_end);
				if (!carries_raw_pin && !qualified && cte_names.find(final_component) != cte_names.end()) {
					// A bare name this query itself binds: the scan it stands for was pinned where the
					// CTE was defined, and only a real relation can carry a qualifier.
					continue;
				}
				if (carries_raw_pin) {
					i = raw_pin_end;
				}
				result += dialect_suffix;
				result += alias_text;
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
