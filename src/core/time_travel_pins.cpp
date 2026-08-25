#include "core/time_travel_pins.hpp"

#include "core/openivm_debug.hpp"
#include "duckdb/catalog/catalog.hpp"
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
	// A CTE body is bound in the scope that precedes its own name, so walk the bodies first with
	// the incoming scope and only then extend the scope for the node that references them.
	for (auto &cte : node.cte_map.map) {
		if (cte.second->query && cte.second->query->node) {
			VisitQueryNode(*cte.second->query->node, visitor);
		}
	}
	RefVisitor scoped {visitor.callback, visitor.cte_names};
	for (auto &cte : node.cte_map.map) {
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
		if (CatalogHonoursPin(context, ref)) {
			return;
		}
		if (!ref.at_clause) {
			unpinned.insert(ref.table_name);
			return;
		}
		Pin pin;
		// INVALID_CATALOG / INVALID_SCHEMA are the empty string, so an unqualified reference already
		// stores the "" this map treats as "matches any qualifier".
		pin.catalog = ref.catalog_name;
		pin.schema = ref.schema_name;
		pin.suffix = " " + ref.at_clause->ToString();
		auto existing = result.pins.find(ref.table_name);
		if (existing != result.pins.end()) {
			if (existing->second.suffix != pin.suffix) {
				ThrowAmbiguousPin(ref.table_name, "it is pinned to both '" + existing->second.suffix.substr(1) +
				                                      "' and '" + pin.suffix.substr(1) + "'");
			}
			if (existing->second.catalog != pin.catalog || existing->second.schema != pin.schema) {
				ThrowAmbiguousPin(ref.table_name, "the same pin names two differently qualified relations");
			}
		}
		OPENIVM_DEBUG_PRINT("[TIME TRAVEL] Peeled pin '%s' off relation '%s'\n", pin.suffix.c_str(),
		                    ref.table_name.c_str());
		ref.at_clause.reset();
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

void TimeTravelPins::RestoreInto(AstNode &ast) const {
	if (pins.empty()) {
		return;
	}
	auto get_node = dynamic_cast<AstGetNode *>(&ast);
	if (get_node && get_node->table_name.find(" AT (") == string::npos) {
		auto entry = pins.find(get_node->table_name);
		if (entry != pins.end()) {
			auto &pin = entry->second;
			bool catalog_matches = pin.catalog.empty() || get_node->catalog.empty() ||
			                       StringUtil::CIEquals(pin.catalog, get_node->catalog);
			bool schema_matches =
			    pin.schema.empty() || get_node->schema.empty() || StringUtil::CIEquals(pin.schema, get_node->schema);
			if (catalog_matches && schema_matches) {
				get_node->table_name += pin.suffix;
			}
		}
	}
	for (auto &child : ast.children) {
		if (child) {
			RestoreInto(*child);
		}
	}
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
	string result;
	result.reserve(sql.size());
	string last_identifier;
	idx_t i = 0;
	while (i < sql.size()) {
		char c = sql[i];
		if (c == '\'') {
			i = CopyQuotedRun(sql, i, result);
			last_identifier.clear();
			continue;
		}
		if (c == '"' || c == '`') {
			idx_t start = result.size();
			i = CopyQuotedRun(sql, i, result);
			last_identifier = result.substr(start + 1, result.size() - start - 2);
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
		if (IsIdentifierStart(c)) {
			idx_t end = i;
			while (end < sql.size() && IsIdentifierPart(sql[end])) {
				end++;
			}
			auto token = sql.substr(i, end - i);
			idx_t after = end;
			while (after < sql.size() && std::isspace(static_cast<unsigned char>(sql[after]))) {
				after++;
			}
			if (StringUtil::CIEquals(token, "at") && after < sql.size() && sql[after] == '(' &&
			    pins.find(last_identifier) != pins.end()) {
				auto close = MatchingParen(sql, after);
				if (close != DConstants::INVALID_INDEX) {
					while (!result.empty() && std::isspace(static_cast<unsigned char>(result.back()))) {
						result.pop_back();
					}
					i = close;
					continue;
				}
			}
			result += token;
			last_identifier = token;
			i = end;
			continue;
		}
		result += c;
		if (c != '.' && !std::isspace(static_cast<unsigned char>(c))) {
			last_identifier.clear();
		}
		i++;
	}
	return result;
}

// Collapse runs of whitespace so two spellings of the same qualifier compare equal.
static string NormalizeQualifierText(const string &qualifier) {
	string normalized;
	normalized.reserve(qualifier.size());
	bool pending_space = false;
	for (auto c : qualifier) {
		if (std::isspace(static_cast<unsigned char>(c))) {
			pending_space = !normalized.empty();
			continue;
		}
		if (pending_space) {
			normalized += ' ';
			pending_space = false;
		}
		normalized += c;
	}
	return normalized;
}

// Words that may legally follow a table reference; none of them can be a bare alias.
static bool CanBeBareAlias(const string &token) {
	// DuckDB's `alias_clause` takes a `ColId`: plain identifiers plus the unreserved and column-name
	// keywords, but not the reserved or type/function ones. So `WHERE`, `JOIN` and `NATURAL` end the
	// relation instead of naming it.
	auto category = Parser::IsKeyword(StringUtil::Lower(token));
	return category != KeywordCategory::KEYWORD_RESERVED && category != KeywordCategory::KEYWORD_TYPE_FUNC;
}

// Map a Spark/Delta temporal keyword at `pos` to the DuckDB `AT (...)` parameter carrying the same
// snapshot. `VERSION`/`SYSTEM_VERSION` pin a commit version, `TIMESTAMP`/`SYSTEM_TIME` a point in time.
static bool ReadTemporalKeyword(const string &sql, idx_t pos, idx_t &end, string &at_parameter) {
	struct TemporalKeyword {
		const char *spelling;
		const char *at_parameter;
	};
	static const TemporalKeyword KEYWORDS[] = {{"system_version", "VERSION"},
	                                           {"version", "VERSION"},
	                                           {"system_time", "TIMESTAMP"},
	                                           {"timestamp", "TIMESTAMP"}};
	for (const auto &candidate : KEYWORDS) {
		if (MatchesKeywordAt(sql, pos, candidate.spelling)) {
			end = pos + strlen(candidate.spelling);
			at_parameter = candidate.at_parameter;
			return true;
		}
	}
	return false;
}

// Read a whole `[FOR] VERSION|TIMESTAMP AS OF <literal>` clause at `pos`, yielding the equivalent
// DuckDB qualifier. Requiring the full sequence keeps a column merely named `version` from matching.
static bool TryReadSourcePin(const string &sql, idx_t pos, idx_t &end, string &qualifier) {
	idx_t keyword_start = pos;
	if (MatchesKeywordAt(sql, pos, "for")) {
		keyword_start = SkipWhitespace(sql, pos + 3);
	}
	idx_t keyword_end;
	string at_parameter;
	if (!ReadTemporalKeyword(sql, keyword_start, keyword_end, at_parameter)) {
		return false;
	}
	idx_t as_pos = SkipWhitespace(sql, keyword_end);
	if (!MatchesKeywordAt(sql, as_pos, "as")) {
		return false;
	}
	idx_t of_pos = SkipWhitespace(sql, as_pos + 2);
	if (!MatchesKeywordAt(sql, of_pos, "of")) {
		return false;
	}
	idx_t value_start = SkipWhitespace(sql, of_pos + 2);
	idx_t value_end;
	string literal;
	string value_sql;
	if (TryReadSingleQuotedLiteral(sql, value_start, value_end, literal)) {
		value_sql = SingleQuotedSqlString(literal);
	} else if (!TryReadNumericToken(sql, value_start, value_end, value_sql)) {
		return false;
	}
	qualifier = "AT (" + at_parameter + " => " + value_sql + ")";
	end = value_end;
	return true;
}

// Read the `[AS] alias` the source dialect allows *after* a temporal clause, returning `pos`
// unchanged when the relation is unaliased and what follows just continues the query.
static idx_t ReadAliasAfterPin(const string &sql, idx_t pos, string &alias) {
	idx_t cursor = SkipWhitespace(sql, pos);
	bool explicit_as = MatchesKeywordAt(sql, cursor, "as");
	if (explicit_as) {
		cursor = SkipWhitespace(sql, cursor + 2);
	}
	if (cursor < sql.size() && (sql[cursor] == '"' || sql[cursor] == '`')) {
		string quoted;
		idx_t quoted_end = CopyQuotedRun(sql, cursor, quoted);
		alias = quoted.size() >= 2 ? quoted.substr(1, quoted.size() - 2) : quoted;
		return quoted_end;
	}
	idx_t token_end;
	string token;
	if (TryReadIdentifierToken(sql, cursor, token_end, token) && (explicit_as || CanBeBareAlias(token))) {
		alias = token;
		return token_end;
	}
	return pos;
}

vector<SnapshotBinding> CollectSourceSnapshotBindings(const string &sql, SqlDialect dialect) {
	vector<SnapshotBinding> bindings;
	if (dialect != SqlDialect::SPARK) {
		return bindings;
	}
	// The relation the next temporal clause would pin. Reset by anything that cannot be part of a
	// qualified relation name, so a pin is never credited to an unrelated identifier.
	string last_identifier;
	idx_t i = 0;
	while (i < sql.size()) {
		char c = sql[i];
		if (c == '\'') {
			string ignored;
			i = CopyQuotedRun(sql, i, ignored);
			last_identifier.clear();
			continue;
		}
		if (c == '"' || c == '`') {
			string quoted;
			i = CopyQuotedRun(sql, i, quoted);
			last_identifier = quoted.size() >= 2 ? quoted.substr(1, quoted.size() - 2) : quoted;
			continue;
		}
		if (c == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
			while (i < sql.size() && sql[i] != '\n') {
				i++;
			}
			continue;
		}
		if (c == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
			auto close = sql.find("*/", i + 2);
			i = close == string::npos ? sql.size() : close + 2;
			continue;
		}
		if (!IsIdentifierStart(c)) {
			if (c != '.' && !std::isspace(static_cast<unsigned char>(c))) {
				last_identifier.clear();
			}
			i++;
			continue;
		}
		idx_t pin_end;
		string qualifier;
		if (!TryReadSourcePin(sql, i, pin_end, qualifier)) {
			idx_t end = i;
			while (end < sql.size() && IsIdentifierPart(sql[end])) {
				end++;
			}
			last_identifier = sql.substr(i, end - i);
			i = end;
			continue;
		}
		string alias;
		idx_t alias_end = ReadAliasAfterPin(sql, pin_end, alias);
		// A keyword in front of the clause names no relation, so there is no association to assert;
		// the normalized text will fail to parse on its own.
		if (!last_identifier.empty() && CanBeBareAlias(last_identifier)) {
			bindings.push_back(SnapshotBinding {last_identifier, alias, qualifier});
			OPENIVM_DEBUG_PRINT("[TIME TRAVEL] Source pin '%s' on relation '%s' aliased '%s'\n", qualifier.c_str(),
			                    last_identifier.c_str(), alias.c_str());
		}
		last_identifier.clear();
		i = alias_end;
	}
	return bindings;
}

void VerifySnapshotBindings(SQLStatement &statement, const vector<SnapshotBinding> &bindings) {
	if (bindings.empty()) {
		return;
	}
	vector<bool> matched(bindings.size(), false);
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		if (!ref.at_clause) {
			return;
		}
		auto qualifier = ref.at_clause->ToString();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (matched[i]) {
				continue;
			}
			auto &binding = bindings[i];
			if (StringUtil::CIEquals(binding.relation, ref.table_name) &&
			    StringUtil::CIEquals(binding.alias, ref.alias) &&
			    StringUtil::CIEquals(NormalizeQualifierText(binding.qualifier), NormalizeQualifierText(qualifier))) {
				matched[i] = true;
				return;
			}
		}
	};
	RefVisitor visitor {callback, case_insensitive_set_t()};
	VisitStatement(statement, visitor);
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (!matched[i]) {
			auto &binding = bindings[i];
			throw NotImplementedException(
			    "OpenIVM lost the time-travel pin '%s' written on relation '%s' (alias '%s') while normalizing the "
			    "view body: no scan of that relation carries it after parsing. The source dialect writes the pin "
			    "between the relation and its alias and DuckDB wants it after both, so the clause is reordered "
			    "before parsing; compiling on would read a different snapshot.",
			    binding.qualifier, binding.relation, binding.alias);
		}
	}
}

} // namespace openivm
} // namespace duckdb
