#include "core/sql_utils.hpp"

#include "core/openivm_constants.hpp"
#include "duckdb.hpp"
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

#include <cctype>
#include <cstring>
#include <fstream>
#include <regex>
#include <sstream>

namespace duckdb {

// A `BaseTableRef` whose unqualified name matches a CTE visible at that point is a reference to
// the CTE, not a scan of a same-named relation, so it must never be treated as an unpinned scan.
struct SourceRefVisitor {
	const std::function<void(unique_ptr<TableRef> &)> &callback;
	case_insensitive_set_t cte_names;
};

static void SourceVisitQueryNode(QueryNode &node, const SourceRefVisitor &visitor);

static void SourceVisitExpression(ParsedExpression &expression, const SourceRefVisitor &visitor) {
	if (expression.GetExpressionClass() == ExpressionClass::SUBQUERY) {
		auto &subquery = expression.Cast<SubqueryExpression>();
		if (subquery.subquery && subquery.subquery->node) {
			SourceVisitQueryNode(*subquery.subquery->node, visitor);
		}
	}
	ParsedExpressionIterator::EnumerateChildren(
	    expression, [&](ParsedExpression &child) { SourceVisitExpression(child, visitor); });
}

static void SourceVisitTableRef(unique_ptr<TableRef> &ref, const SourceRefVisitor &visitor) {
	switch (ref->type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base_table = ref->Cast<BaseTableRef>();
		bool unqualified = base_table.catalog_name.empty() && base_table.schema_name.empty();
		if (unqualified && visitor.cte_names.find(base_table.table_name) != visitor.cte_names.end()) {
			break;
		}
		visitor.callback(ref);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref->Cast<JoinRef>();
		if (join.left) {
			SourceVisitTableRef(join.left, visitor);
		}
		if (join.right) {
			SourceVisitTableRef(join.right, visitor);
		}
		if (join.condition) {
			SourceVisitExpression(*join.condition, visitor);
		}
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subquery = ref->Cast<SubqueryRef>();
		if (subquery.subquery && subquery.subquery->node) {
			SourceVisitQueryNode(*subquery.subquery->node, visitor);
		}
		break;
	}
	case TableReferenceType::PIVOT: {
		auto &pivot = ref->Cast<PivotRef>();
		if (pivot.source) {
			SourceVisitTableRef(pivot.source, visitor);
		}
		for (auto &aggregate : pivot.aggregates) {
			SourceVisitExpression(*aggregate, visitor);
		}
		break;
	}
	case TableReferenceType::EXPRESSION_LIST:
	case TableReferenceType::TABLE_FUNCTION: {
		ParsedExpressionIterator::EnumerateTableRefChildren(*ref, [&](unique_ptr<ParsedExpression> &expression) {
			if (expression) {
				SourceVisitExpression(*expression, visitor);
			}
		});
		if (ref->type == TableReferenceType::TABLE_FUNCTION) {
			auto &function = ref->Cast<TableFunctionRef>();
			if (function.subquery && function.subquery->node) {
				SourceVisitQueryNode(*function.subquery->node, visitor);
			}
		}
		break;
	}
	default:
		break;
	}
}

static void SourceVisitQueryNode(QueryNode &node, const SourceRefVisitor &visitor) {
	// Each CTE body sees preceding siblings, but not its own name.
	SourceRefVisitor scoped {visitor.callback, visitor.cte_names};
	for (auto &cte : node.cte_map.map) {
		if (cte.second->query && cte.second->query->node) {
			SourceVisitQueryNode(*cte.second->query->node, scoped);
		}
		scoped.cte_names.insert(cte.first);
	}
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			SourceVisitTableRef(select.from_table, scoped);
		}
		auto visit = [&](unique_ptr<ParsedExpression> &expression) {
			if (expression) {
				SourceVisitExpression(*expression, scoped);
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
				SourceVisitQueryNode(*child, scoped);
			}
		}
		break;
	}
	case QueryNodeType::RECURSIVE_CTE_NODE: {
		auto &recursive_cte = node.Cast<RecursiveCTENode>();
		SourceRefVisitor recursive_scope {visitor.callback, scoped.cte_names};
		recursive_scope.cte_names.insert(recursive_cte.ctename);
		if (recursive_cte.left) {
			SourceVisitQueryNode(*recursive_cte.left, recursive_scope);
		}
		if (recursive_cte.right) {
			SourceVisitQueryNode(*recursive_cte.right, recursive_scope);
		}
		break;
	}
	case QueryNodeType::CTE_NODE: {
		auto &cte = node.Cast<CTENode>();
		if (cte.query) {
			SourceVisitQueryNode(*cte.query, scoped);
		}
		if (cte.child) {
			SourceRefVisitor child_scope {visitor.callback, scoped.cte_names};
			child_scope.cte_names.insert(cte.ctename);
			SourceVisitQueryNode(*cte.child, child_scope);
		}
		break;
	}
	default:
		break;
	}
	ParsedExpressionIterator::EnumerateQueryNodeModifiers(node, [&](unique_ptr<ParsedExpression> &expression) {
		if (expression) {
			SourceVisitExpression(*expression, scoped);
		}
	});
}

static void SourceVisitStatement(SQLStatement &statement, const SourceRefVisitor &visitor) {
	switch (statement.type) {
	case StatementType::SELECT_STATEMENT: {
		auto &select = statement.Cast<SelectStatement>();
		if (select.node) {
			SourceVisitQueryNode(*select.node, visitor);
		}
		break;
	}
	case StatementType::CREATE_STATEMENT: {
		auto &create = statement.Cast<CreateStatement>();
		if (create.info && create.info->type == CatalogType::TABLE_ENTRY) {
			auto &table_info = create.info->Cast<CreateTableInfo>();
			if (table_info.query && table_info.query->node) {
				SourceVisitQueryNode(*table_info.query->node, visitor);
			}
		}
		break;
	}
	case StatementType::INSERT_STATEMENT: {
		auto &insert = statement.Cast<InsertStatement>();
		if (insert.select_statement && insert.select_statement->node) {
			SourceVisitQueryNode(*insert.select_statement->node, visitor);
		}
		break;
	}
	default:
		break;
	}
}

void SqlUtils::RewriteSourceReferences(SQLStatement &statement,
                                       const std::function<void(unique_ptr<TableRef> &)> &callback) {
	SourceVisitStatement(statement, SourceRefVisitor {callback, {}});
}

void SqlUtils::VisitSourceReferences(SQLStatement &statement, const std::function<void(BaseTableRef &)> &callback) {
	RewriteSourceReferences(statement, [&](unique_ptr<TableRef> &ref) { callback(ref->Cast<BaseTableRef>()); });
}

static constexpr const char *CAST_EXPRESSION_PREFIX = "openivm_expr:";
static constexpr const char *CAST_COLUMN_PLACEHOLDER = "{openivm_column}";

static bool IsIdentifierChar(char c) {
	return static_cast<unsigned char>(c) >= 128 || std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '$';
}

template <class INDEX>
static void SkipWhitespace(const string &sql, INDEX &pos) {
	while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
		pos++;
	}
}

static bool StartsWithKeyword(const string &sql, size_t pos, const string &keyword) {
	if (pos + keyword.size() > sql.size()) {
		return false;
	}
	if (!StringUtil::CIEquals(sql.substr(pos, keyword.size()), keyword)) {
		return false;
	}
	return pos + keyword.size() == sql.size() || !IsIdentifierChar(sql[pos + keyword.size()]);
}

string SqlUtils::TrimSQLFragment(const string &input) {
	idx_t start = 0;
	while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
		start++;
	}
	idx_t end = input.size();
	while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
		end--;
	}
	return input.substr(start, end - start);
}

string SqlUtils::IdentifierEqualsSQL(const string &left, const string &right) {
	// SQL lower() folds Unicode, whereas StringUtil::CIEquals and catalog lookup
	// fold only A-Z. Preserve every other code point, including quoted identifiers.
	auto fold = [](const string &expression) {
		return "translate(" + expression + ", 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')";
	};
	// An application collation must not widen physical identifier equality either.
	return "(" + fold(left) + " COLLATE \"binary\" = " + fold(right) + " COLLATE \"binary\")";
}

string SqlUtils::BuildCastSpec(const string &target_type, bool try_cast) {
	if (!try_cast) {
		return target_type;
	}
	return string(CAST_EXPRESSION_PREFIX) + "TRY_CAST(" + CAST_COLUMN_PLACEHOLDER + " AS " + target_type + ")";
}

string SqlUtils::ApplyCastSpec(const string &column_expression, const string &cast_spec) {
	if (cast_spec.empty()) {
		return column_expression;
	}
	if (!StringUtil::StartsWith(cast_spec, CAST_EXPRESSION_PREFIX)) {
		return "CAST(" + column_expression + " AS " + cast_spec + ")";
	}
	string expression = cast_spec.substr(strlen(CAST_EXPRESSION_PREFIX));
	auto placeholder = expression.find(CAST_COLUMN_PLACEHOLDER);
	if (placeholder == string::npos) {
		throw InternalException("OpenIVM cast expression is missing its column placeholder");
	}
	expression.replace(placeholder, strlen(CAST_COLUMN_PLACEHOLDER), column_expression);
	return expression;
}

string SqlUtils::ComposeCastSpecs(const string &outer_cast_spec, const string &inner_cast_spec) {
	if (outer_cast_spec.empty()) {
		return inner_cast_spec;
	}
	if (inner_cast_spec.empty()) {
		return outer_cast_spec;
	}
	string inner_expression = ApplyCastSpec(CAST_COLUMN_PLACEHOLDER, inner_cast_spec);
	return string(CAST_EXPRESSION_PREFIX) + ApplyCastSpec(inner_expression, outer_cast_spec);
}

static bool ReadCreateTargetName(const string &sql, const string &object_keyword, string &out) {
	string lower = StringUtil::Lower(sql);
	size_t pos = lower.find("create");
	if (pos == string::npos) {
		return false;
	}
	pos += strlen("create");
	SkipWhitespace(sql, pos);
	if (StartsWithKeyword(lower, pos, "or")) {
		pos += strlen("or");
		SkipWhitespace(sql, pos);
		if (!StartsWithKeyword(lower, pos, "replace")) {
			return false;
		}
		pos += strlen("replace");
		SkipWhitespace(sql, pos);
	}
	if (object_keyword == "table") {
		if (!StartsWithKeyword(lower, pos, "table")) {
			return false;
		}
		pos += strlen("table");
	} else {
		if (StartsWithKeyword(lower, pos, "materialized")) {
			pos += strlen("materialized");
			SkipWhitespace(sql, pos);
		}
		if (!StartsWithKeyword(lower, pos, "view")) {
			return false;
		}
		pos += strlen("view");
	}
	SkipWhitespace(sql, pos);
	if (StartsWithKeyword(lower, pos, "if")) {
		pos += strlen("if");
		SkipWhitespace(sql, pos);
		if (!StartsWithKeyword(lower, pos, "not")) {
			return false;
		}
		pos += strlen("not");
		SkipWhitespace(sql, pos);
		if (!StartsWithKeyword(lower, pos, "exists")) {
			return false;
		}
		pos += strlen("exists");
		SkipWhitespace(sql, pos);
	}

	vector<string> parts;
	while (pos < sql.size()) {
		SkipWhitespace(sql, pos);
		string part;
		if (pos < sql.size() && sql[pos] == '"') {
			pos++;
			while (pos < sql.size()) {
				if (sql[pos] == '"') {
					if (pos + 1 < sql.size() && sql[pos + 1] == '"') {
						part += '"';
						pos += 2;
						continue;
					}
					pos++;
					break;
				}
				part += sql[pos++];
			}
		} else {
			while (pos < sql.size() && (IsIdentifierChar(sql[pos]) || sql[pos] == '-')) {
				part += sql[pos++];
			}
		}
		if (part.empty()) {
			break;
		}
		parts.push_back(part);
		SkipWhitespace(sql, pos);
		if (pos >= sql.size() || sql[pos] != '.') {
			break;
		}
		pos++;
	}
	if (parts.empty()) {
		return false;
	}
	out.clear();
	for (idx_t i = 0; i < parts.size(); i++) {
		if (i > 0) {
			out += ".";
		}
		out += SqlUtils::QuoteIdentifier(parts[i]);
	}
	return true;
}

struct IdentifierSegmentSpan {
	idx_t start;
	idx_t end;
	string value;
};

static bool ReadIdentifierSegmentSpan(const string &sql, idx_t &pos, IdentifierSegmentSpan &segment) {
	if (pos >= sql.size()) {
		return false;
	}
	segment.start = pos;
	segment.value.clear();
	if (sql[pos] == '"' || sql[pos] == '`') {
		auto quote = sql[pos++];
		while (pos < sql.size()) {
			if (sql[pos] == quote) {
				if (pos + 1 < sql.size() && sql[pos + 1] == quote) {
					segment.value += quote;
					pos += 2;
					continue;
				}
				pos++;
				segment.end = pos;
				return true;
			}
			segment.value += sql[pos++];
		}
		return false;
	}
	if (!(static_cast<unsigned char>(sql[pos]) >= 128 || std::isalpha(static_cast<unsigned char>(sql[pos])) ||
	      sql[pos] == '_')) {
		return false;
	}
	while (pos < sql.size() && IsIdentifierChar(sql[pos])) {
		segment.value += sql[pos++];
	}
	segment.end = pos;
	return true;
}

static bool ReadIdentifierSegment(const string &sql, idx_t &pos, string &segment) {
	IdentifierSegmentSpan span;
	if (!ReadIdentifierSegmentSpan(sql, pos, span)) {
		return false;
	}
	segment = std::move(span.value);
	return true;
}

vector<string> SqlUtils::ParseQualifiedIdentifier(const string &name) {
	vector<string> parts;
	idx_t pos = 0;
	while (true) {
		SkipWhitespace(name, pos);
		string part;
		if (!ReadIdentifierSegment(name, pos, part)) {
			throw ParserException("Invalid qualified identifier '%s'", name);
		}
		parts.push_back(std::move(part));
		SkipWhitespace(name, pos);
		if (pos == name.size()) {
			return parts;
		}
		if (name[pos++] != '.') {
			throw ParserException("Invalid qualified identifier '%s'", name);
		}
	}
}

static bool ReadQualifiedIdentifierSpans(const string &sql, idx_t start, idx_t &end,
                                         vector<IdentifierSegmentSpan> &segments) {
	idx_t pos = start;
	IdentifierSegmentSpan segment;
	if (!ReadIdentifierSegmentSpan(sql, pos, segment)) {
		return false;
	}
	segments.push_back(std::move(segment));
	while (pos < sql.size() && sql[pos] == '.') {
		idx_t next = pos + 1;
		IdentifierSegmentSpan next_segment;
		if (!ReadIdentifierSegmentSpan(sql, next, next_segment)) {
			break;
		}
		pos = next;
		segments.push_back(std::move(next_segment));
	}
	end = pos;
	return true;
}

vector<string> SqlUtils::ReadQualifiedIdentifier(const string &sql, idx_t start, idx_t &end) {
	vector<IdentifierSegmentSpan> spans;
	vector<string> parts;
	if (ReadQualifiedIdentifierSpans(sql, start, end, spans)) {
		for (auto &span : spans) {
			parts.push_back(std::move(span.value));
		}
	}
	return parts;
}

static bool QualifierMatches(const vector<IdentifierSegmentSpan> &segments, const unordered_set<string> &qualifiers) {
	if (segments.size() <= 1) {
		return false;
	}
	for (idx_t i = 0; i + 1 < segments.size(); i++) {
		if (qualifiers.count(StringUtil::Lower(segments[i].value))) {
			return true;
		}
	}
	return false;
}

static idx_t CopySingleQuotedLiteral(const string &sql, idx_t start, string &out) {
	idx_t i = start + 1;
	while (i < sql.size()) {
		if (sql[i] == '\'' && i + 1 < sql.size() && sql[i + 1] == '\'') {
			i += 2;
			continue;
		}
		if (sql[i++] == '\'') {
			break;
		}
	}
	out += sql.substr(start, i - start);
	return i;
}

static idx_t CopyLineComment(const string &sql, idx_t start, string &out) {
	idx_t i = start + 2;
	while (i < sql.size() && sql[i] != '\n') {
		i++;
	}
	if (i < sql.size()) {
		i++;
	}
	out += sql.substr(start, i - start);
	return i;
}

static idx_t CopyBlockComment(const string &sql, idx_t start, string &out) {
	idx_t i = start + 2;
	while (i + 1 < sql.size()) {
		if (sql[i] == '*' && sql[i + 1] == '/') {
			i += 2;
			out += sql.substr(start, i - start);
			return i;
		}
		i++;
	}
	out += sql.substr(start);
	return sql.size();
}

void SqlUtils::WriteFile(const string &filename, bool append, const string &compiled_query) {
	std::ofstream file;
	if (append) {
		file.open(filename, std::ios_base::app);
	} else {
		file.open(filename);
	}
	if (!file.is_open()) {
		throw IOException(
		    "OpenIVM could not open compiled SQL file '%s'. Create the directory set by "
		    "openivm_files_path and check write permissions; relative paths use the process working directory.",
		    filename);
	}
	file << compiled_query << '\n';
	file.close();
	if (file.fail()) {
		throw IOException("OpenIVM could not finish writing compiled SQL file '%s'. Check free space and permissions.",
		                  filename);
	}
}

string SqlUtils::ExtractTableName(const string &sql) {
	string name;
	if (ReadCreateTargetName(sql, "table", name)) {
		return name;
	}
	// Matches: CREATE TABLE [IF NOT EXISTS] name|"quoted name" (AS ...|(...))
	std::regex table_name_regex(
	    R"re(create\s+table\s+(?:if\s+not\s+exists\s+)?("(?:[^"]+)"|[a-zA-Z0-9_.]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))re");
	std::smatch match;
	if (std::regex_search(sql, match, table_name_regex)) {
		auto name = match[1].str();
		if (name.size() >= 2 && name.front() == '"' && name.back() == '"') {
			name = StringUtil::Replace(name.substr(1, name.size() - 2), "\"\"", "\"");
		}
		return name;
	}
	return "";
}

string SqlUtils::EscapeSingleQuotes(const string &input) {
	std::stringstream escaped_stream;
	for (char c : input) {
		if (c == '\'') {
			escaped_stream << "''";
		} else {
			escaped_stream << c;
		}
	}
	return escaped_stream.str();
}

void SqlUtils::ReplaceMaterializedView(string &query) {
	query = std::regex_replace(query, std::regex("^create\\s+materialized\\s+view\\b"), "create table if not exists");
	query = regex_replace(query, std::regex("\\s*;$"), "");
}

string SqlUtils::ExtractViewQuery(string &query) {
	string lower = StringUtil::Lower(query);
	size_t pos = lower.find("create");
	if (pos != string::npos) {
		pos += strlen("create");
		SkipWhitespace(query, pos);
		if (StartsWithKeyword(lower, pos, "or")) {
			pos += strlen("or");
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "replace")) {
				pos += strlen("replace");
				SkipWhitespace(query, pos);
			}
		}
		bool found_object = false;
		if (StartsWithKeyword(lower, pos, "table")) {
			pos += strlen("table");
			found_object = true;
		} else if (StartsWithKeyword(lower, pos, "materialized")) {
			pos += strlen("materialized");
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "view")) {
				pos += strlen("view");
				found_object = true;
			}
		}
		if (found_object) {
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "if")) {
				pos += strlen("if");
				SkipWhitespace(query, pos);
				if (StartsWithKeyword(lower, pos, "not")) {
					pos += strlen("not");
					SkipWhitespace(query, pos);
				}
				if (StartsWithKeyword(lower, pos, "exists")) {
					pos += strlen("exists");
					SkipWhitespace(query, pos);
				}
			}
			while (pos < query.size()) {
				SkipWhitespace(query, pos);
				if (pos < query.size() && query[pos] == '"') {
					pos++;
					while (pos < query.size()) {
						if (query[pos] == '"') {
							pos++;
							if (pos < query.size() && query[pos] == '"') {
								pos++;
								continue;
							}
							break;
						}
						pos++;
					}
				} else {
					while (pos < query.size() && (IsIdentifierChar(query[pos]) || query[pos] == '-')) {
						pos++;
					}
				}
				SkipWhitespace(query, pos);
				if (pos >= query.size() || query[pos] != '.') {
					break;
				}
				pos++;
			}
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "as")) {
				pos += strlen("as");
				return query.substr(pos);
			}
		}
	}
	// Match only up through "AS " — then return everything after as a raw substr so
	// multi-line CTE bodies (which contain '\n') are not truncated by regex '.' semantics.
	std::regex rgx_create_view(
	    R"re(create\s+(table|materialized view)\s+(?:if\s+not\s+exists\s+)?("(?:[^"]+)"|[a-zA-Z0-9_.]+)\s+as\s+)re");
	std::smatch match;
	if (std::regex_search(query, match, rgx_create_view)) {
		return query.substr(static_cast<size_t>(match.position()) + static_cast<size_t>(match.length()));
	}
	return "";
}

string SqlUtils::SQLToLowercase(const string &sql) {
	std::stringstream lowercase_stream;
	// Quoted identifiers carry physical catalog/schema identities. Folding them
	// can select a different metadata catalog even though DuckDB binds the name.
	char quote = '\0';
	for (char c : sql) {
		if (quote) {
			lowercase_stream << c;
			if (c == quote) {
				quote = '\0';
			}
		} else if (c == '\'' || c == '"') {
			quote = c;
			lowercase_stream << c;
		} else {
			lowercase_stream << StringUtil::CharacterToLower(c);
		}
	}
	return lowercase_stream.str();
}

void SqlUtils::RemoveRedundantWhitespaces(string &query) {
	query = std::regex_replace(query, std::regex("\\s+"), " ");
}

vector<string> SqlUtils::SplitSQLStatements(const string &sql) {
	vector<string> statements;
	idx_t start = 0;
	// Match DuckDB's own statement splitting, including dollar-quoted strings
	// and nested comments, rather than maintaining a second SQL lexer.
	for (auto &token : Parser::Tokenize(sql)) {
		if (token.type != SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR || sql[token.start] != ';') {
			continue;
		}
		auto statement = TrimSQLFragment(sql.substr(start, token.start - start));
		if (!statement.empty()) {
			statements.push_back(std::move(statement));
		}
		start = token.start + 1;
	}
	auto statement = TrimSQLFragment(sql.substr(start));
	if (!statement.empty()) {
		statements.push_back(std::move(statement));
	}
	return statements;
}

string SqlUtils::SQLStatementPreview(const string &statement) {
	auto trimmed = TrimSQLFragment(statement);
	string preview;
	bool in_space = false;
	for (auto c : trimmed) {
		if (std::isspace(static_cast<unsigned char>(c))) {
			if (!in_space && !preview.empty()) {
				preview += ' ';
				in_space = true;
			}
			continue;
		}
		preview += c;
		in_space = false;
		if (preview.size() >= 120) {
			preview += "...";
			break;
		}
	}
	return preview;
}

string SqlUtils::DeltaName(const string &name) {
	// Delta tables use the user-facing view name, not the internal data table name.
	// Strip openivm_data_ prefix if present (handles LPTS-generated SQL that references
	// the data table directly via VIEW expansion).
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	if (name.size() > data_prefix.size() && name.rfind(data_prefix, 0) == 0) {
		return string(openivm::DELTA_PREFIX) + name.substr(data_prefix.size());
	}
	return string(openivm::DELTA_PREFIX) + name;
}

string SqlUtils::LastIdentifierPart(string name) {
	StringUtil::Trim(name);
	idx_t dot_pos = DConstants::INVALID_INDEX;
	bool in_quote = false;
	for (idx_t i = 0; i < name.size(); i++) {
		if (name[i] == '"') {
			in_quote = !in_quote;
			continue;
		}
		if (!in_quote && name[i] == '.') {
			dot_pos = i;
		}
	}
	if (dot_pos != DConstants::INVALID_INDEX) {
		name = name.substr(dot_pos + 1);
	}
	if (name.size() >= 2 && name.front() == '"' && name.back() == '"') {
		name = StringUtil::Replace(name.substr(1, name.size() - 2), "\"\"", "\"");
	}
	return name;
}

string SqlUtils::FullName(const string &catalog, const string &schema, const string &table) {
	return QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + "." + QuoteIdentifier(table);
}

string SqlUtils::QualifiedPrefix(const string &catalog, const string &schema) {
	return QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + ".";
}

vector<string> SqlUtils::ParseColumnList(const string &raw) {
	vector<string> columns;
	string input = raw;
	StringUtil::Trim(input);
	if (input.empty()) {
		return columns;
	}
	if (input.front() == '[') {
		size_t pos = 1;
		while (pos < input.size()) {
			while (pos < input.size() && (std::isspace(static_cast<unsigned char>(input[pos])) || input[pos] == ',')) {
				pos++;
			}
			if (pos >= input.size() || input[pos] == ']') {
				break;
			}
			if (input[pos] != '"') {
				return {};
			}
			pos++;
			string value;
			while (pos < input.size()) {
				char c = input[pos++];
				if (c == '\\' && pos < input.size()) {
					char esc = input[pos++];
					value += (esc == 'n') ? '\n' : esc;
					continue;
				}
				if (c == '"') {
					columns.push_back(value);
					break;
				}
				value += c;
			}
		}
		return columns;
	}

	size_t start = 0;
	while (start <= input.size()) {
		size_t end = input.find(',', start);
		string value = input.substr(start, end == string::npos ? string::npos : end - start);
		StringUtil::Trim(value);
		if ((value.size() >= 2 && value.front() == '"' && value.back() == '"') ||
		    (value.size() >= 2 && value.front() == '\'' && value.back() == '\'')) {
			value = value.substr(1, value.size() - 2);
		}
		if (!value.empty()) {
			columns.push_back(value);
		}
		if (end == string::npos) {
			break;
		}
		start = end + 1;
	}
	return columns;
}

string SqlUtils::JsonQuote(const string &value) {
	string result = "\"";
	for (char c : value) {
		if (c == '"' || c == '\\') {
			result += '\\';
			result += c;
		} else if (c == '\n') {
			result += "\\n";
		} else {
			result += c;
		}
	}
	result += "\"";
	return result;
}

string SqlUtils::JsonArray(const vector<string> &values) {
	string result = "[";
	for (idx_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += JsonQuote(values[i]);
	}
	result += "]";
	return result;
}

string SqlUtils::DuckLakeTableFunction(const string &function_name, const string &catalog, const string &schema,
                                       const string &table, int64_t last_snapshot_id, int64_t current_snapshot_id) {
	return function_name + "('" + EscapeValue(catalog) + "', '" + EscapeValue(schema) + "', '" + EscapeValue(table) +
	       "', " + to_string(last_snapshot_id) + ", " + to_string(current_snapshot_id) + ")";
}

bool SqlUtils::IsDelta(const string &name) {
	static const string prefix(openivm::DELTA_PREFIX);
	return name.size() >= prefix.size() && name.rfind(prefix, 0) == 0;
}

string SqlUtils::JoinQuotedColumns(const vector<string> &columns) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += QuoteIdentifier(columns[i]);
	}
	return result;
}

string SqlUtils::JoinQualifiedQuotedColumns(const vector<string> &columns, const string &alias) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += alias + "." + QuoteIdentifier(columns[i]);
	}
	return result;
}

string SqlUtils::BuildAllNullPredicate(const vector<string> &columns) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += QuoteIdentifier(columns[i]) + " IS NULL";
	}
	return result;
}

string SqlUtils::BuildAnyNullPredicate(const vector<string> &columns, const string &prefix) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " OR ";
		}
		result += prefix + QuoteIdentifier(columns[i]) + " IS NULL";
	}
	return result;
}

string SqlUtils::BuildRowRepetitionSQL(const string &count, const string &alias, SqlDialect dialect) {
	auto quoted_alias = DialectQuoteIdent(alias, dialect);
	auto bound = "CAST(" + count + " AS BIGINT)";
	if (dialect == SqlDialect::SPARK) {
		// sequence(1, 0) descends instead of returning an empty array.
		return " LATERAL VIEW explode(CASE WHEN " + bound + " > 0 THEN sequence(CAST(1 AS BIGINT), " + bound +
		       ") ELSE array() END) " + quoted_alias + " AS `range`";
	}
	return " CROSS JOIN range(" + bound + ") " + quoted_alias;
}

string SqlUtils::BuildNullSafeMatch(const vector<string> &columns, const string &lhs_alias, const string &rhs_alias) {
	return BuildNullSafeKeyPredicate(columns, lhs_alias + ".", rhs_alias + ".");
}

string SqlUtils::BuildNullSafeKeyPredicate(const vector<string> &columns, const string &left_prefix,
                                           const string &right_prefix) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		auto column = QuoteIdentifier(columns[i]);
		result += left_prefix + column + " IS NOT DISTINCT FROM " + right_prefix + column;
	}
	return result;
}

string SqlUtils::BuildFullRecomputeSQL(const string &data_table, const string &view_query_sql,
                                       const vector<string> &unique_keys, const string &temp_table) {
	if (unique_keys.empty() || temp_table.empty()) {
		return "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
	}
	// Safe form for data tables carrying a UNIQUE index. `DELETE FROM t; INSERT INTO t ...` re-inserts
	// keys deleted earlier in the same transaction, and DuckDB's on-disk unique index still reports
	// those keys as present -- raising a spurious "Duplicate key ... violates unique constraint".
	// Materialize the new contents once, delete only the keys that are gone, then upsert the rest, so
	// no non-NULL key is deleted and re-inserted in the same transaction. NULL-containing keys do not
	// conflict in a UNIQUE index, so remove them explicitly before INSERT OR REPLACE to avoid duplicates.
	string keep_match = BuildNullSafeKeyPredicate(unique_keys, "openivm_new.", "openivm_old.");
	string nullable_key = BuildAnyNullPredicate(unique_keys, "openivm_old.");
	string sql = "CREATE OR REPLACE TEMP TABLE " + temp_table + " AS " + view_query_sql + ";\n";
	sql += "DELETE FROM " + data_table + " AS openivm_old\nWHERE (" + nullable_key +
	       ")\n   OR NOT EXISTS (\n  SELECT 1 FROM " + temp_table + " AS openivm_new WHERE " + keep_match + "\n);\n";
	sql += "INSERT OR REPLACE INTO " + data_table + " SELECT * FROM " + temp_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + temp_table + ";\n";
	return sql;
}

string SqlUtils::ReplaceAllOccurrences(string haystack, const string &needle, const string &replacement) {
	if (needle.empty()) {
		return haystack;
	}
	size_t pos = 0;
	while ((pos = haystack.find(needle, pos)) != string::npos) {
		haystack.replace(pos, needle.size(), replacement);
		pos += replacement.size();
	}
	return haystack;
}

vector<string> SqlUtils::ReplaceEachPlainOccurrence(const string &haystack, const string &needle,
                                                    const string &replacement) {
	vector<string> variants;
	if (needle.empty()) {
		return variants;
	}
	size_t pos = 0;
	while ((pos = haystack.find(needle, pos)) != string::npos) {
		string variant = haystack;
		variant.replace(pos, needle.size(), replacement);
		variants.push_back(std::move(variant));
		pos += needle.size();
	}
	return variants;
}

bool SqlUtils::RewriteColumnReferences(string &sql, const string &old_name, const string &new_name,
                                       const unordered_set<string> &qualifiers, bool allow_unqualified) {
	string rewritten;
	rewritten.reserve(sql.size());
	bool changed = false;
	for (idx_t i = 0; i < sql.size();) {
		if (sql[i] == '\'') {
			i = CopySingleQuotedLiteral(sql, i, rewritten);
			continue;
		}
		if (sql[i] == '-' && i + 1 < sql.size() && sql[i + 1] == '-') {
			i = CopyLineComment(sql, i, rewritten);
			continue;
		}
		if (sql[i] == '/' && i + 1 < sql.size() && sql[i + 1] == '*') {
			i = CopyBlockComment(sql, i, rewritten);
			continue;
		}
		if (sql[i] != '"' && !(std::isalpha(static_cast<unsigned char>(sql[i])) || sql[i] == '_')) {
			rewritten += sql[i++];
			continue;
		}

		idx_t end = i;
		vector<IdentifierSegmentSpan> segments;
		if (!ReadQualifiedIdentifierSpans(sql, i, end, segments)) {
			rewritten += sql[i++];
			continue;
		}
		auto &last = segments.back();
		bool replace = StringUtil::CIEquals(last.value, old_name) &&
		               ((segments.size() == 1 && allow_unqualified) || QualifierMatches(segments, qualifiers));
		if (!replace) {
			rewritten += sql.substr(i, end - i);
			i = end;
			continue;
		}
		rewritten += sql.substr(i, last.start - i);
		rewritten += QuoteIdentifier(new_name);
		rewritten += sql.substr(last.end, end - last.end);
		i = end;
		changed = true;
	}
	if (changed) {
		sql = std::move(rewritten);
	}
	return changed;
}

struct TableReferenceMatch {
	idx_t start;
	idx_t end;
};

static bool IsIdentifierTokenStart(char c) {
	return std::isalpha(static_cast<unsigned char>(c)) || c == '_' || c == '"';
}

static bool IsTableReferenceKeyword(const string &identifier) {
	return StringUtil::CIEquals(identifier, "from") || StringUtil::CIEquals(identifier, "join") ||
	       StringUtil::CIEquals(identifier, "update") || StringUtil::CIEquals(identifier, "into");
}

static idx_t ExtendPastTimeTravelClause(const string &sql, idx_t end) {
	idx_t at_pos = end;
	while (at_pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[at_pos]))) {
		at_pos++;
	}
	if (!StringUtil::CIStartsWith(sql.substr(at_pos), "AT (")) {
		return end;
	}
	idx_t depth = 0;
	while (at_pos < sql.size()) {
		if (sql[at_pos] == '(') {
			depth++;
		} else if (sql[at_pos] == ')') {
			depth--;
			if (depth == 0) {
				return at_pos + 1;
			}
		}
		at_pos++;
	}
	return end;
}

static vector<TableReferenceMatch> CollectTableReferenceMatches(const string &sql, const string &table_name,
                                                                bool include_time_travel_clause,
                                                                bool match_all = false) {
	vector<TableReferenceMatch> matches;
	if (table_name.empty() && !match_all) {
		return matches;
	}
	// Keep component boundaries: a quoted dot is part of a name, not qualification.
	auto target_parts = match_all ? vector<string>() : SqlUtils::ParseQualifiedIdentifier(table_name);
	auto matches_table = [&](const vector<IdentifierSegmentSpan> &parts) {
		if (parts.size() < target_parts.size()) {
			return false;
		}
		for (idx_t part = 0; part < target_parts.size(); part++) {
			if (!StringUtil::CIEquals(parts[parts.size() - target_parts.size() + part].value, target_parts[part])) {
				return false;
			}
		}
		return true;
	};
	bool in_single_quote = false;
	bool expect_table = false;
	for (idx_t i = 0; i < sql.size();) {
		char c = sql[i];
		if (c == '\'') {
			i++;
			if (in_single_quote && i < sql.size() && sql[i] == '\'') {
				i++;
				continue;
			}
			in_single_quote = !in_single_quote;
			continue;
		}
		if (!in_single_quote && IsIdentifierTokenStart(c)) {
			idx_t start = i;
			idx_t end = i;
			vector<IdentifierSegmentSpan> parts;
			if (ReadQualifiedIdentifierSpans(sql, start, end, parts)) {
				if (expect_table && matches_table(parts)) {
					matches.push_back({start, include_time_travel_clause ? ExtendPastTimeTravelClause(sql, end) : end});
					i = matches.back().end;
					expect_table = false;
					continue;
				}
				i = end;
				expect_table = parts.size() == 1 && sql[start] != '"' && IsTableReferenceKeyword(parts[0].value);
				continue;
			}
		}
		if (!std::isspace(static_cast<unsigned char>(c))) {
			expect_table = false;
		}
		i++;
	}
	return matches;
}

string SqlUtils::RewriteTableReferences(const string &sql,
                                        const std::function<string(const vector<string> &)> &rewrite) {
	string result;
	idx_t previous = 0;
	for (auto &match : CollectTableReferenceMatches(sql, "", false, true)) {
		auto replacement = rewrite(ParseQualifiedIdentifier(sql.substr(match.start, match.end - match.start)));
		if (!replacement.empty()) {
			result.append(sql, previous, match.start - previous);
			result += replacement;
			previous = match.end;
		}
	}
	if (previous == 0) {
		return sql;
	}
	result.append(sql, previous, sql.size() - previous);
	return result;
}

string SqlUtils::FindTableReference(const string &sql, const string &table_name) {
	auto matches = CollectTableReferenceMatches(sql, table_name, false);
	if (matches.empty()) {
		return "";
	}
	auto &match = matches[0];
	return sql.substr(match.start, match.end - match.start);
}

string SqlUtils::FindTableReferenceOccurrence(const string &sql, const string &table_name, idx_t occurrence) {
	auto matches = CollectTableReferenceMatches(sql, table_name, false);
	if (occurrence >= matches.size()) {
		return "";
	}
	auto &match = matches[occurrence];
	return sql.substr(match.start, match.end - match.start);
}

idx_t SqlUtils::CountTableReferences(const string &sql, const string &table_name) {
	return CollectTableReferenceMatches(sql, table_name, false).size();
}

string SqlUtils::ReplaceTableReferences(const string &sql, const string &table_name, const string &replacement) {
	auto matches = CollectTableReferenceMatches(sql, table_name, true);
	if (matches.empty()) {
		return sql;
	}
	string result;
	result.reserve(sql.size());
	idx_t last = 0;
	for (auto &match : matches) {
		result += sql.substr(last, match.start - last);
		result += replacement;
		last = match.end;
	}
	result += sql.substr(last);
	return result;
}

string SqlUtils::ReplaceTableReferenceOccurrence(const string &sql, const string &table_name, idx_t occurrence,
                                                 const string &replacement, bool &replaced) {
	replaced = false;
	auto matches = CollectTableReferenceMatches(sql, table_name, true);
	if (occurrence >= matches.size()) {
		return sql;
	}
	auto &match = matches[occurrence];
	string result;
	result.reserve(sql.size());
	result += sql.substr(0, match.start);
	result += replacement;
	result += sql.substr(match.end);
	replaced = true;
	return result;
}

vector<string> SqlUtils::ReplaceEachTableReference(const string &sql, const string &table_name,
                                                   const string &replacement) {
	vector<string> variants;
	for (auto &match : CollectTableReferenceMatches(sql, table_name, false)) {
		string variant = sql;
		variant.replace(match.start, match.end - match.start, replacement);
		variants.push_back(std::move(variant));
	}
	return variants;
}

int64_t SqlUtils::ParseRefreshInterval(const string &interval_str) {
	std::regex interval_re(R"((\d+)\s*(minute|minutes|min|hour|hours|day|days))", std::regex::icase);
	std::smatch match;
	if (!std::regex_match(interval_str, match, interval_re)) {
		throw ParserException("Invalid REFRESH EVERY interval: '" + interval_str +
		                      "'. Expected format: '<N> <minutes|hours|days>'");
	}
	int64_t value = std::stoll(match[1].str());
	if (value <= 0) {
		throw ParserException("REFRESH EVERY interval must be a positive number (got " + to_string(value) + ")");
	}
	string unit = StringUtil::Lower(match[2].str());
	int64_t seconds;
	if (unit == "minute" || unit == "minutes" || unit == "min") {
		seconds = value * 60;
	} else if (unit == "hour" || unit == "hours") {
		seconds = value * 3600;
	} else if (unit == "day" || unit == "days") {
		seconds = value * 86400;
	} else {
		throw ParserException("Unknown time unit: '" + unit + "'");
	}
	return seconds;
}

int64_t SqlUtils::ExtractRefreshInterval(string &query) {
	std::regex refresh_re(R"(refresh\s+every\s+'([^']+)')", std::regex::icase);
	std::smatch match;
	if (!std::regex_search(query, match, refresh_re)) {
		return -1;
	}
	string interval_str = match[1].str();
	// Strip the REFRESH EVERY clause from the query
	query = std::regex_replace(query, refresh_re, "");
	return ParseRefreshInterval(interval_str);
}

} // namespace duckdb
