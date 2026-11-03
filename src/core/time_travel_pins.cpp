#include "core/time_travel_pins.hpp"

#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/binder.hpp"
#include "lpts_helpers.hpp"
#include "lpts_sql_scanner.hpp"

#include <cctype>
#include <map>

namespace duckdb {
namespace openivm {

using BaseTableRefCallback = std::function<void(BaseTableRef &)>;

// Resolve without the AT clause; DuckDB owns lookup errors for unresolved relations.
static optional_ptr<CatalogEntry> LookupPinRelation(ClientContext &context, BaseTableRef &ref) {
	QueryErrorContext error_context;
	EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, ref.Table(), error_context);
	try {
		auto catalog = ref.GetQualifiedName().Catalog();
		auto schema = ref.GetQualifiedName().Schema();
		Binder::BindSchemaOrCatalog(context, catalog, schema);
		return Catalog::GetEntry(context, Identifier(catalog), Identifier(schema), lookup,
		                         OnEntryNotFound::RETURN_NULL);
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
			unpinned.insert(ref.Table().GetIdentifierName());
			return;
		}
		Pin pin;
		pin.catalog = ref.GetQualifiedName().Catalog().GetIdentifierName();
		pin.schema = ref.GetQualifiedName().Schema().GetIdentifierName();
		auto relation = LookupPinRelation(context, ref);
		pin.binds_natively = !relation || relation->ParentCatalog().SupportsTimeTravel();
		if (relation) {
			pin.catalog = relation->ParentCatalog().GetName().GetIdentifierName();
			pin.schema = relation->ParentSchema().name.GetIdentifierName();
		}
		// Binding loses the explicit AT clause even when it names the current native snapshot.
		// Preserve it for LPTS, but only remove foreign pins from the locally executable query.
		pin.snapshot = pin.binds_natively ? ref.at_clause->Copy() : std::move(ref.at_clause);
		auto existing = result.pins.find(ref.Table().GetIdentifierName());
		if (existing != result.pins.end()) {
			if (!AtClause::Equals(existing->second.snapshot, pin.snapshot)) {
				ThrowAmbiguousPin(ref.Table().GetIdentifierName(), "it is pinned to both '" +
				                                                       existing->second.snapshot->ToString() +
				                                                       "' and '" + pin.snapshot->ToString() + "'");
			}
			if (existing->second.catalog != pin.catalog || existing->second.schema != pin.schema) {
				ThrowAmbiguousPin(ref.Table().GetIdentifierName(),
				                  "the same pin names two differently qualified relations");
			}
		}
		OPENIVM_DEBUG_PRINT("[TIME TRAVEL] Recorded pin '%s' on relation '%s' (native=%d)\n",
		                    pin.snapshot->ToString().c_str(), ref.Table().GetIdentifierName().c_str(),
		                    pin.binds_natively);
		result.pins[ref.Table().GetIdentifierName()] = std::move(pin);
	};
	SqlUtils::VisitSourceReferences(statement, callback);
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
		auto entry = pins.find(table.name.GetIdentifierName());
		if (entry == pins.end()) {
			return nullptr;
		}
		auto &pin = entry->second;
		if ((!pin.catalog.empty() &&
		     !StringUtil::CIEquals(pin.catalog, table.ParentCatalog().GetName().GetIdentifierName())) ||
		    (!pin.schema.empty() && !StringUtil::CIEquals(pin.schema, table.schema.name.GetIdentifierName()))) {
			return nullptr;
		}
		return pin.snapshot->Copy();
	};
}

string TimeTravelPins::StripFrom(ClientContext &context, const string &sql) const {
	if (pins.empty()) {
		return sql;
	}
	Parser parser(context);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("Expected one view query while stripping time-travel pins");
	}
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		auto entry = pins.find(ref.Table().GetIdentifierName());
		if (entry == pins.end() || entry->second.binds_natively) {
			return;
		}
		auto &pin = entry->second;
		auto relation = LookupPinRelation(context, ref);
		if (relation && StringUtil::CIEquals(pin.catalog, relation->ParentCatalog().GetName().GetIdentifierName()) &&
		    StringUtil::CIEquals(pin.schema, relation->ParentSchema().name.GetIdentifierName())) {
			ref.at_clause.reset();
		}
	};
	SqlUtils::VisitSourceReferences(*parser.statements[0], callback);
	return parser.statements[0]->ToString();
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

string TimeTravelPins::QualifySources(ClientContext &context, const string &sql) {
	Parser parser(context);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1) {
		throw InternalException("Expected one view query while qualifying sources");
	}
	// Parsed locations identify individual references, including repeated names and public MVs
	// whose bound scans name an internal publication table. Keep raw SQL rather than serializing
	// the query node: original-SQL fallback must also work for unsupported plan shapes.
	std::map<idx_t, std::pair<idx_t, string>> replacements;
	BaseTableRefCallback callback = [&](BaseTableRef &ref) {
		auto relation = LookupPinRelation(context, ref);
		if (!relation) {
			return; // Let normal binding report unresolved relations and handle replacement scans.
		}
		if (!ref.query_location.IsValid()) {
			throw InternalException("Missing source location while qualifying '%s'", ref.Table().GetIdentifierName());
		}
		auto start = ref.query_location.Start();
		auto end = start;
		idx_t components = 1 + !ref.GetQualifiedName().Schema().GetIdentifierName().empty() +
		                   !ref.GetQualifiedName().Catalog().GetIdentifierName().empty();
		for (idx_t i = 0; i < components; i++) {
			end = SkipIgnorableSpan(sql, end);
			idx_t quoted_end;
			if (end < sql.size() && sql[end] == '"') {
				end = TryReadSkippableSqlSpan(sql, end, quoted_end) ? quoted_end : sql.size();
			} else {
				while (end < sql.size() && !std::isspace(static_cast<unsigned char>(sql[end])) && sql[end] != '.' &&
				       sql[end] != ',' && sql[end] != ')' && sql[end] != ';' && sql[end] != '/' && sql[end] != '-') {
					end++;
				}
			}
			if (i + 1 < components) {
				end = SkipIgnorableSpan(sql, end);
				if (end >= sql.size() || sql[end] != '.') {
					throw InternalException("Invalid source span while qualifying '%s'",
					                        ref.Table().GetIdentifierName());
				}
				end++;
			}
		}
		auto qualified =
		    SqlUtils::FullName(relation->ParentCatalog().GetName(), relation->ParentSchema().name, relation->name);
		replacements[start] = {end, qualified};
		OPENIVM_DEBUG_PRINT("[CREATE MV] Original SQL source '%s' resolves to %s\n",
		                    ref.Table().GetIdentifierName().c_str(), qualified.c_str());
	};
	SqlUtils::VisitSourceReferences(*parser.statements[0], callback);
	string result = sql;
	for (auto it = replacements.rbegin(); it != replacements.rend(); ++it) {
		result.replace(it->first, it->second.first - it->first, it->second.second);
	}
	return result;
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
			// The relation name pins are keyed by, and whether a catalog/schema prefix (which a
			// CTE reference never carries) qualifies it.
			idx_t chain_end;
			auto parts = SqlUtils::ReadQualifiedIdentifier(sql, i, chain_end);
			if (parts.empty()) {
				// An unterminated quoted identifier runs to the end.
				result.append(sql, i, string::npos);
				break;
			}
			result.append(sql, i, chain_end - i);
			i = chain_end;
			const auto &final_component = parts.back();
			bool qualified = parts.size() > 1;
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
