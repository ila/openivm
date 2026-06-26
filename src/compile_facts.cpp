#include "compile_facts.hpp"

#include "core/openivm_constants.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "upsert/refresh_internal.hpp"

#include <cctype>
#include <cstdlib>
#include <stdexcept>

namespace duckdb {
namespace openivm {

//------------------------------------------------------------------------------
// CompileFacts: defaults
//------------------------------------------------------------------------------

CompileFacts CompileFacts::Default(SqlDialect dialect) {
	CompileFacts out;
	out.target_dialect = dialect;
	return out;
}

//------------------------------------------------------------------------------
// Minimal handrolled JSON parser
//
// Follows the substring-based pattern openivm already uses for its own
// metadata JSON (see the anonymous helpers in core/refresh_metadata.cpp).
// The CompileFacts JSON wire form is closed-loop — both writers (openivm's
// own test fixtures and the openivm-spark driver) emit canonical form with
// no extra whitespace and only the escape characters openivm itself
// produces. The current facts surface only needs scalar strings, booleans,
// and integers; a full JSON parser is unnecessary, and adding yyjson / the
// DuckDB JSON extension would pull in an optional transitive dependency.
//
// These helpers are file-scoped on purpose — they assume the closed-loop
// invariants above and are not safe for general-purpose JSON.
//------------------------------------------------------------------------------

namespace {

bool ExtractJsonString(const string &json, const string &key, string &val) {
	string needle = "\"" + key + "\":\"";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	val.clear();
	while (pos < json.size()) {
		char c = json[pos];
		if (c == '\\' && pos + 1 < json.size()) {
			char esc = json[pos + 1];
			if (esc == 'n') {
				val += '\n';
			} else {
				val += esc;
			}
			pos += 2;
			continue;
		}
		if (c == '"') {
			return true;
		}
		val += c;
		pos++;
	}
	return false;
}

bool ExtractJsonBool(const string &json, const string &key, bool &val) {
	string needle = "\"" + key + "\":";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
		pos++;
	}
	if (pos + 4 <= json.size() && json.compare(pos, 4, "true") == 0) {
		val = true;
		return true;
	}
	if (pos + 5 <= json.size() && json.compare(pos, 5, "false") == 0) {
		val = false;
		return true;
	}
	return false;
}

bool ExtractJsonInt(const string &json, const string &key, int &val) {
	string needle = "\"" + key + "\":";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
		pos++;
	}
	size_t start = pos;
	if (pos < json.size() && (json[pos] == '-' || json[pos] == '+')) {
		pos++;
	}
	while (pos < json.size() && std::isdigit(static_cast<unsigned char>(json[pos]))) {
		pos++;
	}
	if (start == pos) {
		return false;
	}
	try {
		val = std::stoi(json.substr(start, pos - start));
		return true;
	} catch (...) {
		return false;
	}
}

// Extract a nested object's `{...}` substring (braces included) for key. Brace-matched;
// relies on the same closed-loop invariant as the scalar helpers (no '{'/'}' inside string
// values on our wire). Returns false if the "key":{ needle is absent.
bool ExtractJsonObjectBody(const string &json, const string &key, string &out_body) {
	string needle = "\"" + key + "\":{";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	size_t body_start = pos + needle.size() - 1; // points at '{'
	int depth = 0;
	for (size_t i = body_start; i < json.size(); i++) {
		char c = json[i];
		if (c == '{') {
			depth++;
		} else if (c == '}') {
			depth--;
			if (depth == 0) {
				out_body = json.substr(body_start, i - body_start + 1);
				return true;
			}
		}
	}
	return false;
}

// Parse a JSON array of strings: "key":[ "a", "b" ]. Empty [] returns true with empty out.
// Only string elements are supported (sufficient for the closed-loop wire). Escape handling
// mirrors ExtractJsonString. Returns false if the needle is absent or the array is unterminated.
bool ExtractJsonStringArray(const string &json, const string &key, vector<string> &out) {
	string needle = "\"" + key + "\":[";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	out.clear();
	while (pos < json.size()) {
		while (pos < json.size() &&
		       (json[pos] == ' ' || json[pos] == '\t' || json[pos] == '\n' || json[pos] == ',')) {
			pos++;
		}
		if (pos < json.size() && json[pos] == ']') {
			return true;
		}
		if (pos >= json.size() || json[pos] != '"') {
			return false; // only quoted string elements are supported
		}
		pos++; // past opening quote
		string val;
		while (pos < json.size()) {
			char c = json[pos];
			if (c == '\\' && pos + 1 < json.size()) {
				char esc = json[pos + 1];
				val += (esc == 'n') ? '\n' : esc;
				pos += 2;
				continue;
			}
			if (c == '"') {
				pos++;
				break;
			}
			val += c;
			pos++;
		}
		out.push_back(std::move(val));
	}
	return false; // unterminated array
}

SqlDialect ParseDialectString(const string &s) {
	// Reuse the lpts helper so dialect names stay in lockstep with the
	// LPTS pipeline. LPTS throws on unrecognised values which is exactly
	// the contract we want.
	try {
		return ParseSqlDialect(s);
	} catch (const std::exception &e) {
		throw InvalidInputException("openivm_compile_with_facts: target_dialect='%s' is not a recognised SQL dialect "
		                            "(expected 'spark' | 'duckdb' | 'ducklake')",
		                            s.c_str());
	}
}

} // namespace

CompileFacts ParseFactsJson(const string &json) {
	CompileFacts out;

	string dialect_str;
	if (!ExtractJsonString(json, "target_dialect", dialect_str)) {
		throw InvalidInputException(
		    "openivm_compile_with_facts: required field 'target_dialect' missing or not a string");
	}
	out.target_dialect = ParseDialectString(dialect_str);

	int sv = 0;
	if (ExtractJsonInt(json, "schema_version", sv)) {
		out.schema_version = sv;
	}
	ExtractJsonBool(json, "compile_only", out.compile_only);
	// Legacy v1 top-level position (still honored); v2 canonical location is universal.*.
	ExtractJsonBool(json, "force_view_delta_cascade", out.force_view_delta_cascade);

	// v2: universal facts (stable per view). Absence = no assertion; never the negative claim,
	// so an absent/empty field can only forgo an optimization, never disable a correctness guard.
	string universal_body;
	if (ExtractJsonObjectBody(json, "universal", universal_body)) {
		ExtractJsonBool(universal_body, "force_view_delta_cascade", out.force_view_delta_cascade);
		ExtractJsonBool(universal_body, "emit_derived_facts", out.emit_derived_facts);
		ExtractJsonStringArray(universal_body, "source_constraints", out.source_constraints);
		ExtractJsonStringArray(universal_body, "partitioning", out.partitioning);
		ExtractJsonStringArray(universal_body, "target_capabilities", out.target_capabilities);
	}

	// v2: per-batch facts. Presence of the "batch" object gates the per-batch fast paths;
	// when absent, planning keeps its conservative defaults.
	string batch_body;
	if (ExtractJsonObjectBody(json, "batch", batch_body)) {
		out.has_batch_facts = true;
		ExtractJsonStringArray(batch_body, "active_delta_tables", out.active_delta_tables);
		ExtractJsonBool(batch_body, "insert_only", out.batch_insert_only);
	}

	return out;
}

//------------------------------------------------------------------------------
// CompileFactsContextSlot
//
// The slot is stored in `ClientContext::registered_state` via a
// `ClientContextState` subclass that simply owns a `shared_ptr<CompileFacts>`.
// The RAII helper inserts the slot in its ctor and removes it in its dtor,
// even on exception paths from `GenerateRefreshSQL`. This guarantees that
// downstream PRAGMA-refresh calls in the same connection never see leaked
// facts.
//------------------------------------------------------------------------------

namespace {

class CompileFactsState : public ClientContextState {
public:
	explicit CompileFactsState(shared_ptr<CompileFacts> facts_p) : facts(std::move(facts_p)) {
	}
	shared_ptr<CompileFacts> facts;
};

} // namespace

CompileFactsContextSlot::CompileFactsContextSlot(ClientContext &ctx_p, shared_ptr<CompileFacts> facts)
    : ctx(ctx_p), installed(false) {
	if (!facts) {
		return;
	}
	auto state = make_shared_ptr<CompileFactsState>(std::move(facts));
	ctx.registered_state->Insert(SLOT_KEY, std::move(state));
	installed = true;
}

CompileFactsContextSlot::~CompileFactsContextSlot() {
	if (!installed) {
		return;
	}
	try {
		ctx.registered_state->Remove(SLOT_KEY);
	} catch (...) {
		// swallow — destructor must not throw
	}
}

CompileFacts CompileFactsContextSlot::Get(ClientContext &ctx) {
	auto state = ctx.registered_state->Get<CompileFactsState>(SLOT_KEY);
	if (!state || !state->facts) {
		return CompileFacts::Default();
	}
	return *state->facts;
}

//------------------------------------------------------------------------------
// openivm_compile_with_facts(view_name VARCHAR, facts_json VARCHAR) table function
//
// Schema: (refresh_type INTEGER, refresh_type_name VARCHAR,
//          stmt_order INTEGER, stmt_kind VARCHAR, sql VARCHAR)
//
// The bind step parses the JSON into a `CompileFacts`, installs it onto
// `ClientContext::registered_state` via `CompileFactsContextSlot`, then
// invokes `GenerateRefreshSQL`. The resulting SQL is split into logical buckets
// (meta_pre / data / meta_post) and each top-level statement becomes its own
// output row. The slot is removed before bind returns so optimizer rules in
// later refresh calls don't see stale facts.
//------------------------------------------------------------------------------

namespace {

struct CompileWithFactsBindData : public TableFunctionData {
	int32_t refresh_type_int = 0;
	string refresh_type_name;
	vector<string> stmt_kinds;
	vector<string> stmt_sqls;
};

struct CompileWithFactsGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

// Splits a SQL fragment on top-level `;` boundaries (ignoring `;` inside
// quoted strings) and pushes each non-empty trimmed statement, tagged
// with `kind`, into the bind result.
static void PushBucket(CompileWithFactsBindData &bd, const string &kind, const string &sql_block) {
	if (sql_block.empty()) {
		return;
	}
	string current;
	current.reserve(sql_block.size());
	char in_quote = 0; // either 0, '\'', or '"'
	for (size_t i = 0; i < sql_block.size(); i++) {
		char c = sql_block[i];
		if (in_quote) {
			current += c;
			if (c == in_quote) {
				in_quote = 0;
			} else if (c == '\\' && i + 1 < sql_block.size()) {
				current += sql_block[++i];
			}
			continue;
		}
		if (c == '\'' || c == '"') {
			in_quote = c;
			current += c;
			continue;
		}
		if (c == ';') {
			string trimmed = current;
			StringUtil::Trim(trimmed);
			if (!trimmed.empty()) {
				bd.stmt_kinds.push_back(kind);
				bd.stmt_sqls.push_back(trimmed + ";");
			}
			current.clear();
		} else {
			current += c;
		}
	}
	string tail = current;
	StringUtil::Trim(tail);
	if (!tail.empty()) {
		bd.stmt_kinds.push_back(kind);
		bd.stmt_sqls.push_back(tail + ";");
	}
}

} // namespace

unique_ptr<FunctionData> OpenIvmCompileWithFactsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() < 2) {
		throw InvalidInputException("openivm_compile_with_facts(view_name, facts_json) requires exactly two arguments");
	}
	string view_name = StringValue::Get(input.inputs[0]);
	string facts_json = StringValue::Get(input.inputs[1]);

	if (view_name.find('.') != string::npos) {
		throw InvalidInputException(
		    "openivm_compile_with_facts: cannot resolve qualified view_name '%s'; pass unqualified short name only",
		    view_name.c_str());
	}

	auto facts_owned = make_shared_ptr<CompileFacts>(ParseFactsJson(facts_json));

	Connection con(*context.db.get());
	auto resolved = ResolveViewCatalogFromContext(context, con, view_name, /*throw_if_not_found=*/true);
	RefreshMetadata metadata(con);
	RefreshType type = metadata.GetViewType(view_name);

	string sql;
	string out_pre_meta;
	string out_post_meta;
	{
		CompileFactsContextSlot slot(context, facts_owned);
		sql = GenerateRefreshSQL(context, resolved.view_catalog_name, resolved.view_schema_name, view_name,
		                         resolved.cross_system, "", "", resolved.cross_system ? &out_pre_meta : nullptr,
		                         resolved.cross_system ? &out_post_meta : nullptr,
		                         /*compile_profile=*/nullptr, /*precomputed_delta_activity=*/nullptr,
		                         /*out_adaptive_estimate=*/nullptr, /*facts=*/facts_owned.get());
	}

	auto result = make_uniq<CompileWithFactsBindData>();
	result->refresh_type_int = static_cast<int32_t>(type);
	result->refresh_type_name = string(RefreshTypeName(type));

	PushBucket(*result, "meta_pre", out_pre_meta);
	PushBucket(*result, "data", sql);
	PushBucket(*result, "meta_post", out_post_meta);

	// Output enrichment: when requested, return derived UNIVERSAL facts the caller can't cheaply
	// derive itself (it runs on empty schema-only tables). Emitted as a single extra row tagged
	// stmt_kind="derived_fact" carrying JSON in the `sql` column — additive (consumers filtering on
	// meta_pre|data|meta_post ignore it) and NOT routed through PushBucket (no trailing ';').
	if (facts_owned->emit_derived_facts) {
		RefreshMetadata::LeftJoinNullableSources nullable;
		metadata.GetLeftJoinNullableSources(view_name, nullable); // false -> empty, fine
		vector<string> referenced;
		for (auto &dt : metadata.GetDeltaTables(view_name)) {
			referenced.push_back(BaseTableNameFromDeltaKey(dt));
		}
		string derived = "{\"nullable_sources\":" + SqlUtils::JsonArray(nullable.tables) +
		                 ",\"referenced_sources\":" + SqlUtils::JsonArray(referenced) + "}";
		result->stmt_kinds.push_back("derived_fact");
		result->stmt_sqls.push_back(derived);
	}

	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("refresh_type");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("refresh_type_name");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("stmt_order");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("stmt_kind");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sql");

	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> OpenIvmCompileWithFactsInit(ClientContext &context,
                                                                 TableFunctionInitInput &input) {
	return make_uniq<CompileWithFactsGlobalState>();
}

void OpenIvmCompileWithFactsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bd = data_p.bind_data->Cast<CompileWithFactsBindData>();
	auto &state = data_p.global_state->Cast<CompileWithFactsGlobalState>();

	idx_t total = bd.stmt_sqls.size();
	idx_t emitted = 0;
	while (state.offset < total && emitted < STANDARD_VECTOR_SIZE) {
		output.SetValue(0, emitted, Value::INTEGER(bd.refresh_type_int));
		output.SetValue(1, emitted, Value(bd.refresh_type_name));
		output.SetValue(2, emitted, Value::INTEGER(static_cast<int32_t>(state.offset)));
		output.SetValue(3, emitted, Value(bd.stmt_kinds[state.offset]));
		output.SetValue(4, emitted, Value(bd.stmt_sqls[state.offset]));
		state.offset++;
		emitted++;
	}
	output.SetCardinality(emitted);
}

} // namespace openivm
} // namespace duckdb
