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
// We deliberately avoid yyjson / DuckDB's JSON extension here so that the
// openivm extension does not gain a transitive dependency on optional
// extensions. The schema is small (a handful of strings/bools and two
// arrays of fixed-shape objects), so a single-pass scanner is sufficient.
//
// Implements the subset required by `CompileFacts`:
//   - top-level object
//   - string / number / boolean / null primitives
//   - object members ("key": value)
//   - array of objects (downstreams, pending_deltas)
//
// Throws `InvalidInputException` on malformed input.
//------------------------------------------------------------------------------

namespace {

struct JsonScanner {
	const string &src;
	size_t pos = 0;

	explicit JsonScanner(const string &s) : src(s) {
	}

	[[noreturn]] void Fail(const string &msg) const {
		throw InvalidInputException("openivm_compile_with_facts: " + msg + " at offset " + std::to_string(pos));
	}

	void SkipWs() {
		while (pos < src.size()) {
			char c = src[pos];
			if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
				pos++;
			} else {
				break;
			}
		}
	}

	char Peek() {
		SkipWs();
		if (pos >= src.size()) {
			Fail("unexpected end of JSON");
		}
		return src[pos];
	}

	void Expect(char c) {
		SkipWs();
		if (pos >= src.size() || src[pos] != c) {
			Fail(string("expected '") + c + "'");
		}
		pos++;
	}

	bool TryConsume(char c) {
		SkipWs();
		if (pos < src.size() && src[pos] == c) {
			pos++;
			return true;
		}
		return false;
	}

	string ParseString() {
		Expect('"');
		string out;
		while (pos < src.size()) {
			char c = src[pos++];
			if (c == '"') {
				return out;
			}
			if (c == '\\') {
				if (pos >= src.size()) {
					Fail("unterminated string escape");
				}
				char esc = src[pos++];
				switch (esc) {
				case '"':
					out += '"';
					break;
				case '\\':
					out += '\\';
					break;
				case '/':
					out += '/';
					break;
				case 'b':
					out += '\b';
					break;
				case 'f':
					out += '\f';
					break;
				case 'n':
					out += '\n';
					break;
				case 'r':
					out += '\r';
					break;
				case 't':
					out += '\t';
					break;
				case 'u':
					// We only need ASCII for our schema — accept and pass
					// through the 4 hex digits as a placeholder rather than
					// fully implementing UTF-16 surrogate decoding.
					if (pos + 4 > src.size()) {
						Fail("truncated \\u escape");
					}
					out += '?';
					pos += 4;
					break;
				default:
					Fail(string("invalid string escape \\") + esc);
				}
			} else {
				out += c;
			}
		}
		Fail("unterminated string literal");
	}

	bool ParseBool() {
		SkipWs();
		if (pos + 4 <= src.size() && src.compare(pos, 4, "true") == 0) {
			pos += 4;
			return true;
		}
		if (pos + 5 <= src.size() && src.compare(pos, 5, "false") == 0) {
			pos += 5;
			return false;
		}
		Fail("expected boolean");
	}

	int ParseInt() {
		SkipWs();
		size_t start = pos;
		if (pos < src.size() && (src[pos] == '-' || src[pos] == '+')) {
			pos++;
		}
		while (pos < src.size() && std::isdigit(static_cast<unsigned char>(src[pos]))) {
			pos++;
		}
		if (start == pos) {
			Fail("expected integer");
		}
		try {
			return std::stoi(src.substr(start, pos - start));
		} catch (std::exception &) {
			Fail("integer out of range");
		}
	}

	// Skip a value of any type so we can ignore unknown top-level fields
	// (forward-compat per B5 item 4).
	void SkipValue() {
		SkipWs();
		if (pos >= src.size()) {
			Fail("unexpected end of JSON while skipping value");
		}
		char c = src[pos];
		if (c == '"') {
			ParseString();
		} else if (c == '{') {
			SkipObject();
		} else if (c == '[') {
			SkipArray();
		} else if (c == 't' || c == 'f') {
			ParseBool();
		} else if (c == 'n') {
			if (pos + 4 > src.size() || src.compare(pos, 4, "null") != 0) {
				Fail("expected 'null'");
			}
			pos += 4;
		} else {
			// Number — read digits / decimal / sign / exponent
			while (pos < src.size()) {
				char ch = src[pos];
				if (std::isdigit(static_cast<unsigned char>(ch)) || ch == '-' || ch == '+' || ch == '.' || ch == 'e' ||
				    ch == 'E') {
					pos++;
				} else {
					break;
				}
			}
		}
	}

	void SkipObject() {
		Expect('{');
		SkipWs();
		if (TryConsume('}')) {
			return;
		}
		while (true) {
			ParseString();
			Expect(':');
			SkipValue();
			if (TryConsume(',')) {
				continue;
			}
			Expect('}');
			return;
		}
	}

	void SkipArray() {
		Expect('[');
		SkipWs();
		if (TryConsume(']')) {
			return;
		}
		while (true) {
			SkipValue();
			if (TryConsume(',')) {
				continue;
			}
			Expect(']');
			return;
		}
	}
};

static SqlDialect ParseDialectString(const string &s) {
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
	JsonScanner s(json);
	CompileFacts out;

	s.Expect('{');
	bool saw_dialect = false;
	bool first = true;
	while (true) {
		s.SkipWs();
		if (first && s.TryConsume('}')) {
			throw InvalidInputException(
			    "openivm_compile_with_facts: required field 'target_dialect' missing or not a string");
		}
		first = false;

		string key = s.ParseString();
		s.Expect(':');

		if (key == "schema_version") {
			out.schema_version = s.ParseInt();
			if (out.schema_version != CompileFacts::CURRENT_SCHEMA_VERSION) {
				// Reserved-for-future; accept any int but only emit a debug
				// trace if it differs from the current expectation.
			}
		} else if (key == "target_dialect") {
			string val = s.ParseString();
			out.target_dialect = ParseDialectString(val);
			saw_dialect = true;
		} else if (key == "compile_only") {
			out.compile_only = s.ParseBool();
		} else if (key == "force_view_delta_cascade") {
			out.force_view_delta_cascade = s.ParseBool();
		} else if (key == "downstreams") {
			s.Expect('[');
			s.SkipWs();
			if (!s.TryConsume(']')) {
				while (true) {
					CompileFacts::DownstreamView d;
					s.Expect('{');
					bool obj_first = true;
					while (true) {
						s.SkipWs();
						if (obj_first && s.TryConsume('}')) {
							break;
						}
						obj_first = false;
						string k = s.ParseString();
						s.Expect(':');
						if (k == "name") {
							d.name = s.ParseString();
						} else if (k == "cascade") {
							d.cascade = s.ParseBool();
						} else {
							s.SkipValue(); // unknown sub-field — ignore
						}
						if (s.TryConsume(',')) {
							continue;
						}
						s.Expect('}');
						break;
					}
					out.downstreams.push_back(std::move(d));
					if (s.TryConsume(',')) {
						continue;
					}
					s.Expect(']');
					break;
				}
			}
		} else if (key == "pending_deltas") {
			s.Expect('[');
			s.SkipWs();
			if (!s.TryConsume(']')) {
				while (true) {
					CompileFacts::PendingDelta d;
					s.Expect('{');
					bool obj_first = true;
					while (true) {
						s.SkipWs();
						if (obj_first && s.TryConsume('}')) {
							break;
						}
						obj_first = false;
						string k = s.ParseString();
						s.Expect(':');
						if (k == "base") {
							d.base = s.ParseString();
						} else if (k == "op") {
							d.op = s.ParseString();
						} else if (k == "ts") {
							d.ts = s.ParseString();
						} else {
							s.SkipValue();
						}
						if (s.TryConsume(',')) {
							continue;
						}
						s.Expect('}');
						break;
					}
					out.pending_deltas.push_back(std::move(d));
					if (s.TryConsume(',')) {
						continue;
					}
					s.Expect(']');
					break;
				}
			}
		} else {
			// Unknown top-level key — IGNORE for forward-compat (B5 item 4).
			s.SkipValue();
		}

		if (s.TryConsume(',')) {
			continue;
		}
		s.Expect('}');
		break;
	}

	if (!saw_dialect) {
		throw InvalidInputException(
		    "openivm_compile_with_facts: required field 'target_dialect' missing or not a string");
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
// invokes `GenerateRefreshSQL`. The resulting SQL is split into logical
// buckets (meta_pre / companion / data / cleanup / meta_post) and each
// top-level statement becomes its own output row. The slot is removed
// before bind returns so optimizer rules in later refresh calls don't see
// stale facts.
//
// The actual row-per-statement splitting is added in a later commit; this
// commit emits a single concatenated SQL row to keep the diff manageable.
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
