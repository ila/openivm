#include "compile_facts.hpp"

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_context.hpp"

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

} // namespace openivm
} // namespace duckdb
