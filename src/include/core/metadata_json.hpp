#pragma once

#include "duckdb.hpp"
#include "core/sql_utils.hpp"

#include <cctype>

namespace duckdb {
namespace MetadataJson {

// Lenient readers for the JSON written by OpenIVM metadata writers and by CompileFacts producers (e.g.
// Spark, which puts a space after each colon). They find the first `"key":` (whitespace allowed around
// the colon) and decode `\n`; any other escaped character is taken literally. Not a general JSON parser:
// stored metadata may contain raw control characters that a strict parser rejects.

inline void SkipSpace(const string &json, size_t &pos, bool skip_commas = false) {
	while (pos < json.size() &&
	       (std::isspace(static_cast<unsigned char>(json[pos])) || (skip_commas && json[pos] == ','))) {
		pos++;
	}
}

// Position of the value of `key`, or npos.
inline size_t FindValue(const string &json, const string &key) {
	const string needle = "\"" + key + "\"";
	for (size_t pos = json.find(needle); pos != string::npos; pos = json.find(needle, pos + 1)) {
		size_t value = pos + needle.size();
		SkipSpace(json, value);
		if (value < json.size() && json[value] == ':') {
			SkipSpace(json, ++value);
			return value;
		}
	}
	return string::npos;
}

// Decode the string literal starting at `pos` (a quote); `pos` ends past its closing quote.
inline bool ParseString(const string &json, size_t &pos, string &val) {
	if (pos >= json.size() || json[pos] != '"') {
		return false;
	}
	val.clear();
	for (pos++; pos < json.size(); pos++) {
		char c = json[pos];
		if (c == '\\' && pos + 1 < json.size()) {
			pos++;
			val += json[pos] == 'n' ? '\n' : json[pos];
		} else if (c == '"') {
			pos++;
			return true;
		} else {
			val += c;
		}
	}
	return false;
}

inline bool ExtractJsonString(const string &json, const string &key, string &val) {
	size_t pos = FindValue(json, key);
	return pos != string::npos && ParseString(json, pos, val);
}

inline bool ExtractJsonStringArray(const string &json, const string &key, vector<string> &val) {
	size_t pos = FindValue(json, key);
	if (pos == string::npos || json[pos] != '[') {
		return false;
	}
	val.clear();
	for (SkipSpace(json, ++pos, true); pos < json.size(); SkipSpace(json, pos, true)) {
		if (json[pos] == ']') {
			return true;
		}
		string item;
		if (!ParseString(json, pos, item)) {
			return false;
		}
		val.push_back(std::move(item));
	}
	return false;
}

// String-valued members of the object under `key`.
inline bool ExtractJsonStringMap(const string &json, const string &key, unordered_map<string, string> &val) {
	size_t pos = FindValue(json, key);
	if (pos == string::npos || json[pos] != '{') {
		return false;
	}
	for (SkipSpace(json, ++pos, true); pos < json.size(); SkipSpace(json, pos, true)) {
		if (json[pos] == '}') {
			return true;
		}
		string member;
		string value;
		if (!ParseString(json, pos, member)) {
			return false;
		}
		SkipSpace(json, pos);
		if (pos >= json.size() || json[pos] != ':') {
			return false;
		}
		SkipSpace(json, ++pos);
		if (!ParseString(json, pos, value)) {
			return false;
		}
		val[member] = std::move(value);
	}
	return false;
}

inline bool ExtractJsonBool(const string &json, const string &key, bool &val) {
	size_t pos = FindValue(json, key);
	if (pos != string::npos && json.compare(pos, 4, "true") == 0) {
		val = true;
		return true;
	}
	if (pos != string::npos && json.compare(pos, 5, "false") == 0) {
		val = false;
		return true;
	}
	return false;
}

inline bool ExtractJsonInt(const string &json, const string &key, int &val) {
	size_t start = FindValue(json, key);
	if (start == string::npos) {
		return false;
	}
	size_t pos = start + (json[start] == '-' || json[start] == '+' ? 1 : 0);
	while (pos < json.size() && std::isdigit(static_cast<unsigned char>(json[pos]))) {
		pos++;
	}
	try {
		val = std::stoi(json.substr(start, pos - start));
		return true;
	} catch (...) {
		return false;
	}
}

inline bool ParseIndex(const string &text, idx_t &val) {
	try {
		val = static_cast<idx_t>(std::stoull(text));
		return true;
	} catch (...) {
		return false;
	}
}

inline vector<string> ExtractJsonObjectsFromArray(const string &json, const string &key,
                                                  optional_ptr<bool> parsed_complete = nullptr) {
	vector<string> objects;
	if (parsed_complete) {
		*parsed_complete = false;
	}
	size_t pos = FindValue(json, key);
	if (pos == string::npos || json[pos] != '[') {
		return objects;
	}
	int depth = 0;
	bool in_string = false;
	bool escaped = false;
	size_t object_start = string::npos;
	for (pos++; pos < json.size(); pos++) {
		char c = json[pos];
		if (in_string) {
			if (escaped) {
				escaped = false;
			} else if (c == '\\') {
				escaped = true;
			} else if (c == '"') {
				in_string = false;
			}
		} else if (c == '"') {
			in_string = true;
		} else if (c == '{') {
			if (depth++ == 0) {
				object_start = pos;
			}
		} else if (c == '}' && depth > 0) {
			depth--;
			if (depth == 0) {
				objects.push_back(json.substr(object_start, pos - object_start + 1));
			}
		} else if (c == ']' && depth == 0) {
			if (parsed_complete) {
				*parsed_complete = true;
			}
			break;
		}
	}
	return objects;
}

// Each metadata struct describes its fields once using IO::Field<T>: const for Writer, mutable for Reader.
// The field list lives in namespace duckdb. Writer serializes them in that order and Reader parses them back. Booleans
// and indexes are stored as JSON strings, enums by name.
enum class Presence : uint8_t {
	REQUIRED,  // always written; reading fails without it
	OPTIONAL,  // always written; may be absent when read
	OMIT_EMPTY // written only when non-empty; may be absent when read
};

class Writer {
public:
	template <class T>
	using Field = const T;
	void Raw(const char *key, const string &json) {
		out += (out.size() == 1 ? "\"" : ",\"") + string(key) + "\":" + json;
	}
	void Str(const char *key, const string &val, Presence presence = Presence::REQUIRED) {
		if (presence != Presence::OMIT_EMPTY || !val.empty()) {
			Raw(key, SqlUtils::JsonQuote(val));
		}
	}
	void Arr(const char *key, const vector<string> &val, Presence = Presence::REQUIRED) {
		Raw(key, SqlUtils::JsonArray(val));
	}
	void Objects(const char *key, const vector<string> &objects) {
		Raw(key, "[" + StringUtil::Join(objects, ",") + "]");
	}
	void Bool(const char *key, const bool &val, Presence = Presence::REQUIRED) {
		Str(key, val ? "true" : "false");
	}
	void Index(const char *key, const idx_t &val, Presence = Presence::REQUIRED) {
		Str(key, to_string(val));
	}
	void Tag(const char *key, const char *val) {
		Str(key, val);
	}
	template <class ENUM, idx_t N>
	void Enum(const char *key, const ENUM &val, const char *const (&names)[N]) {
		Str(key, names[static_cast<idx_t>(val)]);
	}
	string Finish() const {
		return out + "}";
	}

private:
	string out = "{";
};

class Reader {
public:
	template <class T>
	using Field = T;
	explicit Reader(const string &json) : json(json) {
	}
	void Str(const char *key, string &val, Presence presence = Presence::REQUIRED) {
		Require(ExtractJsonString(json, key, val), presence);
	}
	void Arr(const char *key, vector<string> &val, Presence presence = Presence::REQUIRED) {
		Require(ExtractJsonStringArray(json, key, val), presence);
	}
	void Bool(const char *key, bool &val, Presence presence = Presence::REQUIRED) {
		string text;
		bool found = ExtractJsonString(json, key, text);
		if (found) {
			val = StringUtil::CIEquals(text, "true");
		}
		Require(found, presence);
	}
	void Index(const char *key, idx_t &val, Presence presence = Presence::REQUIRED) {
		string text;
		Require(ExtractJsonString(json, key, text) && ParseIndex(text, val), presence);
	}
	void Tag(const char *key, const char *val) {
		string text;
		Require(ExtractJsonString(json, key, text) && text == val, Presence::REQUIRED);
	}
	template <class ENUM, idx_t N>
	void Enum(const char *key, ENUM &val, const char *const (&names)[N]) {
		string text;
		bool found = false;
		if (ExtractJsonString(json, key, text)) {
			for (idx_t i = 0; i < N && !found; i++) {
				found = StringUtil::CIEquals(text, names[i]);
				val = found ? static_cast<ENUM>(i) : val;
			}
		}
		Require(found, Presence::REQUIRED);
	}
	bool ok = true;

private:
	void Require(bool found, Presence presence) {
		ok = ok && (found || presence != Presence::REQUIRED);
	}
	const string &json;
};

// Serialize / parse a struct that defines `Fields`.
template <class T>
string Write(const T &val) {
	Writer writer;
	Fields(writer, val);
	return writer.Finish();
}

template <class T>
bool Read(const string &json, T &val) {
	if (json.empty()) {
		return false;
	}
	val = T();
	Reader reader(json);
	Fields(reader, val);
	return reader.ok;
}

} // namespace MetadataJson
} // namespace duckdb
