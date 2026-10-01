#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace MetadataJson {

// Readers for the canonical JSON emitted by OpenIVM metadata and CompileFacts
// writers. This wire format has no whitespace around keys; these are not general
// JSON readers. Keep escaping and array completion consistent across consumers.
inline bool ExtractJsonString(const string &json, const string &key, string &val) {
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

inline bool ExtractJsonStringArray(const string &json, const string &key, vector<string> &val) {
	string needle = "\"" + key + "\":[";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	val.clear();
	while (pos < json.size()) {
		while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' || json[pos] == ',')) {
			pos++;
		}
		if (pos < json.size() && json[pos] == ']') {
			return true;
		}
		if (pos >= json.size() || json[pos] != '"') {
			return false;
		}
		pos++;
		string item;
		while (pos < json.size()) {
			char c = json[pos];
			if (c == '\\' && pos + 1 < json.size()) {
				char esc = json[pos + 1];
				item += (esc == 'n') ? '\n' : esc;
				pos += 2;
				continue;
			}
			if (c == '"') {
				pos++;
				break;
			}
			item += c;
			pos++;
		}
		val.push_back(std::move(item));
	}
	return false;
}

inline vector<string> ExtractJsonObjectsFromArray(const string &json, const string &key,
                                                  optional_ptr<bool> parsed_complete = nullptr) {
	vector<string> objects;
	if (parsed_complete) {
		*parsed_complete = false;
	}
	string needle = "\"" + key + "\":[";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return objects;
	}
	pos += needle.size();
	int depth = 0;
	bool in_string = false;
	bool escaped = false;
	size_t object_start = string::npos;
	for (; pos < json.size(); pos++) {
		char c = json[pos];
		if (in_string) {
			if (escaped) {
				escaped = false;
			} else if (c == '\\') {
				escaped = true;
			} else if (c == '"') {
				in_string = false;
			}
			continue;
		}
		if (c == '"') {
			in_string = true;
			continue;
		}
		if (c == '{') {
			if (depth == 0) {
				object_start = pos;
			}
			depth++;
			continue;
		}
		if (c == '}') {
			if (depth > 0) {
				depth--;
				if (depth == 0 && object_start != string::npos) {
					objects.push_back(json.substr(object_start, pos - object_start + 1));
					object_start = string::npos;
				}
			}
			continue;
		}
		if (c == ']' && depth == 0) {
			if (parsed_complete) {
				*parsed_complete = true;
			}
			break;
		}
	}
	return objects;
}

} // namespace MetadataJson
} // namespace duckdb
