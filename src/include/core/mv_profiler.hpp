#pragma once

#include "duckdb.hpp"
#include <chrono>

namespace duckdb {

struct MVProfileStep {
	string step_name;
	int64_t duration_ms;
	string detail;
};

class MVProfiler {
public:
	MVProfiler(ClientContext &context, string total_step, string view_name = "", string view_catalog = "");
	bool Enabled() const {
		return enabled;
	}
	const string &RefreshId() const {
		return refresh_id;
	}
	void SetProfileTable(const string &table) {
		profile_table = table;
	}
	void SetViewName(const string &name, Connection &con);
	void AddStep(const string &name, std::chrono::steady_clock::time_point start, const string &detail = "");
	void AddMeasuredStep(const string &name, int64_t duration_ms, const string &detail = "");
	void AddTotal();
	void Flush();

private:
	ClientContext &context;
	const bool enabled;
	int64_t retention_days = 31;
	string total_step;
	string view_name;
	string view_catalog;
	string refresh_id;
	string profile_table;
	bool flushed = false;
	std::chrono::steady_clock::time_point total_start;
	vector<MVProfileStep> steps;
};

} // namespace duckdb
