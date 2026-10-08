#pragma once

// Helpers for benchmarks that read openivm_views. view_name holds an encoded internal key; the
// SQL-facing name lives in view_sql_name, so a SQL name must be resolved to its key first.

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "core/refresh_metadata.hpp"

#include <string>
#include <vector>

namespace openivm_bench {

struct ViewTypeLookup {
	bool found = false;
	int64_t type = 0;
	std::string key;
	std::string error;
};

// Resolves the MV named `sql_name` in the connection's current catalog/schema to its metadata key
// via RefreshMetadata::FindViewKey, then reads its refresh type from the native catalog's metadata.
// `con` may point at a DuckLake catalog (USE dl.main); metadata always lives in `native_catalog`.
inline ViewTypeLookup LookupViewType(duckdb::DuckDB &db, duckdb::Connection &con, const std::string &native_catalog,
                                     const std::string &sql_name) {
	ViewTypeLookup out;
	try {
		auto loc = con.Query("SELECT current_database(), current_schema()");
		if (!loc || loc->HasError() || loc->RowCount() == 0) {
			out.error = loc ? loc->GetError() : "no result";
			return out;
		}
		duckdb::Connection meta_con(db);
		duckdb::RefreshMetadata::UseCatalog(*meta_con.context, meta_con, native_catalog);
		duckdb::RefreshMetadata metadata(meta_con);
		out.key = metadata.FindViewKey(loc->GetValue(0, 0).ToString(), loc->GetValue(1, 0).ToString(), sql_name);
	} catch (std::exception &e) {
		out.error = e.what();
		return out;
	}
	if (out.key.empty()) {
		return out;
	}
	auto result = con.Query("SELECT type FROM " + duckdb::KeywordHelper::WriteOptionallyQuoted(native_catalog) +
	                        ".main.openivm_views WHERE view_name = " + duckdb::Value(out.key).ToSQLString());
	if (!result || result->HasError()) {
		out.error = result ? result->GetError() : "no result";
		return out;
	}
	if (result->RowCount() > 0) {
		out.type = result->GetValue(0, 0).GetValue<int64_t>();
		out.found = true;
	}
	return out;
}

struct LeftoverView {
	std::string key;
	std::string catalog;
	std::string schema;
};

// Lists metadata rows whose SQL-facing name matches `sql_like` (a LIKE pattern), with the catalog/schema
// that hold their data table. Must run before DROP VIEW removes the rows.
inline std::vector<LeftoverView> ListLeftoverViews(duckdb::Connection &con, const std::string &native_catalog,
                                                   const std::string &sql_like) {
	std::vector<LeftoverView> out;
	auto meta = con.Query("SELECT view_name, view_catalog, view_schema FROM " +
	                      duckdb::KeywordHelper::WriteOptionallyQuoted(native_catalog) +
	                      ".main.openivm_views WHERE COALESCE(view_sql_name, view_name) LIKE " +
	                      duckdb::Value(sql_like).ToSQLString());
	if (meta && !meta->HasError()) {
		for (duckdb::idx_t r = 0; r < meta->RowCount(); r++) {
			auto cat = meta->GetValue(1, r);
			auto sch = meta->GetValue(2, r);
			out.push_back({meta->GetValue(0, r).ToString(), cat.IsNull() ? native_catalog : cat.ToString(),
			               sch.IsNull() ? "main" : sch.ToString()});
		}
	}
	return out;
}

} // namespace openivm_bench
