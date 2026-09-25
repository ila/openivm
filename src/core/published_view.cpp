#include "core/published_view.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"

namespace duckdb {

string PublishedViewName(const string &view_name) {
	return string(openivm::VISIBLE_TABLE_PREFIX) + view_name;
}

string PublishedSourceViewName(string source_name) {
	if (StringUtil::StartsWith(source_name, openivm::DELTA_PREFIX)) {
		source_name = source_name.substr(strlen(openivm::DELTA_PREFIX));
	}
	for (auto prefix : {openivm::DATA_TABLE_PREFIX, openivm::VISIBLE_TABLE_PREFIX}) {
		if (StringUtil::StartsWith(source_name, prefix)) {
			return source_name.substr(strlen(prefix));
		}
	}
	return source_name;
}

string BuildPublishViewSQL(const string &view_name, const string &prefix, const string &query,
                           const vector<string> &columns, bool ducklake, const string &metadata_table,
                           const vector<string> &scope_columns, const string &timestamp_sql, SqlDialect dialect) {
	auto quote = [&](const string &name) {
		return DialectQuoteIdent(name, dialect);
	};
	auto visible = prefix + quote(PublishedViewName(view_name));
	auto delta_name = SqlUtils::DeltaName(PublishedViewName(view_name));
	auto delta = prefix + quote(delta_name);
	auto next = (dialect == SqlDialect::SPARK ? prefix : "") + quote("openivm_publish_next_" + view_name);
	auto changes = (dialect == SqlDialect::SPARK ? prefix : "") + quote("openivm_publish_changes_" + view_name);
	vector<string> quoted_columns;
	for (auto &column : columns) {
		quoted_columns.push_back(quote(column));
	}
	auto column_list = StringUtil::Join(quoted_columns, ", ");
	string equality;
	for (auto &column : columns) {
		if (!equality.empty()) {
			equality += " AND ";
		}
		auto quoted = quote(column);
		equality += "v." + quoted + " IS NOT DISTINCT FROM d." + quoted;
	}
	string scope;
	for (auto &column : scope_columns) {
		if (!scope.empty()) {
			scope += " AND ";
		}
		auto quoted = quote(column);
		scope += "v." + quoted + " IS NOT DISTINCT FROM d." + quoted;
	}
	string scoped_query = query;
	string old_query = "SELECT * FROM " + visible;
	if (!scope.empty()) {
		auto raw_delta = prefix + quote(SqlUtils::DeltaName(view_name));
		auto predicate = " WHERE EXISTS (SELECT 1 FROM " + raw_delta + " d WHERE " + scope + ")";
		scoped_query = "SELECT v.* FROM (" + query + ") v" + predicate;
		old_query = "SELECT v.* FROM " + visible + " v" + predicate;
	}
	// Diff the visible output, not the base query. In particular LIMIT refill and
	// HAVING threshold crossings must retract the previously published rows.
	string create = dialect == SqlDialect::SPARK ? "CREATE TABLE " : "CREATE TEMP TABLE ";
	string sql = create + next + " AS " + scoped_query + ";\n";
	auto weight = quote("openivm_publish_weight");
	sql += create + changes + " AS SELECT " + column_list + ", SUM(" + weight + ") AS " + weight +
	       " FROM (SELECT *, CAST(-1 AS BIGINT) AS " + weight + " FROM (" + old_query +
	       ") old_rows UNION ALL SELECT *, CAST(1 AS BIGINT) FROM " + next + ") signed_rows GROUP BY " + column_list +
	       " HAVING SUM(" + weight + ") <> 0;\n";
	if (!ducklake) {
		string timestamp = timestamp_sql.empty() ? openivm::UTC_NOW_SQL : timestamp_sql;
		vector<string> delta_columns;
		for (auto &column : quoted_columns) {
			delta_columns.push_back("c." + column);
		}
		auto expansion = dialect == SqlDialect::SPARK
		                     ? " LATERAL VIEW explode(sequence(CAST(1 AS BIGINT), CAST(abs(c." + weight +
		                           ") AS BIGINT))) repetitions AS repetition"
		                     : " CROSS JOIN UNNEST(range(CAST(abs(c." + weight + ") AS BIGINT))) repetitions";
		sql += "INSERT INTO " + delta + " (" + column_list + ", openivm_multiplicity, openivm_timestamp) SELECT " +
		       StringUtil::Join(delta_columns, ", ") + ", CAST(sign(c." + weight + ") AS INTEGER), " + timestamp +
		       " FROM " + changes + " c" + expansion + " WHERE EXISTS (SELECT 1 FROM " + metadata_table +
		       " WHERE table_name = '" + SqlUtils::EscapeValue(delta_name) + "');\n";
	}
	// Replace only changed bags. DuckLake keeps its table identity and records the
	// modifications in snapshots; native consumers read the explicit signed delta.
	sql += "DELETE FROM " + visible + " AS v WHERE EXISTS (SELECT 1 FROM " + changes + " d WHERE " + equality + ");\n";
	sql += "INSERT INTO " + visible + " SELECT v.* FROM " + next + " v WHERE EXISTS (SELECT 1 FROM " + changes +
	       " d WHERE " + equality + ");\n";
	sql += "DROP TABLE " + changes + ";\nDROP TABLE " + next + ";\n";
	if (!ducklake) {
		sql += RefreshMetadata::BuildDeltaCleanupSQL(delta, delta_name, metadata_table);
	}
	OPENIVM_DEBUG_PRINT("[PUBLISH] Compiled visible-row publication for %s\n", view_name.c_str());
	return sql;
}

} // namespace duckdb
