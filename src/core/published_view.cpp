#include "core/published_view.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

string PublishedViewName(const string &view_name) {
	return string(openivm::VISIBLE_TABLE_PREFIX) + view_name;
}

bool IsSnapshotPublication(ClientContext &context, const string &catalog, const string &schema,
                           const string &view_name) {
	bool snapshot_publication = false;
	auto lookup = [&]() {
		auto entry = Catalog::GetEntry(context, catalog, schema,
		                               EntryLookupInfo(CatalogType::VIEW_ENTRY, PublishedViewName(view_name)),
		                               OnEntryNotFound::RETURN_NULL);
		snapshot_publication = entry && entry->type == CatalogType::VIEW_ENTRY;
	};
	if (context.transaction.HasActiveTransaction()) {
		lookup();
	} else {
		context.RunFunctionInTransaction(lookup);
	}
	return snapshot_publication;
}

void WarnSnapshotHistoryDeletion(ClientContext &context, LogicalOperator &plan) {
	for (auto &child : plan.children) {
		WarnSnapshotHistoryDeletion(context, *child);
	}
	if (plan.type != LogicalOperatorType::LOGICAL_GET) {
		return;
	}
	auto &get = plan.Cast<LogicalGet>();
	if (get.function.name != "ducklake_expire_snapshots" && get.function.name != "ducklake_cleanup_old_files") {
		return;
	}
	auto dry_run = get.named_parameters.find("dry_run");
	if (dry_run != get.named_parameters.end() && !dry_run->second.IsNull() && dry_run->second.GetValue<bool>()) {
		return;
	}
	D_ASSERT(!get.parameters.empty());
	auto catalog = get.parameters[0].ToString();
	Connection con(*context.db);
	vector<string> affected;
	for (auto &location : RefreshMetadata::MetadataLocations(con)) {
		auto views = con.Query("SELECT view_name, view_schema, COALESCE(view_sql_name, view_name) FROM " +
		                       location.Table(openivm::VIEWS_TABLE) + " WHERE lower(view_catalog) = lower('" +
		                       SqlUtils::EscapeValue(catalog) + "')");
		if (views->HasError()) {
			throw CatalogException("Could not check snapshot publication before history deletion: %s",
			                       views->GetError());
		}
		for (idx_t row = 0; row < views->RowCount(); row++) {
			auto schema = views->GetValue(1, row).ToString();
			if (IsSnapshotPublication(context, catalog, schema, views->GetValue(0, row).ToString())) {
				affected.push_back(SqlUtils::FullName(catalog, schema, views->GetValue(2, row).ToString()));
			}
		}
	}
	if (!affected.empty()) {
		Printer::Print(
		    "Warning: " + get.function.name + " on " + SqlUtils::QuoteIdentifier(catalog) +
		    " may delete snapshot history required by OpenIVM materialized views: " + StringUtil::Join(affected, ", ") +
		    ". Retain their pinned snapshots; deleting them can break reads. This operation is not blocked.");
	}
}

string BuildSnapshotPublicationSQL(Connection &con, const string &catalog, const string &published,
                                   const string &data_table, const string &query) {
	// Use this writer's commit, not a newer commit from another catalog writer.
	// An empty refresh can have no commit of its own; its maintenance is
	// unchanged.
	auto quoted_catalog = SqlUtils::QuoteIdentifier(catalog);
	auto snapshot =
	    con.Query("SELECT COALESCE((SELECT id FROM " + quoted_catalog +
	              ".last_committed_snapshot()), (SELECT id FROM " + quoted_catalog + ".current_snapshot()))");
	if (snapshot->HasError() || snapshot->RowCount() != 1 || snapshot->GetValue(0, 0).IsNull()) {
		throw CatalogException("Could not resolve committed snapshot for publication '%s'", published);
	}
	auto pinned = SqlUtils::ReplaceTableReferences(
	    query, data_table, data_table + " AT (VERSION => " + snapshot->GetValue(0, 0).ToString() + ")");
	if (pinned == query) {
		throw InternalException("Snapshot publication query does not reference its maintenance table");
	}
	OPENIVM_DEBUG_PRINT("[PUBLISH] Pinning %s to committed maintenance snapshot\n", published.c_str());
	return "CREATE OR REPLACE VIEW " + published + " AS " + pinned;
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
                           const vector<string> &scope_columns, const string &timestamp_sql, SqlDialect dialect,
                           const string &appended_rows, const string &scope_rows,
                           const vector<MetadataLocation> &metadata_locations) {
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
	if (!appended_rows.empty()) {
		// The projection compiler supplies the exact rows appended to maintenance
		// state, including bag expansion and the refresh timestamp filter. Reuse
		// them instead of scanning and diffing the entire published relation.
		vector<string> projection;
		for (auto &column : columns) {
			projection.push_back(column == openivm::PUBLISHED_ORDINAL_COL ? "CAST(0 AS BIGINT) AS " + quote(column)
			                                                              : quote(column));
		}
		auto rows = "SELECT " + StringUtil::Join(projection, ", ") + " FROM (" + appended_rows + ") appended_rows";
		string sql = "INSERT INTO " + visible + " " + rows + ";\n";
		if (!ducklake) {
			auto timestamp = timestamp_sql.empty() ? openivm::UTC_NOW_SQL : timestamp_sql;
			sql += "INSERT INTO " + delta + " (" + column_list +
			       ", openivm_multiplicity, openivm_timestamp) SELECT *, 1::INTEGER, " + timestamp + " FROM (" + rows +
			       ") published_rows WHERE EXISTS (SELECT 1 FROM " + metadata_table + " WHERE table_name = '" +
			       SqlUtils::EscapeValue(delta_name) + "');\n";
			sql +=
			    RefreshMetadata::BuildDeltaCleanupSQL(delta, delta_name, metadata_table, nullptr, metadata_locations);
		}
		OPENIVM_DEBUG_PRINT("[PUBLISH] Appending visible projection delta for %s\n", view_name.c_str());
		return sql;
	}
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
		OPENIVM_DEBUG_PRINT("[PUBLISH] Scoping %s by %zu affected columns\n", view_name.c_str(), scope_columns.size());
		auto raw_delta = scope_rows.empty() ? prefix + quote(SqlUtils::DeltaName(view_name)) : scope_rows;
		// The native executor disables deliminator for deeply nested maintenance
		// SQL. Express the semijoin directly to avoid grouping all target rows
		// merely to decorrelate an EXISTS predicate.
		auto predicate = dialect == SqlDialect::DUCKDB
		                     ? " SEMI JOIN " + raw_delta + " d ON " + scope
		                     : " WHERE EXISTS (SELECT 1 FROM " + raw_delta + " d WHERE " + scope + ")";
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
	if (dialect == SqlDialect::DUCKDB) {
		sql += "DELETE FROM " + visible + " AS v USING " + changes + " d WHERE " + equality + ";\n";
		sql += "INSERT INTO " + visible + " SELECT v.* FROM " + next + " v SEMI JOIN " + changes + " d ON " + equality +
		       ";\n";
	} else {
		sql +=
		    "DELETE FROM " + visible + " AS v WHERE EXISTS (SELECT 1 FROM " + changes + " d WHERE " + equality + ");\n";
		sql += "INSERT INTO " + visible + " SELECT v.* FROM " + next + " v WHERE EXISTS (SELECT 1 FROM " + changes +
		       " d WHERE " + equality + ");\n";
	}
	sql += "DROP TABLE " + changes + ";\nDROP TABLE " + next + ";\n";
	if (!ducklake) {
		sql += RefreshMetadata::BuildDeltaCleanupSQL(delta, delta_name, metadata_table, nullptr, metadata_locations);
	}
	OPENIVM_DEBUG_PRINT("[PUBLISH] Compiled visible-row publication for %s\n", view_name.c_str());
	return sql;
}

} // namespace duckdb
