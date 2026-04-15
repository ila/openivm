#include "core/openivm_parser.hpp"

#include "core/ivm_checker.hpp"
#include "core/ivm_plan_rewrite.hpp"
#include "core/openivm_constants.hpp"
#include "lpts_pipeline.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include "core/openivm_debug.hpp"

#include <regex>

namespace duckdb {

static unique_ptr<FunctionData> IVMDDLBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// DDL statements are passed via result.parameters from the plan function.
	if (!input.inputs.empty()) {
		auto &db = DatabaseInstance::GetDatabase(context);
		Connection conn(db);
		for (auto &param : input.inputs) {
			auto q = param.GetValue<string>();
			if (q.empty()) {
				continue;
			}
			auto r = conn.Query(q);
			if (r->HasError()) {
				throw CatalogException("Failed to execute IVM DDL: " + r->GetError());
			}
		}
	}
	names.emplace_back("MATERIALIZED VIEW CREATION");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return make_uniq<IVMFunction::IVMBindData>(true);
}

static void IVMDDLExecuteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<IVMFunction::IVMBindData>();
	auto &gdata = dynamic_cast<IVMFunction::IVMGlobalData &>(*data_p.global_state);
	if (gdata.offset >= 1) {
		return;
	}
	output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
	output.SetCardinality(1);
	gdata.offset++;
}

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	auto query_lower = OpenIVMUtils::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	query_lower.erase(remove(query_lower.begin(), query_lower.end(), '\n'), query_lower.end());
	OpenIVMUtils::RemoveRedundantWhitespaces(query_lower);

	// Handle ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' | SET REFRESH MANUAL
	if (StringUtil::Contains(query_lower, "alter materialized view")) {
		std::regex alter_re("alter\\s+materialized\\s+view\\s+(\"(?:[^\"]+)\"|[a-zA-Z0-9_.]+)\\s+set\\s+refresh\\s+("
		                    "every\\s+'([^']+)'|manual)",
		                    std::regex::icase);
		std::smatch match;
		if (!std::regex_search(query_lower, match, alter_re)) {
			throw ParserException("Invalid ALTER MATERIALIZED VIEW syntax. "
			                      "Expected: ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' "
			                      "or ALTER MATERIALIZED VIEW <name> SET REFRESH MANUAL");
		}
		string alter_view_name = match[1].str();
		if (alter_view_name.size() >= 2 && alter_view_name.front() == '"' && alter_view_name.back() == '"') {
			alter_view_name = alter_view_name.substr(1, alter_view_name.size() - 2);
		}
		string refresh_type = StringUtil::Lower(match[2].str());
		string update_sql;
		if (refresh_type == "manual") {
			update_sql = "UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_interval = NULL WHERE view_name = '" +
			             OpenIVMUtils::EscapeSingleQuotes(alter_view_name) + "'";
		} else {
			int64_t interval = OpenIVMUtils::ParseRefreshInterval(match[3].str());
			update_sql = "UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_interval = " + to_string(interval) +
			             " WHERE view_name = '" + OpenIVMUtils::EscapeSingleQuotes(alter_view_name) + "'";
		}
		// Pass the UPDATE SQL through IVMParseData; IVMPlanFunction will execute it
		Parser alter_parser;
		alter_parser.ParseQuery("SELECT 1");
		auto parse_data =
		    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(alter_parser.statements[0]), true);
		dynamic_cast<IVMParseData &>(*parse_data).alter_sql = update_sql;
		return ParserExtensionParseResult(std::move(parse_data));
	}

	if (!StringUtil::Contains(query_lower, "create materialized view") &&
	    !StringUtil::Contains(query_lower, "create or replace materialized view")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	// Detect CREATE OR REPLACE MATERIALIZED VIEW
	bool is_replace = false;
	std::regex or_replace_re("\\bcreate\\s+or\\s+replace\\s+materialized\\s+view\\b", std::regex::icase);
	if (std::regex_search(query_lower, or_replace_re)) {
		is_replace = true;
		// Strip "or replace" so the rest of the pipeline sees "create materialized view"
		query_lower = std::regex_replace(query_lower, std::regex("\\bor\\s+replace\\s+"), "");
		OpenIVMUtils::RemoveRedundantWhitespaces(query_lower);
	}

	// Extract REFRESH EVERY clause before structural rewrite (strips it from the query)
	int64_t refresh_interval = OpenIVMUtils::ExtractRefreshInterval(query_lower);
	OPENIVM_DEBUG_PRINT("[CREATE MV] Refresh interval: %lld seconds\n", (long long)refresh_interval);

	OpenIVMUtils::ReplaceMaterializedView(query_lower);
	// All other rewrites (DISTINCT, AVG, LEFT JOIN key, aggregate aliases) are done
	// at the plan level in IVMPlanFunction via IVMPlanRewrite + LPTS.
	OPENIVM_DEBUG_PRINT("[CREATE MV] After structural rewrite: %s\n", query_lower.c_str());

	Parser p;
	p.ParseQuery(query_lower);

	auto parse_data =
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(p.statements[0]), true, refresh_interval);
	dynamic_cast<IVMParseData &>(*parse_data).is_replace = is_replace;
	return ParserExtensionParseResult(std::move(parse_data));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	ParserExtensionPlanResult result;

	if (ivm_parse_data.plan) {
		Connection con(*context.db.get());

		// Handle ALTER MATERIALIZED VIEW — just execute the metadata UPDATE
		if (!ivm_parse_data.alter_sql.empty()) {
			auto r = con.Query(ivm_parse_data.alter_sql);
			if (r->HasError()) {
				throw CatalogException("Failed to alter materialized view: " + r->GetError());
			}
			// Return via the DDL executor with no DDL to run (the UPDATE already executed)
			result.function =
			    TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction, IVMFunction::IVMInit);
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			return result;
		}

		// TODO: Remove PAC coupling — IVM should not need to forward PAC settings.
		// Check if PAC extension is loaded (needed later for delta table queries).
		// If so, forward PAC settings to the internal connection so that PAC
		// compilation (noise, seeds, etc.) behaves the same as the user's session.
		Value pac_check_val;
		bool pac_loaded = context.TryGetCurrentSetting("pac_check", pac_check_val);
		if (pac_loaded) {
			for (auto &name : {"pac_mi", "pac_seed", "pac_m", "pac_noise", "pac_hash_repair", "pac_check",
			                   "pac_rewrite", "pac_conservative_mode"}) {
				Value val;
				if (context.TryGetCurrentSetting(name, val) && !val.IsNull()) {
					con.Query("SET " + string(name) + " = " + val.ToString());
				}
			}
		}

		auto full_view_name = OpenIVMUtils::ExtractTableName(statement->query);
		auto original_view_query = OpenIVMUtils::ExtractViewQuery(statement->query);

		// Split catalog-qualified name (e.g. "dl.mv_totals") into prefix and bare name.
		// The prefix (e.g. "dl.") is used to create internal tables in the same catalog.
		string view_catalog_prefix; // e.g. "dl." or "" for default catalog
		string view_name;           // bare name without catalog, e.g. "mv_totals"
		auto dot_pos = full_view_name.rfind('.');
		if (dot_pos != string::npos) {
			view_catalog_prefix = full_view_name.substr(0, dot_pos + 1); // includes the dot
			view_name = full_view_name.substr(dot_pos + 1);
		} else {
			view_name = full_view_name;
		}
		string view_query = original_view_query; // will be overwritten by LPTS for DDL

		// Use con for planning — sees all committed state from previous bind-phase DDL
		con.BeginTransaction();
		auto table_names = con.GetTableNames(statement->query);

		// Plan the full CREATE TABLE AS SELECT statement (for plan walking)
		Planner planner(*con.context);
		planner.CreatePlan(statement->Copy());
		auto plan = std::move(planner.plan);

		// Plan the raw SELECT query separately for IVM plan rewrite + LPTS conversion
		vector<string> output_names;
		string having_predicate; // HAVING predicate as SQL (for VIEW WHERE clause, empty if no HAVING)
		{
			Parser select_parser;
			select_parser.ParseQuery(original_view_query);
			Planner select_planner(*con.context);
			select_planner.CreatePlan(std::move(select_parser.statements[0]));
			auto select_plan = std::move(select_planner.plan);

			// Apply IVM plan rewrites (DISTINCT → GROUP BY + COUNT, AVG → SUM + COUNT, LEFT JOIN key)
			IVMPlanRewrite(context, *select_planner.binder, select_plan, select_planner.names);

			// Sanitize column names: replace special chars with underscores, collapse runs, trim.
			// "min(val)" → "min_val", "count_star()" → "count_star", "SUM(x) AS total" → "total"
			output_names = select_planner.names;
			for (auto &name : output_names) {
				// Don't sanitize internal IVM column names — they need the _ivm_ prefix
				if (IVMTableNames::IsInternalColumn(name)) {
					continue;
				}
				string clean;
				bool last_was_underscore = false;
				for (auto c : name) {
					if (isalnum(c)) {
						clean += c;
						last_was_underscore = false;
					} else if (!last_was_underscore && !clean.empty()) {
						clean += '_';
						last_was_underscore = true;
					}
				}
				// Trim trailing underscore
				if (!clean.empty() && clean.back() == '_') {
					clean.pop_back();
				}
				if (!clean.empty()) {
					name = clean;
				}
			}
			// IVMPlanRewrite may have added extra columns (_ivm_left_key, _ivm_distinct_count).
			// Append names for these from the top projection's expression aliases.
			auto plan_bindings = select_plan->GetColumnBindings();
			if (select_plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &proj = select_plan->Cast<LogicalProjection>();
				while (output_names.size() < plan_bindings.size()) {
					idx_t idx = output_names.size();
					if (idx < proj.expressions.size() && !proj.expressions[idx]->alias.empty()) {
						output_names.push_back(proj.expressions[idx]->alias);
					} else {
						output_names.push_back("_ivm_col_" + to_string(idx));
					}
				}
			}
			// Strip HAVING filter from plan — data table stores all groups.
			// The predicate is extracted as SQL (using output aliases) for the VIEW WHERE clause.
			having_predicate = StripHavingFilter(select_plan, output_names);

			auto ast = LogicalPlanToAst(*con.context, select_plan);
			auto cte_list = AstToCteList(*ast);
			view_query = cte_list->ToQuery(true, output_names);
			if (!view_query.empty() && view_query.back() == ';') {
				view_query.pop_back();
			}
			StringUtil::Trim(view_query);
			OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS view query: %s\n", view_query.c_str());
		}
		con.Rollback();

		OPENIVM_DEBUG_PRINT("[CREATE MV] View name: %s\n", view_name.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] View query: %s\n", view_query.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Logical plan:\n%s\n", plan->ToString().c_str());

		// Single-pass plan analysis: validates IVM compatibility AND extracts metadata
		auto analysis = AnalyzePlan(plan.get());
		bool ivm_compatible = analysis.ivm_compatible;
		bool found_aggregation = analysis.found_aggregation;
		bool found_projection = analysis.found_projection;
		bool found_distinct = analysis.found_distinct;
		bool found_having = analysis.found_having;
		bool found_minmax = analysis.found_minmax;
		bool found_left_join = analysis.found_left_join;
		auto aggregate_columns = std::move(analysis.aggregate_columns);
		auto aggregate_types = std::move(analysis.aggregate_types);

		// Fix expression-based group-by column names: the plan walk may extract
		// "abs(val)" but the MV table column is "abs_val" (from the AS alias).
		for (auto &col : aggregate_columns) {
			if (col.find('(') != string::npos) {
				string lower_col = StringUtil::Lower(col);
				if (lower_col.size() >= 2 && lower_col.front() == '"' && lower_col.back() == '"') {
					lower_col = lower_col.substr(1, lower_col.size() - 2);
				}
				auto pos = view_query.find(lower_col);
				if (pos != string::npos) {
					auto after = view_query.substr(pos + lower_col.size());
					std::regex alias_re(R"(^\s+as\s+(\w+))", std::regex_constants::icase);
					std::smatch m;
					if (std::regex_search(after, m, alias_re)) {
						col = m[1].str();
					}
				}
			}
		}

		IVMType ivm_type;

		if (!ivm_compatible) {
			ivm_type = IVMType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' uses constructs not supported for incremental maintenance. "
			               "Full refresh will be used.");
		} else if (found_distinct) {
			ivm_type = IVMType::AGGREGATE_GROUP;
		} else if (found_having && found_aggregation && !aggregate_columns.empty()) {
			ivm_type = IVMType::AGGREGATE_HAVING;
		} else if (found_aggregation && !aggregate_columns.empty()) {
			ivm_type = IVMType::AGGREGATE_GROUP;
		} else if (found_aggregation && aggregate_columns.empty()) {
			ivm_type = IVMType::SIMPLE_AGGREGATE;
		} else if (found_projection && !found_aggregation) {
			ivm_type = IVMType::SIMPLE_PROJECTION;
		} else {
			ivm_type = IVMType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' has an unrecognized query pattern. Full refresh will be used.");
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Detected IVM type: %s (aggregation=%d, projection=%d, group_cols=%zu)\n",
		                    ivm_type == IVMType::AGGREGATE_GROUP     ? "AGGREGATE_GROUP"
		                    : ivm_type == IVMType::SIMPLE_AGGREGATE  ? "SIMPLE_AGGREGATE"
		                    : ivm_type == IVMType::SIMPLE_PROJECTION ? "SIMPLE_PROJECTION"
		                    : ivm_type == IVMType::FULL_REFRESH      ? "FULL_REFRESH"
		                                                             : "UNKNOWN",
		                    (int)found_aggregation, (int)found_projection, aggregate_columns.size());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Source tables:");
		for (const auto &t : table_names) {
			OPENIVM_DEBUG_PRINT(" %s", t.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n");

		// Build DDL vector directly in memory
		vector<string> ddl;

		// --- System tables DDL ---
		ddl.push_back("create table if not exists " + string(ivm::VIEWS_TABLE) +
		              " (view_name varchar primary key, sql_string varchar, type tinyint,"
		              " has_minmax boolean default false, has_left_join boolean default false,"
		              " last_update timestamp, refresh_interval bigint default null,"
		              " refresh_in_progress boolean default false,"
		              " group_columns varchar default null,"
		              " aggregate_types varchar default null,"
		              " having_predicate varchar default null)");

		// Refresh hooks: extensions can register custom SQL to run on MV refresh
		// mode: 'replace' (instead of ivm), 'before' (before ivm), 'after' (after ivm)
		ddl.push_back("create table if not exists _duckdb_ivm_refresh_hooks"
		              " (view_name varchar primary key, hook_sql varchar not null,"
		              " mode varchar not null default 'after')");

		ddl.push_back("create table if not exists " + string(ivm::DELTA_TABLES_TABLE) +
		              " (view_name varchar, table_name varchar, last_update timestamp,"
		              " catalog_type varchar default 'duckdb', last_snapshot_id bigint default null,"
		              " primary key(view_name, table_name))");

		// Refresh history: stores execution stats for learned cost model calibration
		ddl.push_back("create table if not exists " + string(ivm::HISTORY_TABLE) +
		              " (view_name varchar, refresh_timestamp timestamp default current_timestamp,"
		              " method varchar, ivm_compute_est double, ivm_upsert_est double,"
		              " recompute_compute_est double, recompute_replace_est double,"
		              " actual_duration_ms bigint,"
		              " primary key(view_name, refresh_timestamp))");

		// --- OR REPLACE: drop old MV if it exists ---
		if (ivm_parse_data.is_replace) {
			string qvn_drop = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
			string qdt_drop =
			    view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(view_name));
			string qdv_drop =
			    view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
			// Drop the user-facing VIEW, data table, and delta view table
			ddl.push_back("DROP VIEW IF EXISTS " + qvn_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdt_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdv_drop);
			// Clean metadata (the INSERT OR REPLACE below handles _duckdb_ivm_views)
			ddl.push_back("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
			              OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			ddl.push_back("DELETE FROM " + string(ivm::HISTORY_TABLE) + " WHERE view_name = '" +
			              OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		// Store the LPTS query in metadata — it has hidden columns (DISTINCT count, AVG sum/count,
		// LEFT JOIN key) and preserves user column names.
		string refresh_val = ivm_parse_data.refresh_interval > 0 ? to_string(ivm_parse_data.refresh_interval) : "null";
		// Store GROUP BY columns so CompileAggregateGroups can work without an index (e.g. DuckLake)
		string group_cols_val = "null";
		if (!aggregate_columns.empty()) {
			group_cols_val = "'";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				if (i > 0) {
					group_cols_val += ",";
				}
				group_cols_val += OpenIVMUtils::EscapeSingleQuotes(aggregate_columns[i]);
			}
			group_cols_val += "'";
		}
		// Store per-column aggregate types for insert-only MIN/MAX optimization
		string agg_types_val = "null";
		if (!aggregate_types.empty()) {
			agg_types_val = "'";
			for (size_t i = 0; i < aggregate_types.size(); i++) {
				if (i > 0) {
					agg_types_val += ",";
				}
				agg_types_val += aggregate_types[i];
			}
			agg_types_val += "'";
		}
		string having_val =
		    having_predicate.empty() ? "null" : "'" + OpenIVMUtils::EscapeSingleQuotes(having_predicate) + "'";
		ddl.push_back("insert or replace into " + string(ivm::VIEWS_TABLE) + " values ('" + view_name + "', '" +
		              OpenIVMUtils::EscapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) + ", " +
		              (found_minmax ? "true" : "false") + ", " + (found_left_join ? "true" : "false") + ", now(), " +
		              refresh_val + ", false, " + group_cols_val + ", " + agg_types_val + ", " + having_val + ")");

		// Classify each base table by catalog type (duckdb vs ducklake).
		// DuckLake tables use native change tracking; DuckDB tables use delta tables.
		unordered_set<string> ducklake_tables;
		for (const auto &table_name : table_names) {
			string catalog_type = "duckdb";
			string snapshot_val = "null";
			string meta_table_name = OpenIVMUtils::DeltaName(table_name);

			// Look up the table in all available catalogs to detect its type.
			// GetTableNames() returns unqualified names, so we search the catalog path
			// and also check attached catalogs explicitly.
			con.BeginTransaction();
			auto entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name,
			                                                  OnEntryNotFound::RETURN_NULL);
			// If not found in default search path, search all attached catalogs
			if (!entry) {
				auto &db_manager = DatabaseManager::Get(*con.context);
				auto databases = db_manager.GetDatabases(*con.context);
				for (auto &db : databases) {
					if (entry) {
						break;
					}
					auto &cat_name = db->GetName();
					auto found = Catalog::GetEntry<TableCatalogEntry>(*con.context, cat_name, DEFAULT_SCHEMA,
					                                                  table_name, OnEntryNotFound::RETURN_NULL);
					if (found) {
						entry = found;
					}
				}
			}
			if (entry) {
				string cat_type = entry->ParentCatalog().GetCatalogType();
				if (cat_type == "ducklake") {
					catalog_type = "ducklake";
					meta_table_name = table_name; // no delta table — store base table name
					ducklake_tables.insert(table_name);

					// Get current snapshot ID from DuckLake catalog
					string cat_name = entry->ParentCatalog().GetName();
					con.Rollback();
					auto snap_result = con.Query("SELECT id FROM " + cat_name + ".current_snapshot()");
					if (!snap_result->HasError() && snap_result->RowCount() > 0) {
						snapshot_val = snap_result->GetValue(0, 0).ToString();
					}
				} else {
					con.Rollback();
				}
			} else {
				con.Rollback();
			}

			ddl.push_back("insert into " + string(ivm::DELTA_TABLES_TABLE) + " values ('" + view_name + "', '" +
			              OpenIVMUtils::EscapeSingleQuotes(meta_table_name) + "', now(), '" + catalog_type + "', " +
			              snapshot_val + ")");
		}

		// --- Compiled DDL (MV creation, delta tables, delta view) ---
		// Physical data table stores all columns (including _ivm_* internal cols).
		// All internal tables are created in the same catalog as the MV.
		string data_table = IVMTableNames::DataTableName(view_name);
		string qdt = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table);
		string qvn = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
		ddl.push_back("create table " + qdt + " as " + view_query);
		if (pac_loaded) {
			ddl.push_back("SET pac_check = false");
			ddl.push_back("SET pac_rewrite = false");
		}

		// User-facing VIEW hides internal _ivm_* columns via EXCLUDE
		{
			// Collect internal column names from the LPTS output
			vector<string> internal_cols;
			for (auto &name : output_names) {
				if (IVMTableNames::IsInternalColumn(name)) {
					internal_cols.push_back(name);
				}
			}
			string having_where = having_predicate.empty() ? "" : " where " + having_predicate;
			if (internal_cols.empty()) {
				ddl.push_back("create view " + qvn + " as select * from " + qdt + having_where);
			} else {
				string exclude_list;
				for (size_t i = 0; i < internal_cols.size(); i++) {
					if (i > 0) {
						exclude_list += ", ";
					}
					exclude_list += internal_cols[i];
				}
				ddl.push_back("create view " + qvn + " as select * exclude (" + exclude_list + ") from " + qdt +
				              having_where);
			}
		}

		for (const auto &table_name : table_names) {
			// DuckLake tables don't need delta tables — change tracking is native
			if (ducklake_tables.count(table_name)) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Skipping delta table for DuckLake table '%s'\n", table_name.c_str());
				continue;
			}

			Value catalog_value;
			Value schema_value;

			if (catalog_value.IsNull() && !context.db->config.options.database_path.empty()) {
				// Look up the catalog name for this table via Catalog API
				con.BeginTransaction();
				auto entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA,
				                                                  table_name, OnEntryNotFound::RETURN_NULL);
				if (entry) {
					catalog_value = Value(entry->ParentCatalog().GetName());
				}
				con.Rollback();
			}
			if (catalog_value.IsNull()) {
				catalog_value = Value("memory");
			}

			if (schema_value.IsNull()) {
				schema_value = Value("main");
			}

			auto catalog_schema = catalog_value.ToString() + "." + schema_value.ToString() + ".";

			ddl.push_back("create table if not exists " + catalog_schema + OpenIVMUtils::DeltaName(table_name) +
			              " as select *, true as " + string(ivm::MULTIPLICITY_COL) + ", now()::timestamp as " +
			              string(ivm::TIMESTAMP_COL) + " from " + catalog_schema + table_name + " limit 0");
		}

		// Delta table for the MV — based on the DATA table (has all columns)
		string qdv = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
		ddl.push_back("create table if not exists " + qdv + " as select *, true as " + string(ivm::MULTIPLICITY_COL) +
		              ", now()::timestamp as " + string(ivm::TIMESTAMP_COL) + " from " + qdt + " limit 0");
		ddl.push_back("alter table " + qdv + " alter " + string(ivm::TIMESTAMP_COL) + " set default now()");

		// --- Index DDL (for aggregate group queries) ---
		// DuckLake does not support indexes, so skip for DuckLake-backed MVs.
		if ((ivm_type == IVMType::AGGREGATE_GROUP || ivm_type == IVMType::AGGREGATE_HAVING) &&
		    ducklake_tables.empty()) {
			string index_name = KeywordHelper::WriteOptionallyQuoted(data_table + ivm::INDEX_SUFFIX);
			string index_query_view = "create unique index " + index_name + " on " + qdt + "(";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				index_query_view += KeywordHelper::WriteOptionallyQuoted(aggregate_columns[i]);
				if (i != aggregate_columns.size() - 1) {
					index_query_view += ", ";
				}
			}
			index_query_view += ")";
			ddl.push_back(index_query_view);
		}

		// After all tables are created and populated, update DuckLake snapshot IDs
		// to the current snapshot. This ensures the first refresh only sees changes
		// made AFTER the MV was created (not the initial data load).
		for (const auto &table_name : table_names) {
			if (ducklake_tables.count(table_name)) {
				string cat_name = view_catalog_prefix.empty()
				                      ? "memory"
				                      : view_catalog_prefix.substr(0, view_catalog_prefix.size() - 1);
				// Remove trailing dot and extract catalog name
				auto dot = cat_name.rfind('.');
				if (dot != string::npos) {
					cat_name = cat_name.substr(0, dot);
				}
				ddl.push_back("UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = (SELECT id FROM " +
				              cat_name + ".current_snapshot()) WHERE view_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(view_name) + "' AND table_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(table_name) + "'");
			}
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Compiled %lu DDL queries for bind phase\n", (unsigned long)ddl.size());

		// Write reference SQL files if ivm_files_path is set
		Value files_path_val;
		if (context.TryGetCurrentSetting("ivm_files_path", files_path_val) && !files_path_val.IsNull()) {
			string base_path = files_path_val.ToString();
			// System tables DDL (first 3 statements: _duckdb_ivm_views, _duckdb_ivm_refresh_hooks,
			// _duckdb_ivm_delta_tables)
			string system_tables_sql;
			// Compiled queries (everything after the system tables)
			string compiled_sql;
			for (size_t i = 0; i < ddl.size(); i++) {
				if (i < 3) {
					system_tables_sql += ddl[i] + ";\n\n";
				} else {
					compiled_sql += ddl[i] + ";\n\n";
				}
			}
			OpenIVMUtils::WriteFile(base_path + "/ivm_system_tables.sql", false, system_tables_sql);
			OpenIVMUtils::WriteFile(base_path + "/ivm_compiled_queries_" + view_name + ".sql", false, compiled_sql);
		}

		// Pass DDL via result.parameters — the bind function receives them as input.inputs.
		// This replaces the fragile thread-local g_ivm_pending_ddl mechanism.
		for (auto &q : ddl) {
			result.parameters.push_back(Value(q));
		}
	}

	// Return DDL executor table function
	result.function =
	    TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction, IVMFunction::IVMInit);
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}
} // namespace duckdb
