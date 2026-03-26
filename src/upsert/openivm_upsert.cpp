#include "upsert/openivm_upsert.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_metadata.hpp"
#include "duckdb/main/client_data.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_compile_upsert.hpp"
#include "upsert/openivm_cost_model.hpp"
#include "logical_plan_to_sql.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

static string BuildRecomputeQuery(IVMMetadata &metadata, const string &view_name, const string &view_query_sql,
                                  bool cross_system, const string &attached_catalog = "",
                                  const string &attached_schema = "") {
	string query = "DELETE FROM " + view_name + ";\n";
	query += "INSERT INTO " + view_name + " " + view_query_sql + ";\n\n";

	metadata.UpdateTimestamp(view_name);
	string update_ts = "UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_update = now() WHERE view_name = '" +
	                   OpenIVMUtils::EscapeValue(view_name) + "';\n";

	string delta_cleanup;
	auto delta_tables = metadata.GetDeltaTables(view_name);
	for (auto &dt : delta_tables) {
		string resolved = dt;
		if (cross_system) {
			resolved = attached_catalog + "." + attached_schema + "." + dt;
		}
		delta_cleanup += "DELETE FROM " + resolved + " WHERE " + string(ivm::TIMESTAMP_COL) +
		                 " < (SELECT min(last_update) FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                 " WHERE table_name = '" + OpenIVMUtils::EscapeValue(dt) + "');\n";
	}

	return query + update_ts + "\n" + delta_cleanup;
}

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters) {
	// queries to run in order to materialize IVM upserts
	// these are executed whenever the pragma ivm_upsert is called
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();

	string view_catalog_name;
	string view_schema_name;
	string attached_db_catalog_name;
	string attached_db_schema_name;
	string view_name;
	bool cross_system = false; // to make checks easier
	// if we are in a cross-system scenario, the tables need to be stored separately
	// ex. the delta tables are on the attached database, while the delta views on DuckDB

	// extracting the query from the view definition
	Connection con(*context.db.get());

	if (parameters.values.size() == 3) {
		// ivm_options was called, so different schema and catalog
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		view_name = StringValue::Get(parameters.values[2]);
	} else if (parameters.values.size() == 5) {
		// ivm_cross_system was called, so different schema and catalog
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		attached_db_catalog_name = StringValue::Get(parameters.values[2]);
		attached_db_schema_name = StringValue::Get(parameters.values[3]);
		view_name = StringValue::Get(parameters.values[4]);
		cross_system = true;
	} else {
		// simple ivm, use current catalog and schema from the search path
		auto &search_path = ClientData::Get(context).catalog_search_path;
		auto default_entry = search_path->GetDefault();
		view_catalog_name = default_entry.catalog;
		view_schema_name = default_entry.schema;
		view_name = StringValue::Get(parameters.values[0]);
	}

	// Use con's transaction for catalog access — sees all committed state
	con.BeginTransaction();
	auto &con_ctx = *con.context;
	auto delta_view_catalog_entry = catalog.GetEntry<TableCatalogEntry>(
	    con_ctx, view_catalog_name, view_schema_name, OpenIVMUtils::DeltaName(view_name),
	    OnEntryNotFound::THROW_EXCEPTION, error_context);
	auto index_delta_view_catalog_entry =
	    Catalog::GetEntry(con_ctx, view_catalog_name, view_schema_name,
	                      EntryLookupInfo(CatalogType::INDEX_ENTRY, view_name + "_ivm_index", error_context),
	                      OnEntryNotFound::RETURN_NULL);
	con.Rollback();

	IVMMetadata metadata(con);
	auto view_query_sql = metadata.GetViewQuery(view_name);
	if (view_query_sql.empty()) {
		throw ParserException("View not found! Please call IVM with a materialized view.");
	}
	IVMType view_query_type = metadata.GetViewType(view_name);
	OPENIVM_DEBUG_PRINT("[UPSERT] View: %s, Type: %d, Query: %s\n", view_name.c_str(), (int)view_query_type,
	                    view_query_sql.c_str());

	bool has_minmax = StringUtil::Contains(view_query_sql, "min(") || StringUtil::Contains(view_query_sql, "max(");

	// Check ivm_refresh_mode: 'full' forces full recompute, skipping the IVM pipeline.
	Value refresh_mode_val;
	bool force_full_refresh = false;
	if (context.TryGetCurrentSetting("ivm_refresh_mode", refresh_mode_val) && !refresh_mode_val.IsNull()) {
		auto mode = StringUtil::Lower(refresh_mode_val.ToString());
		if (mode == "full") {
			force_full_refresh = true;
		}
	}

	if (force_full_refresh) {
		return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
		                           attached_db_schema_name);
	}

	// Adaptive cost model: estimate IVM vs full recompute cost.
	// Gated by ivm_adaptive setting (default false — always use IVM).
	Value ivm_adaptive_val;
	bool ivm_adaptive = false;
	if (context.TryGetCurrentSetting("ivm_adaptive", ivm_adaptive_val) && !ivm_adaptive_val.IsNull()) {
		ivm_adaptive = ivm_adaptive_val.GetValue<bool>();
	}
	if (ivm_adaptive) {
		con.BeginTransaction();
		Parser cost_parser;
		cost_parser.ParseQuery(view_query_sql);
		Planner cost_planner(*con.context);
		cost_planner.CreatePlan(cost_parser.statements[0]->Copy());
		Optimizer cost_optimizer(*cost_planner.binder, *con.context);
		auto cost_plan = cost_optimizer.Optimize(std::move(cost_planner.plan));

		auto cost_estimate = EstimateIVMCost(*con.context, *cost_plan, view_name);
		con.Rollback();
		if (cost_estimate.ShouldRecompute()) {
			OPENIVM_DEBUG_PRINT("[ADAPTIVE] Full recompute is cheaper — skipping IVM\n");
			return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name);
		}
	}

	// IVM path: proceed with incremental maintenance

	// first of all we need to understand the keys
	auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
	const ColumnList &delta_view_columns = delta_view_entry->GetColumns();

	auto column_names = delta_view_columns.GetColumnNames();
	// Check if the delta view has a timestamp column (present when created via CREATE MATERIALIZED VIEW)
	bool has_ts_col =
	    std::find(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL)) != column_names.end();
	// Remove _duckdb_ivm_timestamp — it's auto-filled by DEFAULT (for chained MV support)
	column_names.erase(std::remove(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL)),
	                   column_names.end());

	// Detect list mode: use element-wise list operations for LIST-typed aggregate columns
	bool list_mode = false;
	for (auto &col : delta_view_columns.Logical()) {
		if (col.GetName() != ivm::MULTIPLICITY_COL && col.GetType().id() == LogicalTypeId::LIST) {
			list_mode = true;
			break;
		}
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] List mode: %s\n", list_mode ? "true" : "false");

	string upsert_query;

	// Build a timestamp filter for the delta_view reads in the upsert query.
	// This prevents double-counting when chained MVs accumulate delta_view rows
	// from multiple refresh rounds (because downstream views haven't consumed them yet).
	// The filter uses the view's current last_update — rows inserted by the IVM query
	// in the current round have timestamps >= this value.
	string delta_ts_filter;
	if (has_ts_col) {
		auto last_update_result =
		    con.Query("SELECT last_update FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
		              OpenIVMUtils::EscapeValue(view_name) + "' LIMIT 1");
		if (!last_update_result->HasError() && last_update_result->RowCount() > 0) {
			auto ts = last_update_result->GetValue(0, 0);
			if (!ts.IsNull()) {
				delta_ts_filter = string(ivm::TIMESTAMP_COL) + " >= '" + ts.ToString() + "'::TIMESTAMP";
			}
		}
	} // has_ts_col

	// this is to compile the query to merge the materialized view with its delta version
	// depending on the query type, this procedure will be done differently
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: %s\n",
	                    view_query_type == IVMType::AGGREGATE_GROUP     ? "AGGREGATE_GROUP"
	                    : view_query_type == IVMType::SIMPLE_AGGREGATE  ? "SIMPLE_AGGREGATE"
	                    : view_query_type == IVMType::SIMPLE_PROJECTION ? "SIMPLE_PROJECTION"
	                                                                    : "UNKNOWN");
	switch (view_query_type) {
	case IVMType::AGGREGATE_GROUP: {
		upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names,
		                                      view_query_sql, has_minmax, list_mode, delta_ts_filter);
		break;
	}

	// note: simple_filter removed 2024-12-11
	case IVMType::SIMPLE_PROJECTION: {
		upsert_query = CompileProjectionsFilters(view_name, column_names, delta_ts_filter);
		break;
	}

	case IVMType::SIMPLE_AGGREGATE: {
		upsert_query =
		    CompileSimpleAggregates(view_name, column_names, view_query_sql, has_minmax, list_mode, delta_ts_filter);
		break;
	}
		// todo joins
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Upsert query:\n%s\n", upsert_query.c_str());
	// DoIVM is a table function (root of the tree)
	string ivm_query;

	// splitting the query in two to make it easier to turn into string (insertions are the same)
	string do_ivm = "select * from DoIVM('" + OpenIVMUtils::EscapeValue(view_catalog_name) + "','" +
	                OpenIVMUtils::EscapeValue(view_schema_name) + "','" + OpenIVMUtils::EscapeValue(view_name) + "');";

	con.BeginTransaction();
	auto delta_table_names = metadata.GetDeltaTables(view_name);

	// now we can plan the query
	OPENIVM_DEBUG_PRINT("[UPSERT] Planning DoIVM query: %s\n", do_ivm.c_str());
	Parser p;
	p.ParseQuery(do_ivm);

	Planner planner(*con.context);
	planner.CreatePlan(std::move(p.statements[0]));
	auto plan = std::move(planner.plan);
	OPENIVM_DEBUG_PRINT("[UPSERT] Unoptimized plan:\n%s\n", plan->ToString().c_str());
	Optimizer optimizer(*planner.binder, *con.context);
	plan = optimizer.Optimize(std::move(plan)); // this transforms the plan into an incremental plan
	OPENIVM_DEBUG_PRINT("[UPSERT] Optimized (incremental) plan:\n%s\n", plan->ToString().c_str());
	con.Rollback();

	// we turn the plan into a string using LogicalPlanToSql (replacement for LogicalPlanToString)
	LogicalPlanToSql lpts(con_ctx, plan);
	auto cte_list = lpts.LogicalPlanToCteList();
	string raw_ivm_sql = LogicalPlanToSql::CteListToSql(cte_list);

	// Use explicit column list in INSERT INTO delta_view, excluding _duckdb_ivm_timestamp
	// so the DEFAULT now() fills it in (for chained MV support)
	string delta_view_name = OpenIVMUtils::DeltaName(view_name);
	string insert_target = "INSERT INTO " + delta_view_name;
	auto insert_pos = raw_ivm_sql.find(insert_target);
	if (insert_pos != string::npos) {
		string col_list = "(";
		for (size_t i = 0; i < column_names.size(); i++) {
			if (i > 0) {
				col_list += ", ";
			}
			col_list += column_names[i];
		}
		col_list += ") ";
		raw_ivm_sql.insert(insert_pos + insert_target.size(), " " + col_list);
	}
	ivm_query += raw_ivm_sql;

	// Delete from delta view: timestamp-based if downstream views depend on it, unconditional otherwise
	auto downstream_check = con.Query("SELECT COUNT(*) FROM " + string(ivm::DELTA_TABLES_TABLE) +
	                                  " WHERE table_name = '" + OpenIVMUtils::EscapeValue(delta_view_name) + "'");
	bool has_downstream = !downstream_check->HasError() && downstream_check->RowCount() > 0 &&
	                      downstream_check->GetValue(0, 0).GetValue<int64_t>() > 0;

	// For AGGREGATE_GROUP views with downstream consumers: insert companion false/zero
	// rows for groups that already exist in the MV.  This ensures downstream COUNT(*)
	// correctly treats aggregate updates as net-zero (+1 true, -1 false) instead of +1.
	// SUM downstream is unaffected because the false rows carry 0 aggregate values.
	string companion_query;
	if (view_query_type == IVMType::AGGREGATE_GROUP && has_downstream && index_delta_view_catalog_entry) {
		auto *idx = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
		auto key_ids = idx->column_ids;
		vector<string> keys;
		unordered_set<string> keys_set;
		for (auto &kid : key_ids) {
			keys.push_back(column_names[kid]);
			keys_set.insert(column_names[kid]);
		}

		string col_list, val_list, join_cond;
		for (auto &col : column_names) {
			if (!col_list.empty()) {
				col_list += ", ";
				val_list += ", ";
			}
			col_list += col;
			if (keys_set.count(col)) {
				val_list += "d." + col;
			} else if (col == ivm::MULTIPLICITY_COL) {
				val_list += "false";
			} else {
				val_list += "0";
			}
		}
		for (size_t i = 0; i < keys.size(); i++) {
			if (i > 0) {
				join_cond += " AND ";
			}
			join_cond += "d." + keys[i] + " IS NOT DISTINCT FROM m." + keys[i];
		}

		companion_query = "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + val_list + " FROM " +
		                  delta_view_name + " d WHERE d." + string(ivm::MULTIPLICITY_COL) + " = true";
		if (!delta_ts_filter.empty()) {
			companion_query += " AND d." + delta_ts_filter;
		}
		companion_query += " AND EXISTS (SELECT 1 FROM " + view_name + " m WHERE " + join_cond + ");\n";
		OPENIVM_DEBUG_PRINT("[UPSERT] Companion query:\n%s\n", companion_query.c_str());
	}

	string delete_from_view_query;
	if (has_downstream) {
		delete_from_view_query = "DELETE FROM " + delta_view_name + " WHERE " + string(ivm::TIMESTAMP_COL) +
		                         " < (SELECT min(last_update) FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                         " WHERE table_name = '" + OpenIVMUtils::EscapeValue(delta_view_name) + "');";
	} else {
		delete_from_view_query = "DELETE FROM " + delta_view_name + ";";
	}
	string ivm_result;

	// now we can also delete from the delta table, but only if all the dependent views have been refreshed
	// example: if two views A and B are on the same table T, we can only remove tuples from T
	// if both A and B have been refreshed (up to some timestamp)
	// to check this, we extract the minimum timestamp from _duckdb_ivm_delta_tables
	string delete_from_delta_table_query;
	// firstly we reset the timestamp
	string update_timestamp_query = "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
	                                " SET last_update = now() WHERE view_name = '" +
	                                OpenIVMUtils::EscapeValue(view_name) + "';\n";

	for (auto &dt : delta_table_names) {
		string resolved = dt;
		if (cross_system) {
			resolved = attached_db_catalog_name + "." + attached_db_schema_name + "." + dt;
		}
		delete_from_delta_table_query += "DELETE FROM " + resolved + " WHERE " + string(ivm::TIMESTAMP_COL) +
		                                 " < (SELECT min(last_update) FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                                 " WHERE table_name = '" + OpenIVMUtils::EscapeValue(dt) + "');\n";
	}

	// Build the clean SQL (written to file for reference/replay)
	// Order: IVM query → upsert → update timestamps → cleanup
	// update_timestamp MUST run after upsert so the delta_ts_filter in the upsert
	// can correctly distinguish current-round vs old delta_view rows.
	string clean_query = ivm_query + "\n" + companion_query + "\n" + upsert_query + "\n" + update_timestamp_query +
	                     "\n" + delete_from_view_query + "\n" + ivm_result + "\n" + delete_from_delta_table_query;

	// now also compiling the queries for future usage
	string db_path;
	if (!context.db->config.options.database_path.empty()) {
		db_path = context.db->GetFileSystem().GetWorkingDirectory();
	} else {
		Value db_path_value;
		context.TryGetCurrentSetting("ivm_files_path", db_path_value);
		db_path = db_path_value.ToString();
	}
	string ivm_file_path = db_path + "/ivm_upsert_queries_" + view_name + ".sql";
	duckdb::OpenIVMUtils::WriteFile(ivm_file_path, false, clean_query);

	OPENIVM_DEBUG_PRINT("[UPSERT] Generated query:\n%s\n", clean_query.c_str());

	return clean_query;
}

} // namespace duckdb
