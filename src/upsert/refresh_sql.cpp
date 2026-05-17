#include "upsert/refresh_internal.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_compiler.hpp"
#include "upsert/refresh_cost_model.hpp"
#include "lpts_pipeline.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/planner.hpp"

namespace duckdb {

namespace {

constexpr const char *DUCKLAKE_SYNTHETIC_DELTA_TS = "1970-01-02 00:00:00";
constexpr const char *DUCKLAKE_SYNTHETIC_LAST_UPDATE = "1970-01-01 00:00:00";

struct SemiAntiSourceInput {
	string table_sql;
	string delta_sql;
	string last_update;
};

static string ResolveSemiAntiMetadataKey(const string &table_name, const vector<string> &delta_table_names) {
	vector<string> candidates;
	candidates.push_back(table_name);
	candidates.push_back(SqlUtils::LastIdentifierPart(table_name));
	candidates.push_back(SqlUtils::DeltaName(table_name));
	candidates.push_back(SqlUtils::DeltaName(SqlUtils::LastIdentifierPart(table_name)));
	for (auto &dt : delta_table_names) {
		for (auto &candidate : candidates) {
			if (StringUtil::CIEquals(dt, candidate)) {
				return dt;
			}
		}
	}
	return SqlUtils::DeltaName(SqlUtils::LastIdentifierPart(table_name));
}

static string BuildDuckLakeSignedDeltaRelation(const DuckLakeSourceLocation &loc, int64_t last_snapshot_id,
                                               int64_t current_snapshot_id) {
	string insertions = "SELECT *, 1::INTEGER AS " + string(openivm::MULTIPLICITY_COL) + ", TIMESTAMP '" +
	                    DUCKLAKE_SYNTHETIC_DELTA_TS + "' AS " + string(openivm::TIMESTAMP_COL) + " FROM " +
	                    SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", loc.catalog_name, loc.schema_name,
	                                                    loc.table_name, last_snapshot_id, current_snapshot_id);
	string deletions = "SELECT *, -1::INTEGER AS " + string(openivm::MULTIPLICITY_COL) + ", TIMESTAMP '" +
	                   DUCKLAKE_SYNTHETIC_DELTA_TS + "' AS " + string(openivm::TIMESTAMP_COL) + " FROM " +
	                   SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", loc.catalog_name, loc.schema_name,
	                                                   loc.table_name, last_snapshot_id, current_snapshot_id);
	return "(" + insertions + "\nUNION ALL\n" + deletions + ")";
}

static SemiAntiSourceInput ResolveSemiAntiSourceInput(RefreshMetadata &metadata, Connection &con,
                                                      const string &view_name, const string &table_name,
                                                      const vector<string> &delta_table_names,
                                                      const string &view_catalog_name, const string &view_schema_name,
                                                      const string &attached_db_catalog_name,
                                                      const string &attached_db_schema_name) {
	SemiAntiSourceInput input;
	string metadata_key = ResolveSemiAntiMetadataKey(table_name, delta_table_names);
	if (metadata.IsDuckLakeTable(view_name, metadata_key)) {
		auto loc = ResolveDuckLakeSourceLocation(con, view_name, metadata_key, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name);
		int64_t last_snapshot_id = metadata.GetLastSnapshotId(view_name, metadata_key);
		int64_t current_snapshot_id = metadata.GetCurrentDuckLakeSnapshot(loc.catalog_name);
		if (last_snapshot_id < 0 || current_snapshot_id < 0) {
			throw Exception(ExceptionType::CATALOG,
			                "IVM: missing DuckLake snapshot metadata for semi/anti refresh of view '" + view_name +
			                    "', table '" + metadata_key + "'");
		}
		input.table_sql = SqlUtils::FullName(loc.catalog_name, loc.schema_name, loc.table_name);
		input.delta_sql = BuildDuckLakeSignedDeltaRelation(loc, last_snapshot_id, current_snapshot_id);
		input.last_update = DUCKLAKE_SYNTHETIC_LAST_UPDATE;
		return input;
	}

	input.table_sql = table_name;
	input.delta_sql = metadata_key;
	input.last_update = metadata.GetLastUpdate(view_name, metadata_key);
	return input;
}

} // namespace

string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                          const string &view_name, bool cross_system, const string &attached_db_catalog_name,
                          const string &attached_db_schema_name, string *out_pre_meta, string *out_post_meta,
                          RefreshCompileProfile *compile_profile) {
	auto profile_now = []() {
		return std::chrono::steady_clock::now();
	};
	auto add_profile_step = [&](const string &step_name, std::chrono::steady_clock::time_point start,
	                            const string &detail = string()) {
		if (compile_profile) {
			compile_profile->AddStep(step_name, start, detail);
		}
	};
	auto context_start = profile_now();
	QueryErrorContext error_context = QueryErrorContext();
	Connection con(*context.db.get());
	Value skip_empty_val;
	bool skip_empty_enabled = true;
	if (context.TryGetCurrentSetting("openivm_skip_empty_deltas", skip_empty_val) && !skip_empty_val.IsNull()) {
		skip_empty_enabled = skip_empty_val.GetValue<bool>();
	}
	string default_db;
	string default_schema = "main";
	{
		auto db_res = con.Query("SELECT current_database()");
		if (!db_res->HasError() && db_res->RowCount() > 0 && !db_res->GetValue(0, 0).IsNull()) {
			default_db = db_res->GetValue(0, 0).ToString();
		}
		auto schema_res = con.Query("SELECT current_schema()");
		if (!schema_res->HasError() && schema_res->RowCount() > 0 && !schema_res->GetValue(0, 0).IsNull()) {
			default_schema = schema_res->GetValue(0, 0).ToString();
		}
	}
	add_profile_step("generate_refresh_sql.context", context_start,
	                 "cross_system=" + string(cross_system ? "true" : "false"));
	string catalog_prefix;
	if (!view_catalog_name.empty() && view_catalog_name != "memory") {
		catalog_prefix =
		    SqlUtils::QuoteIdentifier(view_catalog_name) + "." + SqlUtils::QuoteIdentifier(view_schema_name) + ".";
	}
	string internal_catalog_name = view_catalog_name;
	string internal_schema_name = view_schema_name;
	string internal_catalog_prefix = catalog_prefix;
	auto metadata_start = profile_now();
	RefreshMetadata metadata(con);
	bool target_is_ducklake = metadata.IsDuckLakeCatalog(view_catalog_name);
	if (!target_is_ducklake && cross_system && !default_db.empty() && default_db != "memory" &&
	    view_catalog_name != default_db) {
		internal_catalog_name = default_db;
		internal_schema_name = default_schema;
		internal_catalog_prefix =
		    SqlUtils::QuoteIdentifier(default_db) + "." + SqlUtils::QuoteIdentifier(default_schema) + ".";
	}
	string data_table_bare = IncrementalTableNames::DataTableName(view_name);
	string data_table = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table_bare);
	OPENIVM_DEBUG_PRINT("[UPSERT] Looking up delta view '%s' in catalog '%s.%s'\n",
	                    SqlUtils::DeltaName(view_name).c_str(), view_catalog_name.c_str(), view_schema_name.c_str());
	optional_ptr<TableCatalogEntry> delta_view_catalog_entry;
	optional_ptr<CatalogEntry> index_delta_view_catalog_entry;
	if (internal_catalog_prefix.empty() || internal_catalog_name == default_db) {
		con.BeginTransaction();
		delta_view_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    *con.context, internal_catalog_name, internal_schema_name, SqlUtils::DeltaName(view_name),
		    OnEntryNotFound::THROW_EXCEPTION, error_context);
		index_delta_view_catalog_entry = Catalog::GetEntry(
		    *con.context, internal_catalog_name, internal_schema_name,
		    EntryLookupInfo(CatalogType::INDEX_ENTRY, data_table_bare + openivm::INDEX_SUFFIX, error_context),
		    OnEntryNotFound::RETURN_NULL);
		con.Rollback();
	}
	auto view_query_sql = metadata.GetViewQuery(view_name);
	if (view_query_sql.empty()) {
		throw ParserException("View not found! Please call IVM with a materialized view.");
	}
	RefreshType view_query_type = metadata.GetViewType(view_name);
	OPENIVM_DEBUG_PRINT("[UPSERT] View: %s, Type: %d, Query: %s\n", view_name.c_str(), (int)view_query_type,
	                    view_query_sql.c_str());
	auto delta_table_names = metadata.GetDeltaTables(view_name);
	add_profile_step("generate_refresh_sql.metadata_lookup", metadata_start,
	                 "refresh_type=" + string(RefreshTypeName(view_query_type)) +
	                     "; delta_tables=" + to_string(delta_table_names.size()) +
	                     "; target_ducklake=" + string(target_is_ducklake ? "true" : "false"));
	auto qualify_start = profile_now();
	view_query_sql =
	    QualifyViewQuerySources(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
	                            view_schema_name, attached_db_catalog_name, attached_db_schema_name);
	add_profile_step("generate_refresh_sql.qualify_sources", qualify_start,
	                 "query_bytes=" + to_string(view_query_sql.size()));
	auto recovery_start = profile_now();
	{
		auto flag_result = con.Query("SELECT refresh_in_progress FROM " + string(openivm::VIEWS_TABLE) +
		                             " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
		if (!flag_result->HasError() && flag_result->RowCount() > 0 && !flag_result->GetValue(0, 0).IsNull() &&
		    flag_result->GetValue(0, 0).GetValue<bool>()) {
			Printer::Print("Warning: recovering '" + view_name + "' from interrupted refresh via full recompute.");
			metadata.SetRefreshInProgress(view_name, false);
			return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name, internal_catalog_prefix, out_post_meta);
		}
	}
	add_profile_step("generate_refresh_sql.recovery_check", recovery_start);
	bool source_has_left_join = metadata.HasLeftJoin(view_name);
	bool source_has_full_outer = source_has_left_join && metadata.HasFullOuter(view_name);
	bool left_join_merge = false;
	Value lj_merge_val;
	if (source_has_left_join && !source_has_full_outer) {
		if (context.TryGetCurrentSetting("openivm_left_join_merge", lj_merge_val) && !lj_merge_val.IsNull()) {
			left_join_merge = lj_merge_val.GetValue<bool>();
		}
	}
	bool full_outer_merge = false;
	if (source_has_full_outer) {
		Value foj_merge_val;
		if (context.TryGetCurrentSetting("openivm_full_outer_merge", foj_merge_val) && !foj_merge_val.IsNull()) {
			full_outer_merge = foj_merge_val.GetValue<bool>();
		}
	}
	bool has_minmax = metadata.HasMinMax(view_name) || view_query_type == RefreshType::AGGREGATE_HAVING ||
	                  (source_has_left_join && !source_has_full_outer && !left_join_merge) ||
	                  (source_has_full_outer && !full_outer_merge);
	Value refresh_mode_val;
	bool force_full_refresh = false;
	if (context.TryGetCurrentSetting("openivm_refresh_mode", refresh_mode_val) && !refresh_mode_val.IsNull()) {
		auto mode = StringUtil::Lower(refresh_mode_val.ToString());
		if (mode == "full") {
			force_full_refresh = true;
		}
	}

	if (force_full_refresh || view_query_type == RefreshType::FULL_REFRESH) {
		auto full_refresh_start = profile_now();
		auto recompute_query =
		    BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
		                        attached_db_schema_name, internal_catalog_prefix, out_post_meta);
		add_profile_step("generate_refresh_sql.dispatch", full_refresh_start,
		                 "full_recompute=true; sql_bytes=" + to_string(recompute_query.size()));
		return recompute_query;
	}
	Value adaptive_refresh_val;
	bool adaptive_refresh = false;
	if (context.TryGetCurrentSetting("openivm_adaptive_refresh", adaptive_refresh_val) &&
	    !adaptive_refresh_val.IsNull()) {
		adaptive_refresh = adaptive_refresh_val.GetValue<bool>();
	}
	if (adaptive_refresh) {
		auto adaptive_start = profile_now();
		con.BeginTransaction();
		Parser cost_parser;
		cost_parser.ParseQuery(view_query_sql);
		Planner cost_planner(*con.context);
		cost_planner.CreatePlan(cost_parser.statements[0]->Copy());
		Optimizer cost_optimizer(*cost_planner.binder, *con.context);
		auto cost_plan = cost_optimizer.Optimize(std::move(cost_planner.plan));

		auto cost_estimate = EstimateRefreshCost(*con.context, *cost_plan, view_name);
		con.Rollback();
		if (cost_estimate.ShouldRecompute()) {
			OPENIVM_DEBUG_PRINT("[ADAPTIVE] Full recompute is cheaper — skipping IVM\n");
			add_profile_step("generate_refresh_sql.adaptive_cost", adaptive_start, "strategy=full");
			return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name, internal_catalog_prefix, out_post_meta);
		}
		add_profile_step("generate_refresh_sql.adaptive_cost", adaptive_start, "strategy=incremental");
	}
	auto column_metadata_start = profile_now();
	vector<string> column_names;
	vector<LogicalType> column_types;
	bool list_mode = false;
	if (delta_view_catalog_entry) {
		auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
		const ColumnList &delta_view_columns = delta_view_entry->GetColumns();
		column_names = delta_view_columns.GetColumnNames();
		for (auto &col : delta_view_columns.Logical()) {
			column_types.push_back(col.GetType());
			if (col.GetName() != openivm::MULTIPLICITY_COL && col.GetType().id() == LogicalTypeId::LIST) {
				list_mode = true;
			}
		}
	} else {
		auto col_result =
		    con.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = '" +
		              SqlUtils::EscapeValue(internal_catalog_name) + "' AND table_schema = '" +
		              SqlUtils::EscapeValue(internal_schema_name) + "' AND table_name = '" +
		              SqlUtils::EscapeValue(SqlUtils::DeltaName(view_name)) + "' ORDER BY ordinal_position");
		if (!col_result->HasError()) {
			for (idx_t i = 0; i < col_result->RowCount(); i++) {
				column_names.push_back(col_result->GetValue(0, i).ToString());
				try {
					column_types.push_back(
					    TransformStringToLogicalType(col_result->GetValue(1, i).ToString(), context));
				} catch (...) {
					column_types.push_back(LogicalType::VARCHAR);
				}
			}
		}
	}
	bool has_ts_col =
	    std::find(column_names.begin(), column_names.end(), string(openivm::TIMESTAMP_COL)) != column_names.end();
	auto it = std::find(column_names.begin(), column_names.end(), string(openivm::TIMESTAMP_COL));
	if (it != column_names.end()) {
		auto offset = it - column_names.begin();
		column_names.erase(it);
		if (offset < static_cast<decltype(offset)>(column_types.size())) {
			column_types.erase(column_types.begin() + offset);
		}
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] List mode: %s\n", list_mode ? "true" : "false");
	add_profile_step("generate_refresh_sql.column_metadata", column_metadata_start,
	                 "columns=" + to_string(column_names.size()) +
	                     "; list_mode=" + string(list_mode ? "true" : "false"));

	string upsert_query;
	bool skip_compute_delta_for_refresh = false;
	string delta_ts_filter = BuildDeltaTimestampFilter(con, view_name, has_ts_col);
	bool has_left_join =
	    std::find(column_names.begin(), column_names.end(), openivm::LEFT_KEY_COL) != column_names.end();
	bool has_full_outer =
	    std::find(column_names.begin(), column_names.end(), openivm::RIGHT_KEY_COL) != column_names.end();
	OPENIVM_DEBUG_PRINT("[UPSERT] has_left_join=%d has_full_outer=%d\n", has_left_join, has_full_outer);

	auto fast_path_start = profile_now();
	auto fast_paths = ResolveDeltaFastPathFlags(context, metadata, con, view_name, view_query_sql, delta_table_names,
	                                            view_catalog_name, view_schema_name, attached_db_catalog_name,
	                                            attached_db_schema_name, cross_system);
	add_profile_step("generate_refresh_sql.delta_fast_paths", fast_path_start,
	                 "insert_only=" + string(fast_paths.insert_only ? "true" : "false") +
	                     "; skip_agg_delete=" + string(fast_paths.skip_agg_delete ? "true" : "false") +
	                     "; skip_proj_delete=" + string(fast_paths.skip_proj_delete ? "true" : "false") +
	                     "; minmax_incremental=" + string(fast_paths.minmax_incremental ? "true" : "false"));
	bool insert_only = fast_paths.insert_only;
	bool skip_agg_delete = fast_paths.skip_agg_delete;
	bool skip_proj_delete = fast_paths.skip_proj_delete;
	bool minmax_incremental = fast_paths.minmax_incremental;
	auto group_cols = metadata.GetGroupColumns(view_name);
	auto agg_types = metadata.GetAggregateTypes(view_name);
	bool has_argminmax = std::any_of(agg_types.begin(), agg_types.end(),
	                                 [](const string &t) { return t == "arg_min" || t == "arg_max"; });
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: %s\n", RefreshTypeName(view_query_type));
	auto dispatch_start = profile_now();
	switch (view_query_type) {
	case RefreshType::AGGREGATE_HAVING: {
		Value having_merge_val;
		bool having_merge = true;
		if (context.TryGetCurrentSetting("openivm_having_merge", having_merge_val) && !having_merge_val.IsNull()) {
			having_merge = having_merge_val.GetValue<bool>();
		}
		if (having_merge) {
			bool effective_insert_only = has_argminmax ? false : (has_minmax ? minmax_incremental : skip_agg_delete);
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, has_minmax, list_mode,
			    delta_ts_filter, group_cols, internal_catalog_prefix, effective_insert_only, agg_types, column_types);
		} else {
			upsert_query =
			    CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql,
			                           /*has_minmax=*/true, list_mode, delta_ts_filter, group_cols,
			                           internal_catalog_prefix, /*insert_only=*/false, agg_types, column_types);
		}
		break;
	}
	case RefreshType::AGGREGATE_GROUP: {
		if (source_has_full_outer && !full_outer_merge) {
			upsert_query = BuildFullOuterAffectedGroupRefresh(metadata, view_name, delta_table_names, group_cols,
			                                                  data_table, view_query_sql, delta_ts_filter,
			                                                  internal_catalog_prefix, "openivm_recompute");
		} else if (source_has_full_outer && full_outer_merge) {
			bool effective_insert_only = has_argminmax ? false : skip_agg_delete;
			upsert_query =
			    CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql,
			                           /*has_minmax=*/has_argminmax, list_mode, delta_ts_filter, group_cols,
			                           internal_catalog_prefix, effective_insert_only, agg_types, column_types);
			upsert_query += BuildFullOuterAffectedGroupRefresh(metadata, view_name, delta_table_names, group_cols,
			                                                   data_table, view_query_sql, delta_ts_filter,
			                                                   internal_catalog_prefix, "openivm_unmatched");
		} else {
			bool effective_insert_only = has_argminmax ? false : (has_minmax ? minmax_incremental : skip_agg_delete);
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, has_minmax, list_mode,
			    delta_ts_filter, group_cols, internal_catalog_prefix, effective_insert_only, agg_types, column_types);
		}
		break;
	}
	case RefreshType::SIMPLE_PROJECTION: {
		if (!has_full_outer && !has_left_join &&
		    TryBuildDuckLakeProjectionKeyRefresh(metadata, con, view_name, delta_table_names, data_table,
		                                         view_query_sql, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name, upsert_query)) {
			skip_compute_delta_for_refresh = true;
		} else {
			upsert_query = CompileProjectionRefresh(metadata, view_name, column_names, delta_table_names, data_table,
			                                        view_query_sql, delta_ts_filter, internal_catalog_prefix,
			                                        has_full_outer, has_left_join, skip_proj_delete);
		}
		break;
	}

	case RefreshType::SIMPLE_AGGREGATE: {
		bool sa_insert_only = has_argminmax ? false : insert_only;
		RefreshMetadata::FilteredGroupCountAuxMeta aux_meta;
		if (metadata.GetFilteredGroupCountAuxMeta(view_name, aux_meta)) {
			string delta_source = SqlUtils::DeltaName(aux_meta.source);
			for (auto &dt : delta_table_names) {
				if (StringUtil::CIEquals(dt, delta_source)) {
					delta_source = dt;
					break;
				}
			}
			string ts = metadata.GetLastUpdate(view_name, delta_source);
			upsert_query = CompileFilteredGroupCount(
			    view_name, aux_meta.aux_table, delta_source, ts, aux_meta.group_col, aux_meta.sum_col,
			    aux_meta.output_col, aux_meta.comparison_op, aux_meta.threshold_sql, internal_catalog_prefix);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling SIMPLE_AGGREGATE filtered-group-count aux (%s, sum=%s %s %s)\n",
			                    aux_meta.group_col.c_str(), aux_meta.sum_col.c_str(), aux_meta.comparison_op.c_str(),
			                    aux_meta.threshold_sql.c_str());
		} else {
			upsert_query =
			    CompileSimpleAggregates(view_name, column_names, view_query_sql, has_minmax, list_mode, delta_ts_filter,
			                            internal_catalog_prefix, sa_insert_only, column_types);
		}
		if (!has_minmax && aux_meta.aux_table.empty()) {
			AppendSimpleAggregateEmptySourceNulling(metadata, upsert_query, view_name, column_names, data_table,
			                                        view_catalog_name, view_schema_name, attached_db_catalog_name,
			                                        attached_db_schema_name);
		}
		break;
	}
	case RefreshType::WINDOW_PARTITION: {
		upsert_query = BuildWindowPartitionRefresh(metadata, con, view_name, view_query_sql, delta_table_names,
		                                           column_names, data_table, delta_ts_filter, internal_catalog_prefix,
		                                           view_catalog_name, view_schema_name, attached_db_catalog_name,
		                                           attached_db_schema_name, cross_system);
		break;
	}
	case RefreshType::DISTINCT_INCREMENTAL: {
		RefreshMetadata::DistinctAuxMeta aux_meta;
		if (metadata.GetDistinctAuxMeta(view_name, aux_meta)) {
			auto group_columns = metadata.GetGroupColumns(view_name);
			string delta_source = SqlUtils::DeltaName(aux_meta.source);
			string ts = metadata.GetLastUpdate(view_name, delta_source);
			upsert_query = CompileDistinctIncremental(
			    view_name, aux_meta.aux_table, aux_meta.cols, delta_source, ts, aux_meta.filter, group_columns,
			    aux_meta.sum_arg, aux_meta.sum_out, string(openivm::COUNT_STAR_COL), internal_catalog_prefix);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: DISTINCT_INCREMENTAL (%zu distinct cols, "
			                    "%zu group cols, sum_arg=%s, sum_out=%s)\n",
			                    aux_meta.cols.size(), group_columns.size(), aux_meta.sum_arg.c_str(),
			                    aux_meta.sum_out.c_str());
			break;
		}
		OPENIVM_DEBUG_PRINT("[UPSERT] DISTINCT_INCREMENTAL view has no aux meta — falling through to "
		                    "GROUP_RECOMPUTE\n");
		[[fallthrough]];
	}
	case RefreshType::SEMI_ANTI_RECOMPUTE: {
		RefreshMetadata::SemiAntiAuxMeta aux_meta;
		if (metadata.GetSemiAntiAuxMeta(view_name, aux_meta)) {
			auto left_input = ResolveSemiAntiSourceInput(metadata, con, view_name, aux_meta.left_table,
			                                             delta_table_names, view_catalog_name, view_schema_name,
			                                             attached_db_catalog_name, attached_db_schema_name);
			auto right_input = ResolveSemiAntiSourceInput(metadata, con, view_name, aux_meta.right_table,
			                                              delta_table_names, view_catalog_name, view_schema_name,
			                                              attached_db_catalog_name, attached_db_schema_name);
			string left_delta = left_input.delta_sql;
			string left_ts = left_input.last_update;
			if (left_ts.empty()) {
				left_delta.clear();
			}
			upsert_query = CompileSemiAntiRecompute(
			    view_name, aux_meta.aux_table, aux_meta.join_type, left_input.table_sql, aux_meta.left_alias,
			    right_input.table_sql, aux_meta.right_alias, aux_meta.predicate, aux_meta.post_filter,
			    aux_meta.left_cols, aux_meta.left_exprs, aux_meta.output_cols, left_delta, right_input.delta_sql,
			    left_ts, right_input.last_update, internal_catalog_prefix);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: SEMI_ANTI_RECOMPUTE (%s, %zu left cols)\n",
			                    aux_meta.join_type.c_str(), aux_meta.left_cols.size());
			break;
		}
		OPENIVM_DEBUG_PRINT("[UPSERT] SEMI_ANTI_RECOMPUTE view has no aux meta — falling through to "
		                    "GROUP_RECOMPUTE\n");
		[[fallthrough]];
	}
	case RefreshType::GROUP_RECOMPUTE: {
		auto group_columns = metadata.GetGroupColumns(view_name);
		bool group_recompute_has_ducklake_source = false;
		for (auto &dt : delta_table_names) {
			if (metadata.IsDuckLakeTable(view_name, dt)) {
				group_recompute_has_ducklake_source = true;
				break;
			}
		}
		string recompute_ducklake_catalog;
		if (group_recompute_has_ducklake_source) {
			recompute_ducklake_catalog = ResolveDuckLakeCatalogName(con, view_catalog_name, attached_db_catalog_name);
		}
		string recompute_ducklake_schema = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
		auto delta_specs = BuildGroupRecomputeDeltaSpecs(metadata, view_name, con, delta_table_names,
		                                                 recompute_ducklake_catalog, recompute_ducklake_schema);
		string lpts_cat = view_catalog_name.empty() ? "memory" : view_catalog_name;
		string lpts_sch = view_schema_name.empty() ? "main" : view_schema_name;
		string lpts_table_prefix = SqlUtils::QualifiedPrefix(lpts_cat, lpts_sch);
		upsert_query = CompileGroupRecompute(view_name, view_query_sql, group_columns, delta_specs,
		                                     internal_catalog_prefix, lpts_table_prefix);
		OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: GROUP_RECOMPUTE (%zu group cols, %zu sources)\n",
		                    group_columns.size(), delta_specs.size());
		break;
	}
	case RefreshType::TOP_K:
		[[fallthrough]];
	case RefreshType::FULL_REFRESH: {
		throw InternalException("FULL_REFRESH views should not reach incremental upsert compilation");
	}
	}
	add_profile_step("generate_refresh_sql.dispatch", dispatch_start,
	                 "refresh_type=" + string(RefreshTypeName(view_query_type)) +
	                     "; upsert_bytes=" + to_string(upsert_query.size()));
	OPENIVM_DEBUG_PRINT("[UPSERT] Upsert query:\n%s\n", upsert_query.c_str());
	string delta_query;
	string companion_query;
	string pre_companion;
	string post_companion;
	string compact_delta_view_query;
	string delete_from_view_query;
	string delta_view_name_bare = SqlUtils::DeltaName(view_name);
	string delta_view_name = internal_catalog_prefix + delta_view_name_bare;
	auto downstream_check = con.Query("SELECT COUNT(*) FROM " + string(openivm::DELTA_TABLES_TABLE) +
	                                  " WHERE table_name = '" + SqlUtils::EscapeValue(delta_view_name_bare) + "'");
	bool has_downstream = !downstream_check->HasError() && downstream_check->RowCount() > 0 &&
	                      downstream_check->GetValue(0, 0).GetValue<int64_t>() > 0;
	auto build_snapshot_companion = [&]() {
		string col_list;
		for (auto &col : column_names) {
			if (!col_list.empty()) {
				col_list += ", ";
			}
			col_list += SqlUtils::QuoteIdentifier(col);
		}

		string select_old, select_new;
		bool first = true;
		for (auto &col : column_names) {
			if (!first) {
				select_old += ", ";
				select_new += ", ";
			}
			first = false;
			if (col == string(openivm::MULTIPLICITY_COL)) {
				select_old += "-1";
				select_new += "1";
			} else {
				select_old += SqlUtils::QuoteIdentifier(col);
				select_new += SqlUtils::QuoteIdentifier(col);
			}
		}

		string temp_name = string(openivm::TEMP_TABLE_PREFIX) + view_name;
		string qt = KeywordHelper::WriteOptionallyQuoted(temp_name);
		string qdvn = delta_view_name.find('.') == string::npos ? KeywordHelper::WriteOptionallyQuoted(delta_view_name)
		                                                        : delta_view_name;
		const string &qdt = data_table;
		pre_companion = "CREATE TEMP TABLE " + qt + " AS SELECT * FROM " + qdt + ";\n";
		post_companion = "DELETE FROM " + qdvn + " WHERE 1=1";
		if (!delta_ts_filter.empty()) {
			post_companion += " AND " + delta_ts_filter;
		}
		post_companion += ";\n";
		post_companion += "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_old + " FROM " + qt + ";\n";
		post_companion += "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_new + " FROM " + qdt + ";\n";
		post_companion += "DROP TABLE " + qt + ";\n";
		OPENIVM_DEBUG_PRINT("[UPSERT] Pre-companion: %s\n", pre_companion.c_str());
		OPENIVM_DEBUG_PRINT("[UPSERT] Post-companion: %s\n", post_companion.c_str());
	};

	if (skip_compute_delta_for_refresh || view_query_type == RefreshType::WINDOW_PARTITION ||
	    view_query_type == RefreshType::GROUP_RECOMPUTE || view_query_type == RefreshType::DISTINCT_INCREMENTAL ||
	    view_query_type == RefreshType::SEMI_ANTI_RECOMPUTE) {
		OPENIVM_DEBUG_PRINT("[UPSERT] Skipping ComputeDelta for %s\n",
		                    skip_compute_delta_for_refresh                         ? "SIMPLE_PROJECTION_KEY"
		                    : view_query_type == RefreshType::DISTINCT_INCREMENTAL ? "DISTINCT_INCREMENTAL"
		                    : view_query_type == RefreshType::SEMI_ANTI_RECOMPUTE  ? "SEMI_ANTI_RECOMPUTE"
		                    : view_query_type == RefreshType::GROUP_RECOMPUTE      ? "GROUP_RECOMPUTE"
		                                                                           : "WINDOW_PARTITION");
		delta_query = "";
		if (has_downstream) {
			build_snapshot_companion();
		}
	} else {
		string compute_delta = "select * from ComputeDelta('" + SqlUtils::EscapeValue(internal_catalog_name) + "','" +
		                       SqlUtils::EscapeValue(internal_schema_name) + "','" + SqlUtils::EscapeValue(view_name) +
		                       "');";
		OPENIVM_DEBUG_PRINT("[UPSERT] Planning ComputeDelta query: %s\n", compute_delta.c_str());
		auto compute_delta_plan_start = profile_now();
		Parser p;
		p.ParseQuery(compute_delta);

		con.BeginTransaction();
		auto &con_ctx = *con.context;
		OPENIVM_DEBUG_PRINT("[UPSERT] Creating planner...\n");
		Planner planner(con_ctx);
		OPENIVM_DEBUG_PRINT("[UPSERT] CreatePlan...\n");
		planner.CreatePlan(std::move(p.statements[0]));
		auto plan = std::move(planner.plan);
		OPENIVM_DEBUG_PRINT("[UPSERT] Plan created. Running optimizer...\n");
		Optimizer optimizer(*planner.binder, con_ctx);
		plan = optimizer.Optimize(std::move(plan)); // this transforms the plan into an incremental plan
		OPENIVM_DEBUG_PRINT("[UPSERT] Optimizer done.\n");
		con.Rollback();
		add_profile_step("generate_refresh_sql.compute_delta_plan", compute_delta_plan_start);
		string raw_refresh_sql;
		if (IsEmptyDeltaPlan(plan.get())) {
			raw_refresh_sql = BuildEmptyDeltaInsert(view_name, column_names, column_types);
			OPENIVM_DEBUG_PRINT("[UPSERT] Delta plan is empty; generated no-op delta insert for '%s'\n",
			                    view_name.c_str());
		} else {
			try {
				auto lpts_start = profile_now();
				auto ast = LogicalPlanToAst(con_ctx, plan);
				auto cte_list = AstToCteList(*ast);
				raw_refresh_sql = cte_list->ToQuery(false);
				add_profile_step("generate_refresh_sql.lpts", lpts_start,
				                 "delta_sql_bytes=" + to_string(raw_refresh_sql.size()));
				OPENIVM_DEBUG_PRINT("[UPSERT] ToQuery done. SQL:\n%s\n", raw_refresh_sql.c_str());
			} catch (const std::exception &e) {
				OPENIVM_DEBUG_PRINT("[UPSERT] LPTS serialization failed (%s) for view '%s'\n", e.what(),
				                    view_name.c_str());
				throw Exception(ExceptionType::EXECUTOR, "IVM: failed to serialize incremental delta plan for view '" +
				                                             view_name + "': " + e.what());
			}
		}
		string insert_target_bare = "INSERT INTO " + SqlUtils::DeltaName(view_name);
		auto insert_pos = raw_refresh_sql.find(insert_target_bare);
		if (insert_pos != string::npos) {
			if (!internal_catalog_prefix.empty()) {
				raw_refresh_sql.replace(insert_pos, insert_target_bare.size(), "INSERT INTO " + delta_view_name);
				insert_pos = raw_refresh_sql.find("INSERT INTO " + delta_view_name);
			}
			string col_list = "(" + SqlUtils::JoinQuotedColumns(column_names) + ") ";
			string full_insert = "INSERT INTO " + delta_view_name;
			raw_refresh_sql.insert(insert_pos + full_insert.size(), " " + col_list);
		}
		delta_query += raw_refresh_sql;

		if (has_downstream && view_query_type == RefreshType::SIMPLE_AGGREGATE) {
			build_snapshot_companion();
		} else if ((view_query_type == RefreshType::AGGREGATE_GROUP ||
		            view_query_type == RefreshType::AGGREGATE_HAVING) &&
		           has_downstream && index_delta_view_catalog_entry) {
			auto *idx = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
			auto key_ids = idx->column_ids;
			vector<string> keys;
			unordered_set<string> keys_set;
			for (auto &kid : key_ids) {
				keys.push_back(column_names[kid]);
				keys_set.insert(column_names[kid]);
			}

			string col_list, val_list;
			for (auto &col : column_names) {
				if (!col_list.empty()) {
					col_list += ", ";
					val_list += ", ";
				}
				col_list += col;
				if (keys_set.count(col)) {
					val_list += "d." + col;
				} else if (col == openivm::MULTIPLICITY_COL) {
					val_list += "-1";
				} else {
					val_list += "0";
				}
			}
			string join_cond = SqlUtils::BuildNullSafeMatch(keys, "d", "m");

			companion_query = "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + val_list + " FROM " +
			                  delta_view_name + " d WHERE d." + string(openivm::MULTIPLICITY_COL) + " > 0";
			if (!delta_ts_filter.empty()) {
				companion_query += " AND d." + delta_ts_filter;
			}
			companion_query += " AND EXISTS (SELECT 1 FROM " + data_table + " m WHERE " + join_cond + ");\n";
			OPENIVM_DEBUG_PRINT("[UPSERT] Companion query:\n%s\n", companion_query.c_str());
		}
	}

	auto assembly_start = profile_now();
	if (has_downstream) {
		if (skip_empty_enabled) {
			compact_delta_view_query =
			    BuildCompactDeltaViewSQL(view_name, delta_view_name, column_names, delta_ts_filter);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compact delta-view query:\n%s\n", compact_delta_view_query.c_str());
		}
		delete_from_view_query = RefreshMetadata::BuildDeltaCleanupSQL(delta_view_name, delta_view_name_bare);
	} else {
		delete_from_view_query = "DELETE FROM " + delta_view_name + ";";
	}
	string delete_from_delta_table_query;
	string update_timestamp_query;
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			update_timestamp_query += "UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
			                          " SET last_update = now(), last_refresh_ts = now() WHERE view_name = '" +
			                          SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
			                          SqlUtils::EscapeValue(dt) + "';\n";
			continue;
		}
		string resolved = dt;
		if (cross_system) {
			resolved = metadata.ResolveDeltaQualifiedName(view_name, dt);
		}
		update_timestamp_query += "UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
		                          " SET last_update = COALESCE("
		                          "(SELECT MAX(" +
		                          string(openivm::TIMESTAMP_COL) + ") + INTERVAL '1 microsecond' FROM " + resolved +
		                          "), now()), last_refresh_ts = now()"
		                          " WHERE view_name = '" +
		                          SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
		                          SqlUtils::EscapeValue(dt) + "';\n";
	}
	string snapshot_update_query;
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			auto loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
			                                         attached_db_catalog_name, attached_db_schema_name);
			if (loc.catalog_name.empty()) {
				throw Exception(ExceptionType::CATALOG,
				                "Could not resolve DuckLake catalog for source table '" + dt + "'");
			}
			string dl_snapshot_expr = cross_system ? DuckLakeSnapshotPlaceholder(loc.catalog_name)
			                                       : "(SELECT id FROM " + SqlUtils::QuoteIdentifier(loc.catalog_name) +
			                                             ".current_snapshot())";
			snapshot_update_query += "UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
			                         " SET last_snapshot_id = " + dl_snapshot_expr + " WHERE view_name = '" +
			                         SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
			                         SqlUtils::EscapeValue(dt) + "';\n";
		}
	}

	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		if (cross_system) {
			continue;
		}
		delete_from_delta_table_query += RefreshMetadata::BuildDeltaCleanupSQL(dt, dt);
	}
	string set_in_progress = "UPDATE " + string(openivm::VIEWS_TABLE) +
	                         " SET refresh_in_progress = true WHERE view_name = '" + SqlUtils::EscapeValue(view_name) +
	                         "';\n";
	string clear_in_progress = "UPDATE " + string(openivm::VIEWS_TABLE) +
	                           " SET refresh_in_progress = false WHERE view_name = '" +
	                           SqlUtils::EscapeValue(view_name) + "';\n";
	string data_sql = pre_companion + delta_query + "\n" + companion_query + "\n" + upsert_query + "\n" +
	                  post_companion + compact_delta_view_query + delete_from_view_query + "\n" +
	                  delete_from_delta_table_query;
	const string &meta_pre_sql = set_in_progress;
	string meta_post_sql = update_timestamp_query + snapshot_update_query + "\n" + clear_in_progress;

	string clean_query;
	if (cross_system && out_pre_meta != nullptr && out_post_meta != nullptr) {
		*out_pre_meta = meta_pre_sql;
		*out_post_meta = meta_post_sql;
		clean_query = data_sql;
	} else {
		clean_query = meta_pre_sql + data_sql + meta_post_sql;
	}
	Value files_path_val;
	if (context.TryGetCurrentSetting("openivm_files_path", files_path_val) && !files_path_val.IsNull()) {
		string refresh_file_path = files_path_val.ToString() + "/openivm_upsert_queries_" + view_name + ".sql";
		duckdb::SqlUtils::WriteFile(refresh_file_path, false, clean_query);
	}

	OPENIVM_DEBUG_PRINT("[UPSERT] Generated query:\n%s\n", clean_query.c_str());
	add_profile_step("generate_refresh_sql.assembly", assembly_start,
	                 "sql_bytes=" + to_string(clean_query.size()) +
	                     "; meta_post_bytes=" + to_string(meta_post_sql.size()));

	return clean_query;
}

} // namespace duckdb
