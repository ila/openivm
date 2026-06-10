#include "core/parser.hpp"

#include "core/plan_rewrite.hpp"
#include "core/openivm_constants.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/parser_sql_extractors.hpp"
#include "core/refresh_metadata.hpp"
#include "core/ivm_delta_model.hpp"
#include "core/ivm_view_classifier.hpp"
#include "lpts_pipeline.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_compiler.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/planner.hpp"

#include "core/openivm_debug.hpp"

#include <chrono>

namespace duckdb {

static vector<RefreshMetadata::GroupRecomputeSourceOccurrence>
BuildGroupRecomputeSourceOccurrences(const CreateMVPlanFacts &facts) {
	vector<RefreshMetadata::GroupRecomputeSourceOccurrence> occurrences;
	unordered_map<string, idx_t> occurrence_index;
	for (auto &source : facts.source_occurrences) {
		if (source.table.empty()) {
			continue;
		}
		string key = StringUtil::Lower(source.table);
		auto it = occurrence_index.find(key);
		if (it == occurrence_index.end()) {
			occurrence_index[key] = occurrences.size();
			RefreshMetadata::GroupRecomputeSourceOccurrence occurrence;
			occurrence.table_name = source.table;
			occurrence.count = 1;
			occurrences.push_back(std::move(occurrence));
		} else {
			occurrences[it->second].count++;
		}
	}
	return occurrences;
}

static bool HasDuckLakeSourceForModel(const CreateMVPlanFacts &facts, const unordered_set<string> &table_names,
                                      bool target_is_ducklake) {
	if (!facts.ducklake_table_info.empty()) {
		return true;
	}
	if (!target_is_ducklake) {
		return false;
	}
	for (auto &table_name : table_names) {
		if (StringUtil::StartsWith(table_name, openivm::DATA_TABLE_PREFIX)) {
			return true;
		}
	}
	return false;
}

ParserExtensionPlanResult
MaterializedViewParserExtension::PlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                              unique_ptr<ParserExtensionParseData> parse_data) {
	// CREATE MATERIALIZED VIEW stores a relation. Physical insertion order is not
	// semantically observable unless users query with ORDER BY, so keep OpenIVM's
	// whole execution path on DuckDB's lower-memory unordered mode.
	ClientConfig::GetConfig(context).user_settings.SetUserSetting(PreserveInsertionOrderSetting::SettingIndex,
	                                                              Value::BOOLEAN(false));
	auto &parse_data_ref = dynamic_cast<MaterializedViewParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(parse_data_ref.statement.get());

	ParserExtensionPlanResult result;

	Connection con(*context.db.get());
	struct CreateMVPreProfileStep {
		string step_name;
		int64_t duration_ms;
		string detail;
	};
	vector<CreateMVPreProfileStep> create_profile_steps;
	Value create_profile_val;
	bool create_profile_enabled = context.TryGetCurrentSetting("openivm_profile_refresh", create_profile_val) &&
	                              !create_profile_val.IsNull() && create_profile_val.GetValue<bool>();
	auto create_profile_now = []() {
		return std::chrono::steady_clock::now();
	};
	auto add_create_profile_step = [&](const string &step_name, std::chrono::steady_clock::time_point start,
	                                   const string &detail = string()) {
		auto duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		create_profile_steps.push_back({step_name, duration_ms, detail});
	};

	// Capture the current catalog/schema from the originating context. DDLExecutorBindFunction
	// creates a fresh Connection that reflects the DatabaseInstance's physical default
	// catalog (not the session's USE setting). We only inject "USE catalog.schema" when
	// the session's active catalog differs from that physical default — e.g. when DuckLake
	// ("dl") is active but the file DB ("rewriter_benchmark_sf1") is the physical default.
	auto context_start = create_profile_now();
	string current_catalog;
	string current_schema;
	{
		auto &sp = ClientData::Get(context).catalog_search_path;
		auto def = sp->GetDefault();
		current_catalog = def.catalog;
		current_schema = def.schema.empty() ? "main" : def.schema;
	}
	add_create_profile_step("create_compile_session_context", context_start);
	// Query the physical default by running SELECT current_database() on the fresh `con`
	// (created above without any USE, so it reflects the DB's true default, not the session).
	auto default_context_start = create_profile_now();
	string default_db;
	string default_schema = "main";
	{
		auto res = con.Query("SELECT current_database()");
		if (!res->HasError() && res->RowCount() > 0) {
			default_db = res->GetValue(0, 0).ToString();
		}
		auto schema_res = con.Query("SELECT current_schema()");
		if (!schema_res->HasError() && schema_res->RowCount() > 0) {
			default_schema = schema_res->GetValue(0, 0).ToString();
		}
	}
	add_create_profile_step("create_compile_default_context", default_context_start);
	string default_catalog_schema =
	    KeywordHelper::WriteOptionallyQuoted(default_db) + "." + KeywordHelper::WriteOptionallyQuoted(default_schema);
	string current_catalog_schema = KeywordHelper::WriteOptionallyQuoted(current_catalog) + "." +
	                                KeywordHelper::WriteOptionallyQuoted(current_schema);

	// Handle ALTER MATERIALIZED VIEW — just execute the metadata UPDATE
	if (!parse_data_ref.alter_sql.empty()) {
		auto r = con.Query(parse_data_ref.alter_sql);
		if (r->HasError()) {
			throw CatalogException("Failed to alter materialized view: " + r->GetError());
		}
		// Return via the DDL executor with no DDL to run (the UPDATE already executed)
		ConfigureDDLExecutorResult(result);
		return result;
	}

	// PAC compatibility boundary: internal planning uses a fresh connection, so
	// forward PAC settings when that extension is loaded in the caller session.
	bool pac_loaded = IsPacLoaded(context);
	ForwardPacSettingsIfLoaded(context, con);

	auto name_resolution_start = create_profile_now();
	auto full_view_name = SqlUtils::ExtractTableName(statement->query);
	// Keep the user's raw AS-query as the source of truth for original-SQL fallback.
	// Do not recover this from DuckDB's parsed QueryNode::ToString(): that path is a
	// best-effort pretty-printer and has segfaulted on set-operation query nodes with
	// incomplete CTE/query internals. LPTS remains the normalized serializer below
	// for supported logical plans; this string is only the safe fallback input.
	auto original_view_query = SqlUtils::ExtractViewQuery(statement->query);

	// Split catalog-qualified name (e.g. "dl.mv_totals") into prefix and bare name.
	string view_catalog_prefix; // e.g. "dl." or "" for default catalog
	string view_name;           // bare name without catalog, e.g. "mv_totals"
	string view_target_catalog = current_catalog;
	string view_target_schema = current_schema;
	auto dot_pos = full_view_name.rfind('.');
	if (dot_pos != string::npos) {
		auto raw_prefix = full_view_name.substr(0, dot_pos);
		view_catalog_prefix = SqlUtils::QuoteQualifiedPrefix(raw_prefix + ".");
		auto schema_dot_pos = raw_prefix.rfind('.');
		if (schema_dot_pos != string::npos) {
			view_target_catalog = raw_prefix.substr(0, schema_dot_pos);
			view_target_schema = raw_prefix.substr(schema_dot_pos + 1);
		} else {
			view_target_catalog = raw_prefix;
			view_target_schema = current_schema.empty() ? "main" : current_schema;
		}
		view_name = full_view_name.substr(dot_pos + 1);
	} else {
		view_name = full_view_name;
		// When the MV name is unqualified but the session is in a non-default catalog
		// (e.g. USE dl.main), explicitly qualify so data/view tables land in dl rather
		// than the physical default. Metadata tables (unqualified) stay in the physical
		// default — PRAGMA refresh() always uses a fresh connection without USE.
		if (!current_catalog.empty() && current_catalog != default_db) {
			view_catalog_prefix = SqlUtils::QualifiedPrefix(current_catalog, current_schema);
		}
	}
	RefreshMetadata metadata(con);
	bool target_is_ducklake = metadata.IsDuckLakeCatalog(view_target_catalog);
	string internal_catalog_prefix = view_catalog_prefix;
	// Native MVs created from another active catalog keep OpenIVM state in the physical
	// default DB. DuckLake-targeted MVs store their data/delta tables in DuckLake so
	// initial materialization follows the same storage path as DuckLake CTAS.
	if (!target_is_ducklake && !view_catalog_prefix.empty() && default_db != "memory") {
		internal_catalog_prefix = SqlUtils::QualifiedPrefix(default_db, default_schema);
	}
	string data_table = IncrementalTableNames::DataTableName(view_name);
	string qdt = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table);
	string qvn = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
	string view_query = original_view_query; // will be overwritten by LPTS for DDL
	string top_k_suffix;                     // ORDER BY … LIMIT k, appended to the CREATE VIEW
	add_create_profile_step("create_compile_name_resolution", name_resolution_start,
	                        "target_ducklake=" + string(target_is_ducklake ? "true" : "false"));

	// Apply the session's active catalog to `con` so unqualified table references in the
	// MV query resolve in the user's catalog (e.g. `dl.main`) rather than the physical
	// default. Without this, `CREATE MATERIALIZED VIEW mv AS SELECT * FROM WAREHOUSE`
	// issued under `USE dl.main` fails during planning with "Table WAREHOUSE does not
	// exist" because the fresh connection resolves against the physical-default catalog.
	if (!current_catalog.empty() && current_catalog != default_db) {
		auto use_start = create_profile_now();
		con.Query("USE " + current_catalog + "." + current_schema);
		add_create_profile_step("create_compile_use_context", use_start, current_catalog_schema);
	}

	if (!parse_data_ref.is_replace) {
		// Fail before registering cleanup DDL. Otherwise a duplicate CREATE attempt
		// can fail on the pre-existing backing table and then cleanup would drop the
		// original MV's user-facing view/data table.
		if (RelationExists(con, qvn) || RelationExists(con, qdt)) {
			throw CatalogException("Table with name \"" + view_name + "\" already exists!");
		}
	}

	// Use con for planning — sees all committed state from previous bind-phase DDL
	con.BeginTransaction();
	// GetTableNames binds the query internally. For MV queries that DuckDB's binder
	// can't evaluate out-of-context (e.g. multi-column `(a, b) IN (SELECT x, y FROM t)`
	// triggers an ARRAY-sublink path that rejects the 2-column subquery), the call
	// throws. Catch and use an empty table_names — the later ExtractViewQuery path
	// re-derives what it needs from the plan.
	unordered_set<string> table_names;
	auto table_names_start = create_profile_now();
	try {
		table_names = con.GetTableNames(statement->query);
	} catch (const std::exception &e) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] GetTableNames failed: %s — continuing\n", e.what());
	}
	add_create_profile_step("create_compile_get_table_names", table_names_start,
	                        "tables=" + to_string(table_names.size()));

	// Plan the raw SELECT query once. The same plan serves both roles below:
	//   - select_plan: gets the full PlanRewrite (DISTINCT→GROUP BY, AVG→SUM+COUNT, …) and is
	//     serialized to the stored view_query via LPTS.
	//   - facts_plan: a copy taken BEFORE PlanRewrite, with only the tree-preserving light rewrites
	//     (FILTER→CASE, constant-subquery folding) applied, so the classifier and FindOuterAggregate
	//     see DISTINCT/AVG/etc. in their original shape. Deriving facts from the same SELECT that
	//     gets maintained also keeps classification consistent with the stored definition.
	// (Previously the full CREATE statement was parsed+planned a second time just for this; the
	//  CREATE wrapper is transparent to AnalyzeNode, so a copy of the SELECT body is equivalent.)
	unique_ptr<LogicalOperator> facts_plan;

	vector<string> output_names;
	string having_predicate; // HAVING predicate as SQL (for VIEW WHERE clause, empty if no HAVING)
	bool stored_query_has_aggregate_filter = false;
	bool pre_rewrite_has_aggregate_filter = false;
	bool has_hidden_minmax_having = false;
	bool has_computed_minmax_aggregate_projection = false;
	{
		auto select_parse_plan_start = create_profile_now();
		Parser select_parser;
		select_parser.ParseQuery(original_view_query);
		Planner select_planner(*con.context);
		select_planner.CreatePlan(std::move(select_parser.statements[0]));
		auto select_plan = std::move(select_planner.plan);
		add_create_profile_step("create_compile_select_plan", select_parse_plan_start);

		// Inline CTEs without running the full optimizer, which can reshape plans
		// before OpenIVM's structural rewrites.
		auto select_rewrite_start = create_profile_now();
		InlineCtesIfPresent(context, *select_planner.binder, select_plan);
		pre_rewrite_has_aggregate_filter = PlanContainsBoundAggregateFilter(select_plan.get());

		// Snapshot the plan for classification BEFORE the structural PlanRewrite below. The checker
		// must see DISTINCT/AVG/etc. unrewritten, so we apply only the two light, tree-preserving
		// rewrites here (same order as the former full-plan path: FILTER→CASE, then constant folding).
		facts_plan = select_plan->Copy(*con.context);
		RewriteAggregateFilters(context, facts_plan);
		FoldConstantScalarSubqueries(context, facts_plan);

		// Strip a bare top-level LIMIT (no ORDER BY) BEFORE the IVM rewrites so the hidden-column
		// rewrites (group COUNT(*), AVG/STDDEV decomposition, LEFT JOIN match keys) see the aggregate
		// or join underneath. A bare LIMIT is presentation-only — the data table holds the full
		// relation and the user-facing view applies the LIMIT. If the LIMIT node stays on top here,
		// InjectGroupCountStar etc. don't descend into it, so the view is maintained without
		// group-emptiness / match-count state (deleted groups linger as zero rows; LEFT JOIN counts
		// wrong). ORDER-BY-LIMIT / TOP_N keep their post-rewrite handling below (they need output_names
		// to render the ORDER BY suffix, and are genuine top-k shapes).
		if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_LIMIT && !select_plan->children.empty() &&
		    select_plan->children[0]->type != LogicalOperatorType::LOGICAL_ORDER_BY &&
		    select_plan->Cast<LogicalLimit>().limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			auto &limit_op = select_plan->Cast<LogicalLimit>();
			idx_t lval = limit_op.limit_val.GetConstantValue();
			idx_t oval = limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE
			                 ? limit_op.offset_val.GetConstantValue()
			                 : 0;
			top_k_suffix = "LIMIT " + to_string(lval);
			if (oval > 0) {
				top_k_suffix += " OFFSET " + to_string(oval);
			}
			select_plan = std::move(select_plan->children[0]);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped bare LIMIT before rewrite, suffix='%s'\n", top_k_suffix.c_str());
		}

		// Apply IVM plan rewrites (DISTINCT → GROUP BY + COUNT, AVG → SUM + COUNT, LEFT JOIN key)
		PlanRewrite(context, *select_planner.binder, select_plan, select_planner.names);

		output_names = PrepareOutputNames(select_plan.get(), select_planner.names);
		// Strip HAVING filter from plan — data table stores all groups.
		// The predicate is extracted as SQL (using output aliases) for the VIEW WHERE clause.
		having_predicate = StripHavingFilter(select_plan, output_names);
		auto aggregate_probes = AnalyzeCreateMvAggregateProbes(select_plan.get());
		stored_query_has_aggregate_filter = aggregate_probes.has_aggregate_filter;
		has_hidden_minmax_having = aggregate_probes.has_hidden_minmax_having;
		has_computed_minmax_aggregate_projection = aggregate_probes.has_computed_minmax_aggregate_projection;

		// Keep data tables unlimited/unordered; apply ORDER BY/LIMIT in the user-facing view.
		{
			LogicalOperator *limit_node = nullptr;
			LogicalOperator *order_node = nullptr;

			if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_TOP_N) {
				limit_node = select_plan.get();
				order_node = select_plan.get(); // same node holds both orders + limit
			} else if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_LIMIT &&
			           !select_plan->children.empty() &&
			           select_plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
				limit_node = select_plan.get();
				order_node = select_plan->children[0].get();
			}
			// Note: a bare LIMIT (no ORDER BY) is already stripped before PlanRewrite above, so the
			// hidden-column rewrites see the aggregate/join underneath. Only TOP_N / ORDER-BY-LIMIT
			// (genuine top-k, needing the ORDER BY suffix) are handled here.

			if (limit_node) {
				if (limit_node->type == LogicalOperatorType::LOGICAL_TOP_N) {
					auto &top_n = limit_node->Cast<LogicalTopN>();
					top_k_suffix = BuildTopKSuffix(top_n.orders, top_n.limit, top_n.offset, output_names);
					select_plan = std::move(select_plan->children[0]);
				} else {
					auto &order_op = order_node->Cast<LogicalOrder>();
					auto &limit_op = limit_node->Cast<LogicalLimit>();
					idx_t lval = 0;
					idx_t oval = 0;
					if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
						lval = limit_op.limit_val.GetConstantValue();
					}
					if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
						oval = limit_op.offset_val.GetConstantValue();
					}
					top_k_suffix = BuildTopKSuffix(order_op.orders, lval, oval, output_names);
					select_plan = std::move(select_plan->children[0]->children[0]);
				}
				OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped top-k wrapper, suffix='%s'\n", top_k_suffix.c_str());
			}
		}

		// Strip a standalone ORDER_BY at the top of select_plan (e.g. DISTINCT + ORDER BY
		// without LIMIT, or simple projection + ORDER BY). The data table stores unordered
		// rows; the suffix is appended to the CREATE VIEW instead.
		if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_ORDER_BY && top_k_suffix.empty() &&
		    !select_plan->children.empty()) {
			auto &order_op = select_plan->Cast<LogicalOrder>();
			top_k_suffix = BuildTopKSuffix(order_op.orders, 0, 0, output_names);
			select_plan = std::move(select_plan->children[0]);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped standalone ORDER_BY, suffix='%s'\n", top_k_suffix.c_str());
		}
		add_create_profile_step("create_compile_select_rewrite", select_rewrite_start,
		                        "output_cols=" + to_string(output_names.size()));

		auto lpts_start = create_profile_now();
		try {
			// CREATE MATERIALIZED VIEW always stores the view body in DuckDB's own dialect.
			// Refresh-time target dialects are selected per CompileFacts.
			SqlDialect dialect = SqlDialect::DUCKDB;
			view_query = SqlUtils::PlanToSql(*con.context, select_plan, dialect, /*use_newlines=*/true, output_names);
			StringUtil::Trim(view_query);
			OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS view query: %s\n", view_query.c_str());
		} catch (const std::exception &e) {
			// LPTS must be able to serialize every plan DuckDB can produce. A failure here is a bug
			// in LPTS (third_party/lpts), not something to silently work around — fail loudly so the
			// gap gets fixed instead of storing un-normalized SQL the refresh path can't maintain.
			throw NotImplementedException("IVM: LPTS could not serialize the plan for materialized view '" + view_name +
			                              "': " + string(e.what()) +
			                              ". LPTS must support every query; fix it in third_party/lpts.");
		}
		add_create_profile_step("create_compile_lpts", lpts_start, "query_bytes=" + to_string(view_query.size()));
	}
	con.Rollback();

	OPENIVM_DEBUG_PRINT("[CREATE MV] View name: %s\n", view_name.c_str());
	OPENIVM_DEBUG_PRINT("[CREATE MV] View query: %s\n", view_query.c_str());
	OPENIVM_DEBUG_PRINT("[CREATE MV] Logical plan:\n%s\n", facts_plan->ToString().c_str());

	// facts_plan was snapshotted from select_plan before PlanRewrite and already carries the two
	// light rewrites (FILTER→CASE, constant folding) the checker needs — see the copy site above.
	auto analysis_start = create_profile_now();
	auto facts = BuildCreateMVPlanFacts(facts_plan.get(), current_catalog);
	if (!facts.source_table_info.empty()) {
		table_names.clear();
		for (const auto &entry : facts.source_table_info) {
			table_names.insert(entry.second.table_name);
		}
	}
	auto analysis = facts.analysis;
	add_create_profile_step("create_compile_analyze_plan", analysis_start,
	                        "sources=" + to_string(table_names.size()) +
	                            "; ducklake_sources=" + to_string(facts.ducklake_table_info.size()));
	auto classification_start = create_profile_now();
	// Keep partition metadata for joined windows. The refresh compiler uses plan-walk lineage when it can cover all
	// changed sources and otherwise falls back to full recompute, so dropping the metadata here only prevents safe
	// partial/cascade plans from being compiled.
	bool keep_window_join_partitions = true;
	bool has_full_outer_aggregate = analysis.found_full_outer && analysis.found_aggregation;
	bool has_cte_self_join = facts.has_repeated_cte_ref_under_join;
	// An outer join beneath a set operation cannot get its match-count/key rewrite (it would break
	// set-op branch arity). Non-aggregate shapes stay FULL_REFRESH in the classifier; aggregate shapes
	// with stable visible group keys can use GROUP_RECOMPUTE with current-diff affected keys instead.
	bool has_unsupported_incremental_construct = facts.has_unsupported_set_operation || facts.has_pivot;

	DeltaViewModelInput model_input;
	model_input.facts = &facts;
	model_input.output_names = &output_names;
	model_input.has_unsupported_incremental_construct = has_unsupported_incremental_construct;
	model_input.keep_window_join_partitions = keep_window_join_partitions;
	model_input.stored_query_has_aggregate_filter =
	    stored_query_has_aggregate_filter || pre_rewrite_has_aggregate_filter;
	model_input.stored_query_has_top_k = false;
	model_input.has_hidden_minmax_having = has_hidden_minmax_having;
	model_input.has_computed_minmax_aggregate_projection = has_computed_minmax_aggregate_projection;
	model_input.has_ducklake_source = HasDuckLakeSourceForModel(facts, table_names, target_is_ducklake);
	const bool distinct_at_top = IsDistinctAtTop(analysis, output_names);

	// Populated by ExtractInnerDistinct when classified as DISTINCT_INCREMENTAL.
	vector<string> distinct_extracted_cols;
	string distinct_extracted_input_sql;
	string distinct_extracted_source;
	string distinct_extracted_filter;
	// Outer-aggregate spec for the v0 aux-state pipeline. Single SUM(<arg>) only —
	// any other shape (multiple SUMs, AVG, COUNT, etc.) demotes to GROUP_RECOMPUTE.
	// `sum_arg` is the column name from the DISTINCT input (one of distinct_cols).
	// `sum_out` is the user-facing output column name in the data table.
	string distinct_sum_arg;
	string distinct_sum_out;
	SemiAntiExtract semi_anti_extract;
	vector<string> semi_anti_left_cols;
	FilteredGroupCountExtract filtered_group_count_extract;
	FilteredGroupCountAuxRequirement filtered_group_count_aux_candidate;
	if (analysis.found_nested_aggregate &&
	    RecognizeFilteredGroupCount(facts, output_names, filtered_group_count_extract)) {
		string aux_table = "openivm_filtered_group_count_" + view_name;
		string group_q = KeywordHelper::WriteOptionallyQuoted(filtered_group_count_extract.group_col);
		string sum_q = KeywordHelper::WriteOptionallyQuoted(filtered_group_count_extract.sum_col);
		filtered_group_count_aux_candidate = {
		    {aux_table, SqlUtils::LastIdentifierPart(filtered_group_count_extract.source),
		     filtered_group_count_extract.group_col, filtered_group_count_extract.sum_col, group_q, sum_q,
		     filtered_group_count_extract.output_col, filtered_group_count_extract.comparison_op,
		     filtered_group_count_extract.threshold_sql},
		    filtered_group_count_extract.source};
		model_input.filtered_group_count_aux_candidate = &filtered_group_count_aux_candidate;
	}

	if (analysis.found_semi_anti_join && !analysis.found_aggregation) {
		if (ExtractSemiAntiQuery(original_view_query, semi_anti_extract)) {
			string left_table_name = SqlUtils::LastIdentifierPart(semi_anti_extract.left_table);
			auto col_result =
			    con.Query("SELECT column_name FROM information_schema.columns WHERE lower(table_name) = lower('" +
			              SqlUtils::EscapeSingleQuotes(left_table_name) + "') AND table_schema = '" +
			              SqlUtils::EscapeSingleQuotes(current_schema) + "' ORDER BY ordinal_position");
			auto add_semi_anti_left_col = [&](const string &col_name) {
				for (auto &existing : semi_anti_left_cols) {
					if (StringUtil::CIEquals(existing, col_name)) {
						return;
					}
				}
				semi_anti_left_cols.push_back(col_name);
			};
			if (!semi_anti_extract.output_cols.empty()) {
				for (auto &col : semi_anti_extract.output_cols) {
					add_semi_anti_left_col(col);
				}
				// The aux state must evaluate the semi/anti predicate on refresh. If the
				// MV projects only a subset of the left table, keep the base columns as
				// hidden aux-state key columns as well (e.g. output C_ID/C_LAST, predicate
				// uses C_W_ID). The user-facing MV still emits only output_cols.
				if (semi_anti_extract.left_table.find('(') == string::npos && !col_result->HasError() &&
				    col_result->RowCount() > 0) {
					for (idx_t i = 0; i < col_result->RowCount(); i++) {
						add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
					}
				}
			} else if (!col_result->HasError() && col_result->RowCount() > 0) {
				for (idx_t i = 0; i < col_result->RowCount(); i++) {
					add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
				}
			}
		}
	}

	if (analysis.found_distinct && !distinct_at_top && analysis.found_aggregation) {
		// Inner DISTINCT under an aggregate. Two paths:
		//   - `openivm_distinct_aux_state = true` AND single-source body → DISTINCT_INCREMENTAL.
		//     Maintains per-DISTINCT-tuple count auxiliary state; on refresh emits ±1
		//     only on count transitions across zero (DBSP distinct(R)=sgn(R[t])). Strictly
		//     fewer rows reach the parent aggregate's MERGE than GROUP_RECOMPUTE.
		//   - Otherwise → GROUP_RECOMPUTE: re-evaluate only the outer GROUP BY keys touched
		//     by source deltas. Correctness-equivalent fallback; multi-source views stay
		//     there until the aux-state path can substitute each source independently.
		// Read the flag from the user's ClientContext (`context`), not the local
		// `con` — the local connection is a fresh one and doesn't inherit the
		// caller's session settings.
		Value aux_val;
		bool aux_enabled = false;
		if (context.TryGetCurrentSetting("openivm_distinct_aux_state", aux_val) && !aux_val.IsNull()) {
			aux_enabled = aux_val.GetValue<bool>();
		}
		bool single_source = table_names.size() == 1;
		if (aux_enabled && single_source) {
			vector<string> dcols;
			string d_input_sql, d_source, d_filter;
			if (!ExtractInnerDistinct(original_view_query, dcols, d_input_sql, d_source, d_filter)) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL extractor failed — demoting to "
				                    "GROUP_RECOMPUTE\n");
			} else {
				distinct_extracted_cols = std::move(dcols);
				distinct_extracted_input_sql = std::move(d_input_sql);
				distinct_extracted_source = std::move(d_source);
				distinct_extracted_filter = std::move(d_filter);
			}
		}
		// Walk the rewritten plan for the outer aggregate's expressions. v0 supports
		// exactly one SUM(<arg>) — `openivm_count_star` (auto-injected by PlanRewrite)
		// is allowed alongside it. Anything else (AVG, COUNT, MIN/MAX, multiple SUMs)
		// demotes back to GROUP_RECOMPUTE.
		if (!distinct_extracted_cols.empty()) {
			LogicalAggregate *outer_agg = FindOuterAggregate(facts_plan.get());
			int sum_count = 0;
			bool unsupported_agg = false;
			if (outer_agg) {
				for (auto &expr : outer_agg->expressions) {
					if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
						continue;
					}
					auto &bound = expr->Cast<BoundAggregateExpression>();
					const string &fname = bound.function.name;
					if (fname == "count_star") {
						continue; // injected by PlanRewrite — fine
					}
					if (fname != "sum") {
						unsupported_agg = true;
						break;
					}
					if (bound.children.empty() ||
					    bound.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
						unsupported_agg = true;
						break;
					}
					auto &bcr = bound.children[0]->Cast<BoundColumnRefExpression>();
					distinct_sum_arg = bcr.alias.empty() ? bcr.GetName() : bcr.alias;
					distinct_sum_out = bound.alias;
					sum_count++;
				}
			}
			if (!outer_agg || unsupported_agg || sum_count != 1) {
				distinct_sum_arg.clear();
				distinct_sum_out.clear();
				OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL outer-agg not single-SUM — demoting "
				                    "to GROUP_RECOMPUTE\n");
			} else if (distinct_sum_out.empty()) {
				// `SUM(c) AS s` puts the alias `s` on the SELECT-list BCR above the
				// aggregate, not on the BoundAggregateExpression itself. Recover it
				// from output_names: the SUM output column is the first non-group
				// position (group cols come first in the data table layout).
				if (analysis.group_count < output_names.size()) {
					distinct_sum_out = output_names[analysis.group_count];
				}
			}
		}
	}

	RefreshMetadata::DistinctAuxMeta distinct_aux_candidate;
	if (!distinct_sum_arg.empty() && !distinct_sum_out.empty()) {
		string aux_table = "openivm_distinct_count_" + view_name;
		distinct_aux_candidate = {aux_table,
		                          distinct_extracted_cols,
		                          distinct_extracted_cols,
		                          distinct_extracted_input_sql,
		                          distinct_extracted_source,
		                          distinct_extracted_filter,
		                          distinct_sum_arg,
		                          distinct_sum_out};
		model_input.distinct_aux_candidate = &distinct_aux_candidate;
	}
	RefreshMetadata::SemiAntiAuxMeta semi_anti_aux_candidate;
	if (!semi_anti_left_cols.empty()) {
		vector<string> semi_anti_left_exprs;
		for (size_t i = 0; i < semi_anti_left_cols.size(); i++) {
			string qcol = KeywordHelper::WriteOptionallyQuoted(semi_anti_left_cols[i]);
			string source_expr = semi_anti_extract.left_alias + "." + qcol;
			for (size_t j = 0; j < semi_anti_extract.output_cols.size(); j++) {
				if (StringUtil::CIEquals(semi_anti_extract.output_cols[j], semi_anti_left_cols[i]) &&
				    j < semi_anti_extract.output_exprs.size()) {
					source_expr = semi_anti_extract.output_exprs[j];
					break;
				}
			}
			semi_anti_left_exprs.push_back(source_expr);
		}
		string aux_table = "openivm_semi_anti_state_" + view_name;
		semi_anti_aux_candidate = {aux_table,
		                           semi_anti_extract.join_type,
		                           semi_anti_extract.left_table,
		                           semi_anti_extract.left_alias,
		                           semi_anti_extract.right_table,
		                           semi_anti_extract.right_alias,
		                           semi_anti_extract.predicate,
		                           semi_anti_extract.post_filter,
		                           semi_anti_left_cols,
		                           semi_anti_left_exprs,
		                           semi_anti_extract.output_cols};
		model_input.semi_anti_aux_candidate = &semi_anti_aux_candidate;
	}
	auto view_model = BuildDeltaViewModel(model_input);
	auto lineage_start = create_profile_now();
	PopulateDeltaViewModelLineage(view_model, facts, output_names);
	string lineage_json = BuildDeltaViewModelLineageJson(view_model);
	int64_t lineage_duration_ms =
	    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - lineage_start).count();
	idx_t lineage_entry_count = view_model.LineageEntryCount();
	string model_profile_detail = create_profile_enabled ? BuildDeltaViewModelProfileDetail(view_model) : string();
	RefreshType refresh_type = view_model.type;
	auto aggregate_columns = std::move(view_model.group_columns);
	auto aggregate_types = std::move(view_model.aggregate_types);
	auto window_partition_columns = std::move(view_model.window_partition_columns);
	bool has_minmax_metadata = view_model.has_minmax_metadata;
	auto group_recompute_affected_mode = view_model.group_recompute_affected_mode;
	auto group_recompute_source_occurrences = BuildGroupRecomputeSourceOccurrences(facts);
	string full_outer_join_cols = std::move(view_model.full_outer_join_cols);

	if (view_model.warn_unsupported_incremental) {
		Printer::Print("Warning: materialized view '" + view_name +
		               "' uses constructs not supported for incremental maintenance. "
		               "Full refresh will be used.");
	}
	if (view_model.warn_unrecognized_pattern) {
		Printer::Print("Warning: materialized view '" + view_name +
		               "' has an unrecognized query pattern. Full refresh will be used.");
	}

	add_create_profile_step("create_compile_classification", classification_start, model_profile_detail);

	OPENIVM_DEBUG_PRINT("[CREATE MV] Detected IVM type: %s (aggregation=%d, projection=%d, group_cols=%zu)\n",
	                    RefreshTypeName(refresh_type), (int)analysis.found_aggregation, (int)analysis.found_projection,
	                    aggregate_columns.size());
	for (auto reason : view_model.strategy_reasons) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] Strategy reason: %s\n", DeltaStrategyReasonName(reason));
	}
	for (auto feature : view_model.features) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] Delta model feature: %s\n", DeltaModelFeatureName(feature));
	}
	for (auto reason : view_model.unsupported_reasons) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] Delta unsupported reason: %s\n", DeltaUnsupportedReasonName(reason));
	}
	for (auto semantics : view_model.update_semantics) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] Delta update semantics: %s\n", DeltaUpdateSemanticsName(semantics));
	}
	for (auto &domain : view_model.affected_domains) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] Affected domain: %s keys=%zu sources=%zu delta_local=%d "
		                    "needs_lookup=%d\n",
		                    DeltaAffectedDomainKindName(domain.kind), domain.key_columns.size(),
		                    domain.source_tables.size(), (int)domain.delta_local, (int)domain.needs_base_lookup);
	}
	for (auto &node : view_model.nodes) {
		OPENIVM_DEBUG_PRINT("[CREATE MV] Delta node %llu: %s rule=%s children=%zu sources=%zu keys=%zu "
		                    "occurrence=%llu domains=%zu lineage=%zu semantics=%zu unsupported=%zu "
		                    "maintenance=%s state=%s\n",
		                    static_cast<unsigned long long>(node.id), DeltaModelNodeKindName(node.kind),
		                    DeltaRuleKindName(node.rule), node.children.size(), node.source_tables.size(),
		                    node.affected_key_columns.size(), static_cast<unsigned long long>(node.source_occurrence),
		                    node.affected_domains.size(), node.lineage_facts.size(), node.update_semantics.size(),
		                    node.unsupported_reasons.size(), DeltaMaintenanceModeName(node.maintenance.mode),
		                    DeltaMaintenanceStateKindName(node.maintenance.state));
	}
	OPENIVM_DEBUG_PRINT("[CREATE MV] Source tables:");
	for (const auto &t : table_names) {
		OPENIVM_DEBUG_PRINT(" %s", t.c_str());
	}
	OPENIVM_DEBUG_PRINT("\n");

	vector<string> ddl;
	vector<string> cleanup_ddl;
	vector<string> metadata_ddl;
	vector<string> aux_metadata_ddl;
	auto add_cleanup = [&](const string &query) {
		cleanup_ddl.push_back(string(OPENIVM_DDL_CLEANUP_PREFIX) + query);
	};
	auto add_profile_marker = [&](const string &step_name, const string &detail = string()) {
		ddl.push_back(string(OPENIVM_DDL_PROFILE_PREFIX) + view_name + "\t" + step_name + "\t" + detail);
	};
	auto add_profile_record = [&](const string &step_name, int64_t duration_ms, const string &detail = string()) {
		ddl.push_back(string(OPENIVM_DDL_PROFILE_RECORD_PREFIX) + view_name + "\t" + step_name + "\t" +
		              to_string(duration_ms) + "\t" + detail);
	};
	for (const auto &step : create_profile_steps) {
		add_profile_record(step.step_name, step.duration_ms, step.detail);
	}

	add_profile_marker("create_mv_system_tables", "refresh_type=" + string(RefreshTypeName(refresh_type)));
	AppendCreateMVSystemTablesDDL(ddl, view_name, parse_data_ref.is_replace);

	if (parse_data_ref.is_replace) {
		add_profile_marker("create_mv_replace_cleanup");
		string qvn_drop = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
		string qdt_drop = internal_catalog_prefix +
		                  KeywordHelper::WriteOptionallyQuoted(IncrementalTableNames::DataTableName(view_name));
		string qdv_drop =
		    internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
		ddl.push_back("DROP VIEW IF EXISTS " + qvn_drop);
		ddl.push_back("DROP TABLE IF EXISTS " + qdt_drop);
		ddl.push_back("DROP TABLE IF EXISTS " + qdv_drop);
		ddl.push_back("DELETE FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
		              SqlUtils::EscapeSingleQuotes(view_name) + "'");
		ddl.push_back("DELETE FROM " + string(openivm::HISTORY_TABLE) + " WHERE view_name = '" +
		              SqlUtils::EscapeSingleQuotes(view_name) + "'");
	}

	string refresh_val = parse_data_ref.refresh_interval > 0 ? to_string(parse_data_ref.refresh_interval) : "null";
	auto &cols_to_store = analysis.found_window ? window_partition_columns : aggregate_columns;
	string group_cols_val = SqlCsvLiteralOrNull(cols_to_store);
	string agg_types_val = SqlCsvLiteralOrNull(aggregate_types);
	string having_val = having_predicate.empty() ? "null" : "'" + SqlUtils::EscapeSingleQuotes(having_predicate) + "'";
	string group_recompute_mode_val = "'" + string(GroupRecomputeAffectedModeName(group_recompute_affected_mode)) + "'";
	string group_recompute_source_occurrences_json =
	    RefreshMetadata::GroupRecomputeSourceOccurrencesToJson(group_recompute_source_occurrences);
	string group_recompute_source_occurrences_val =
	    group_recompute_source_occurrences.empty()
	        ? "null"
	        : "'" + SqlUtils::EscapeSingleQuotes(group_recompute_source_occurrences_json) + "'";

	string full_outer_join_cols_val = "null";
	if (analysis.found_full_outer) {
		if (!full_outer_join_cols.empty()) {
			full_outer_join_cols_val = "'" + SqlUtils::EscapeSingleQuotes(full_outer_join_cols) + "'";
		}
	}

	metadata_ddl.push_back("insert or replace into " + string(openivm::VIEWS_TABLE) +
	                       " (view_name, sql_string, type, has_minmax, has_left_join, last_update, "
	                       "refresh_interval, refresh_in_progress, group_columns, aggregate_types, "
	                       "having_predicate, group_recompute_affected_mode, "
	                       "group_recompute_source_occurrences_json, has_full_outer, full_outer_join_cols) values ('" +
	                       view_name + "', '" + SqlUtils::EscapeSingleQuotes(view_query) + "', " +
	                       to_string((int)refresh_type) + ", " + (has_minmax_metadata ? "true" : "false") + ", " +
	                       (analysis.found_left_join ? "true" : "false") + ", now(), " + refresh_val + ", false, " +
	                       group_cols_val + ", " + agg_types_val + ", " + having_val + ", " + group_recompute_mode_val +
	                       ", " + group_recompute_source_occurrences_val + ", " +
	                       (analysis.found_full_outer ? "true" : "false") + ", " + full_outer_join_cols_val + ")");

	if (!lineage_json.empty()) {
		aux_metadata_ddl.push_back(BuildUpdateViewJsonSQL("lineage_json", lineage_json, view_name));
	}
	add_profile_record("create_compile_lineage", lineage_duration_ms, "entries=" + to_string(lineage_entry_count));

	Value match_flag_val;
	bool view_matching_enabled = context.TryGetCurrentSetting("openivm_enable_view_matching", match_flag_val) &&
	                             !match_flag_val.IsNull() && BooleanValue::Get(match_flag_val);
	if (view_matching_enabled) {
		// table_names may include openivm_data_<x> when this MV reads from
		// another MV (DuckDB binds the user-facing view to its data table).
		// Strip the prefix so source_tables_json reflects user-facing names
		// and the dependency-edge lookup hits a registered MV row.
		vector<string> sorted_tables;
		sorted_tables.reserve(table_names.size());
		for (const auto &t : table_names) {
			if (StringUtil::StartsWith(t, openivm::DATA_TABLE_PREFIX)) {
				sorted_tables.push_back(t.substr(strlen(openivm::DATA_TABLE_PREFIX)));
			} else {
				sorted_tables.push_back(t);
			}
		}
		std::sort(sorted_tables.begin(), sorted_tables.end());
		metadata_ddl.push_back(
		    BuildUpdateViewJsonSQL("source_tables_json", SqlUtils::JsonArray(sorted_tables), view_name));
		// Replace any prior edges for this child, then re-emit. INSERTs are
		// conditional on the source being a registered MV (the SELECT
		// returns zero rows for non-MV sources).
		metadata_ddl.push_back("DELETE FROM " + string(openivm::MV_DEPS_TABLE) + " WHERE child_view = '" +
		                       SqlUtils::EscapeSingleQuotes(view_name) + "'");
		for (const auto &t : sorted_tables) {
			metadata_ddl.push_back("INSERT INTO " + string(openivm::MV_DEPS_TABLE) +
			                       " (parent_view, child_view, edge_kind) SELECT view_name, '" +
			                       SqlUtils::EscapeSingleQuotes(view_name) + "', 'direct' FROM " +
			                       string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
			                       SqlUtils::EscapeSingleQuotes(t) + "'");
		}
	}

	// DISTINCT_INCREMENTAL: create the per-tuple count auxiliary table and store its
	// metadata so refresh-time can find the source SQL, the column list, and the aux
	// table name. The aux table is populated from the (DISTINCT-stripped) input SQL
	// at CREATE time; refresh-time MERGE keeps it in sync with delta multiplicities.
	if (view_model.HasDistinctAux()) {
		add_profile_marker("create_mv_distinct_aux");
		const auto &meta = view_model.distinct_aux;
		string aux_target = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(meta.aux_table);
		string aux_create =
		    BuildDistinctAuxStateCreateSQL(aux_target, meta.cols, meta.source_exprs, "(" + meta.input_sql + ")", "",
		                                   /*replace=*/false);
		ddl.push_back(aux_create);
		add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
		            KeywordHelper::WriteOptionallyQuoted(meta.aux_table));
		aux_metadata_ddl.push_back(
		    BuildUpdateViewJsonSQL("distinct_aux_meta_json", RefreshMetadata::DistinctAuxMetaToJson(meta), view_name));
	}

	if (view_model.HasFilteredGroupCountAux()) {
		add_profile_marker("create_mv_filtered_group_count_aux");
		const auto &req = view_model.filtered_group_count_aux;
		const auto &meta = req.meta;
		string source_table = QualifyCreateSourceTable(req.create_source, current_catalog, current_schema, default_db);
		string aux_target = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(meta.aux_table);
		string aux_create = BuildFilteredGroupCountAuxStateCreateSQL(
		    aux_target, source_table, meta.group_col, meta.sum_col, meta.source_group_expr, meta.source_sum_expr,
		    /*replace=*/false);
		ddl.push_back(aux_create);
		add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
		            KeywordHelper::WriteOptionallyQuoted(meta.aux_table));
		aux_metadata_ddl.push_back(BuildUpdateViewJsonSQL(
		    "aggregate_decomposition_json", RefreshMetadata::FilteredGroupCountAuxMetaToJson(meta), view_name));
	}

	if (view_model.HasSemiAntiAux()) {
		add_profile_marker("create_mv_semi_anti_aux");
		const auto &meta = view_model.semi_anti_aux;
		string left_source_table =
		    QualifyCreateSourceTable(meta.left_table, current_catalog, current_schema, default_db);
		string right_source_table =
		    QualifyCreateSourceTable(meta.right_table, current_catalog, current_schema, default_db);
		string aux_target = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(meta.aux_table);
		string aux_create = BuildSemiAntiAuxStateCreateSQL(
		    aux_target, left_source_table, meta.left_alias, right_source_table, meta.right_alias, meta.predicate,
		    meta.post_filter, meta.left_cols, meta.left_exprs, /*replace=*/false);
		ddl.push_back(aux_create);
		add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
		            KeywordHelper::WriteOptionallyQuoted(meta.aux_table));
		aux_metadata_ddl.push_back(
		    BuildUpdateViewJsonSQL("semi_anti_aux_meta_json", RefreshMetadata::SemiAntiAuxMetaToJson(meta), view_name));
	}

	const auto &source_table_info = facts.source_table_info;
	const auto &dl_table_info = facts.ducklake_table_info; // keyed by lowercased name

	unordered_set<string> ducklake_tables;
	// Single snapshot query per DuckLake catalog (all tables share the same snapshot).
	string dl_snapshot_val = "null";
	if (!dl_table_info.empty()) {
		// Use the first entry's catalog — all source tables in one MV share one catalog.
		string cat = dl_table_info.begin()->second.catalog_name;
		auto snapshot_id = metadata.GetCurrentDuckLakeSnapshot(cat);
		if (snapshot_id >= 0) {
			dl_snapshot_val = to_string(snapshot_id);
		}
	}

	vector<string> source_metadata_values;
	unordered_map<string, vector<string>> snapshot_update_tables_by_catalog;
	unordered_set<string> inserted_meta_table_names;
	for (const auto &table_name : table_names) {
		string catalog_type = "duckdb";
		string snapshot_val = "null";
		string source_table_id_val = "null";
		string meta_table_name = SqlUtils::DeltaName(table_name);
		string source_catalog_val = current_catalog.empty() ? "memory" : current_catalog;
		string source_schema_val = current_schema.empty() ? "main" : current_schema;

		string table_lc = StringUtil::Lower(table_name);
		auto source_info_it = source_table_info.find(table_name);
		if (source_info_it != source_table_info.end()) {
			source_catalog_val = source_info_it->second.catalog_name;
			source_schema_val = source_info_it->second.schema_name;
		}
		auto it = dl_table_info.find(table_lc);
		if (it != dl_table_info.end()) {
			catalog_type = "ducklake";
			meta_table_name = it->second.table_name; // case-preserved name
			ducklake_tables.insert(it->second.table_name);
			ducklake_tables.insert(table_name); // also insert SQL-parsed name
			snapshot_val = dl_snapshot_val;
			if (it->second.table_id >= 0) {
				source_table_id_val = to_string(it->second.table_id);
			}
			source_catalog_val = it->second.catalog_name;
			source_schema_val = it->second.schema_name;
			snapshot_update_tables_by_catalog[source_catalog_val].push_back(meta_table_name);
			OPENIVM_DEBUG_PRINT("[CREATE MV] DuckLake table '%s' → meta_name='%s', snap=%s\n", table_name.c_str(),
			                    meta_table_name.c_str(), snapshot_val.c_str());
		}

		// A single physical source can appear under multiple logical names after planning.
		// DuckLake chained views are the common case: the query can contain both the
		// user-facing MV name and its backing openivm_data_* table. Metadata is keyed by
		// (view_name, table_name), so emit only one dependency row for the canonical
		// metadata table name after the DuckLake mapping above.
		if (!inserted_meta_table_names.insert(meta_table_name).second) {
			continue;
		}

		source_metadata_values.push_back(
		    "('" + SqlUtils::EscapeSingleQuotes(view_name) + "', '" + SqlUtils::EscapeSingleQuotes(meta_table_name) +
		    "', now(), '" + SqlUtils::EscapeSingleQuotes(catalog_type) + "', " + snapshot_val + ", now(), '" +
		    SqlUtils::EscapeSingleQuotes(source_catalog_val) + "', '" +
		    SqlUtils::EscapeSingleQuotes(source_schema_val) + "', " + source_table_id_val + ")");
	}
	vector<string> source_metadata_ddl;
	if (!source_metadata_values.empty()) {
		source_metadata_ddl.push_back("insert or replace into " + string(openivm::DELTA_TABLES_TABLE) +
		                              " (view_name, table_name, last_update, catalog_type, last_snapshot_id, "
		                              "last_refresh_ts, source_catalog, source_schema, source_table_id) values " +
		                              StringUtil::Join(source_metadata_values, ", "));
	}

	// --- Compiled DDL (MV creation, delta tables, delta view) ---
	// Physical data table stores all columns (including openivm_* internal cols).
	// DuckLake-targeted MVs store the data/delta tables in DuckLake and keep only
	// OpenIVM metadata in the physical default catalog. CREATE/REFRESH split metadata
	// writes from DuckLake data writes because DuckDB cannot commit one transaction
	// across both catalogs.
	if (SqlUtils::GetBoolSetting(context, "openivm_explain_initial_load", false)) {
		// This diagnostic intentionally reports the exact first heavy statement that
		// CREATE MV will run. DuckLake-targeted MVs should now write openivm_data_*
		// directly; if staging reappears, this output makes the extra copy visible.
		if (!current_catalog.empty() && current_catalog != default_db) {
			con.Query("USE " + current_catalog_schema);
		}
		string initial_load_statement = "CREATE TABLE " + qdt + " AS " + view_query;
		string diagnostic;
		diagnostic += "\n[OpenIVM initial-load diagnostic]\n";
		diagnostic += "view_name: " + view_name + "\n";
		diagnostic += "refresh_type: " + string(RefreshTypeName(refresh_type)) + "\n";
		diagnostic += "uses_staging_table: false\n";
		diagnostic += "initial_load_statement:\n" + initial_load_statement + "\n\n";
		diagnostic += "original_view_query:\n" + original_view_query + "\n\n";
		diagnostic += "generated_view_query:\n" + view_query + "\n\n";
		diagnostic += ExplainInitialLoadQuery(con, "EXPLAIN original_view_query:", original_view_query);
		diagnostic += ExplainInitialLoadQuery(con, "EXPLAIN generated_view_query:", view_query);
		diagnostic += ExplainInitialLoadQuery(con, "EXPLAIN initial_load_statement:", initial_load_statement);
		Printer::Print(diagnostic);

		Value files_path_val;
		if (context.TryGetCurrentSetting("openivm_files_path", files_path_val) && !files_path_val.IsNull()) {
			SqlUtils::WriteFile(files_path_val.ToString() + "/openivm_initial_load_explain_" + view_name + ".txt",
			                    false, diagnostic);
		}
		if (SqlUtils::GetBoolSetting(context, "openivm_explain_initial_load_only", false)) {
			ConfigureDDLExecutorResult(result);
			return result;
		}
	}
	// The view_query may contain unqualified base-table references (e.g. `FROM WAREHOUSE`
	// when the user wrote the MV under `USE dl.main`). The DDL executor's fresh
	// Connection starts in the physical-default catalog, so apply USE before CREATE
	// TABLE AS so those unqualified names resolve in the MV's catalog.
	add_profile_marker("create_mv_initial_load", "sources=" + to_string(table_names.size()) +
	                                                 "; generated_query_bytes=" + to_string(view_query.size()));
	if (!current_catalog.empty() && current_catalog != default_db) {
		ddl.push_back("use " + current_catalog_schema);
	}
	ddl.push_back("create table " + qdt + " as " + view_query);
	if (!view_catalog_prefix.empty()) {
		// Keep the same connection after a DuckLake CTAS. Reopening here can force
		// connection teardown while DuckLake still owns the transaction used by the
		// CTAS, which surfaces as a self-inflicted SQLite metadata "database is
		// locked" on complex chained MV creates.
		ddl.push_back("use " + default_catalog_schema);
	}
	add_cleanup("DROP VIEW IF EXISTS " + qvn);
	add_cleanup("DROP TABLE IF EXISTS " + qdt);
	if (pac_loaded) {
		add_profile_marker("create_mv_session_settings", "pac");
		ddl.push_back("SET pac_check = false");
		ddl.push_back("SET pac_rewrite = false");
	}

	// User-facing VIEW hides internal openivm_* columns via EXCLUDE.
	// If LPTS fell back to the original SQL, the data table has only the
	// user-visible columns — no `openivm_*` columns even if the rewritten plan
	// would have added them via AVG/STDDEV decomposition. Skip the EXCLUDE
	// list in that case; otherwise CREATE VIEW fails on nonexistent columns.
	add_profile_marker("create_mv_user_view");
	{
		// Collect internal column names from the LPTS output
		vector<string> internal_cols;
		for (auto &name : output_names) {
			if (IncrementalTableNames::IsInternalColumn(name)) {
				internal_cols.push_back(name);
			}
		}
		string having_where = having_predicate.empty() ? "" : " where " + having_predicate;
		// For aggregate+top-k the VIEW appends ORDER BY ... LIMIT k after the HAVING WHERE.
		string top_k_view_suffix = top_k_suffix.empty() ? "" : " " + top_k_suffix;
		string view_tail = having_where + top_k_view_suffix;
		if (internal_cols.empty()) {
			ddl.push_back("create view " + qvn + " as select * from " + qdt + view_tail);
		} else {
			ddl.push_back("create view " + qvn + " as select * exclude (" + SqlUtils::JoinQuotedColumns(internal_cols) +
			              ") from " + qdt + view_tail);
		}
	}

	add_profile_marker("create_mv_source_delta_tables", "source_count=" + to_string(table_names.size()));
	for (const auto &table_name : table_names) {
		// DuckLake tables don't need delta tables — change tracking is native.
		// `ducklake_tables` stores the catalog-normalized (lowercase) name, so
		// compare against a normalized copy of the SQL-parsed name.
		string table_lc = StringUtil::Lower(table_name);
		if (ducklake_tables.count(table_name) || ducklake_tables.count(table_lc)) {
			OPENIVM_DEBUG_PRINT("[CREATE MV] Skipping delta table for DuckLake table '%s'\n", table_name.c_str());
			continue;
		}

		Value catalog_value;
		Value schema_value;
		auto source_it = source_table_info.find(table_name);
		if (source_it != source_table_info.end()) {
			catalog_value = Value(source_it->second.catalog_name);
			schema_value = Value(source_it->second.schema_name);
		}

		if (catalog_value.IsNull() && !context.db->config.options.database_path.empty()) {
			// Look up the catalog name for this table via Catalog API
			con.BeginTransaction();
			auto entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name,
			                                                  OnEntryNotFound::RETURN_NULL);
			if (entry) {
				catalog_value = Value(entry->ParentCatalog().GetName());
				schema_value = Value(entry->schema.name);
			}
			con.Rollback();
		}
		if (catalog_value.IsNull()) {
			catalog_value = Value(current_catalog.empty() ? "memory" : current_catalog);
		}

		if (schema_value.IsNull()) {
			schema_value = Value(current_schema.empty() ? "main" : current_schema);
		}

		auto catalog_schema = SqlUtils::QualifiedPrefix(catalog_value.ToString(), schema_value.ToString());

		ddl.push_back("create table if not exists " + catalog_schema +
		              KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(table_name)) +
		              " as select *, 1::INTEGER as " + string(openivm::MULTIPLICITY_COL) + ", now()::timestamp as " +
		              string(openivm::TIMESTAMP_COL) + " from " + catalog_schema +
		              KeywordHelper::WriteOptionallyQuoted(table_name) + " limit 0");
	}

	// Delta table for the MV — based on the DATA table (has all columns)
	add_profile_marker("create_mv_mv_delta_table");
	string qdv = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
	ddl.push_back(string(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX) + qdv + "\t" + qdt);
	add_cleanup("DROP TABLE IF EXISTS " + qdv);

	// --- Index DDL (for aggregate group queries) ---
	// DuckLake source scans and DuckLake-backed MV state do not support this optional
	// native index.
	if ((refresh_type == RefreshType::AGGREGATE_GROUP || refresh_type == RefreshType::AGGREGATE_HAVING) &&
	    !aggregate_columns.empty() && ducklake_tables.empty() && view_catalog_prefix.empty()) {
		add_profile_marker("create_view_index", "columns=" + to_string(aggregate_columns.size()));
		string index_name = KeywordHelper::WriteOptionallyQuoted(data_table + openivm::INDEX_SUFFIX);
		ddl.push_back("create unique index " + index_name + " on " + qdt + "(" +
		              SqlUtils::JoinQuotedColumns(aggregate_columns) + ")");
	}

	// Restore physical-default catalog so subsequent unqualified references to
	// system tables (openivm_delta_tables, etc.) resolve correctly. The USE
	// inserted before `create table qdt as view_query` routed unqualified base
	// tables through the user's catalog; flip back for the metadata UPDATE below.
	if (!current_catalog.empty() && current_catalog != default_db) {
		add_profile_marker("create_mv_restore_catalog");
		ddl.push_back("use " + default_catalog_schema);
	}

	// Record source-table metadata only after physical MV objects exist. If a later
	// DuckLake publish fails, the DDL executor removes these rows before the retry.
	add_profile_marker("create_mv_source_metadata", "rows=" + to_string(source_metadata_ddl.size()));
	ddl.insert(ddl.end(), source_metadata_ddl.begin(), source_metadata_ddl.end());
	add_cleanup("DELETE FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	            SqlUtils::EscapeSingleQuotes(view_name) + "'");

	// After all tables are created and populated, update DuckLake snapshot IDs
	// to the current snapshot. This ensures the first refresh only sees changes
	// made AFTER the MV was created (not the initial data load).
	add_profile_marker("create_mv_snapshot_metadata");
	for (auto &entry : snapshot_update_tables_by_catalog) {
		unordered_set<string> seen_tables;
		vector<string> table_literals;
		for (auto &table_name : entry.second) {
			if (!seen_tables.insert(table_name).second) {
				continue;
			}
			table_literals.push_back("'" + SqlUtils::EscapeSingleQuotes(table_name) + "'");
		}
		if (table_literals.empty()) {
			continue;
		}
		// Use the DuckLake catalog for current_snapshot() — NOT view_catalog_prefix
		// or current_catalog. For cross-system MVs (native MV reading from dl.*),
		// view_catalog_prefix is empty and current_catalog is the physical-default
		// (e.g. the file DB) which doesn't have `current_snapshot()`.
		string snapshot_catalog = KeywordHelper::WriteOptionallyQuoted(entry.first);
		ddl.push_back("UPDATE " + string(openivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = (SELECT id FROM " +
		              snapshot_catalog + ".current_snapshot()) WHERE view_name = '" +
		              SqlUtils::EscapeSingleQuotes(view_name) + "' AND table_name IN (" +
		              StringUtil::Join(table_literals, ", ") + ")");
	}

	// Publish the MV metadata last. CREATE MV touches both the physical DuckDB catalog
	// and DuckLake's external metadata catalog, so OpenIVM cannot rely on a single
	// cross-catalog transaction. The executor registers cleanup DDL up front and this
	// late publish keeps incomplete attempts out of openivm_views.
	add_profile_marker("create_mv_publish_metadata",
	                   "rows=" + to_string(metadata_ddl.size() + aux_metadata_ddl.size()));
	ddl.insert(ddl.end(), metadata_ddl.begin(), metadata_ddl.end());
	ddl.insert(ddl.end(), aux_metadata_ddl.begin(), aux_metadata_ddl.end());
	add_cleanup("DELETE FROM " + string(openivm::MV_DEPS_TABLE) + " WHERE child_view = '" +
	            SqlUtils::EscapeSingleQuotes(view_name) + "'");
	add_cleanup("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	            SqlUtils::EscapeSingleQuotes(view_name) + "'");

	OPENIVM_DEBUG_PRINT("[CREATE MV] Compiled %lu DDL queries for bind phase\n", (unsigned long)ddl.size());

	// Write reference SQL files if openivm_files_path is set
	Value files_path_val;
	if (context.TryGetCurrentSetting("openivm_files_path", files_path_val) && !files_path_val.IsNull()) {
		string base_path = files_path_val.ToString();
		// System tables DDL (first 3 statements: openivm_views, openivm_refresh_hooks,
		// openivm_delta_tables)
		string system_tables_sql;
		// Compiled queries (everything after the system tables)
		string compiled_sql;
		idx_t visible_ddl_idx = 0;
		for (size_t i = 0; i < ddl.size(); i++) {
			if (StringUtil::StartsWith(ddl[i], OPENIVM_DDL_PROFILE_PREFIX)) {
				continue;
			}
			if (visible_ddl_idx < 3) {
				system_tables_sql += ddl[i] + ";\n\n";
			} else if (StringUtil::StartsWith(ddl[i], OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)) {
				compiled_sql += "-- OpenIVM derives the MV delta-table schema from the physical data table "
				                "at DDL execution time.\n\n";
			} else {
				compiled_sql += ddl[i] + ";\n\n";
			}
			visible_ddl_idx++;
		}
		SqlUtils::WriteFile(base_path + "/openivm_system_tables.sql", false, system_tables_sql);
		SqlUtils::WriteFile(base_path + "/openivm_compiled_queries_" + view_name + ".sql", false, compiled_sql);
	}

	// Pass DDL via result.parameters — the bind function receives them as input.inputs.
	// This replaces the fragile thread-local pending-DDL mechanism.
	for (auto &q : cleanup_ddl) {
		result.parameters.push_back(Value(q));
	}
	for (auto &q : ddl) {
		result.parameters.push_back(Value(q));
	}

	// Return DDL executor table function
	ConfigureDDLExecutorResult(result);
	return result;
}
} // namespace duckdb
