#include "core/parser.hpp"

#include "core/incremental_checker.hpp"
#include "core/plan_rewrite.hpp"
#include "core/openivm_constants.hpp"
#include "core/parser_ddl.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/parser_sql_extractors.hpp"
#include "core/refresh_metadata.hpp"
#include "lpts_pipeline.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
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

namespace duckdb {

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

	if (parse_data_ref.plan) {
		Connection con(*context.db.get());

		// Capture the current catalog/schema from the originating context. DDLExecutorBindFunction
		// creates a fresh Connection that reflects the DatabaseInstance's physical default
		// catalog (not the session's USE setting). We only inject "USE catalog.schema" when
		// the session's active catalog differs from that physical default — e.g. when DuckLake
		// ("dl") is active but the file DB ("rewriter_benchmark_sf1") is the physical default.
		string current_catalog;
		string current_schema;
		{
			auto &sp = ClientData::Get(context).catalog_search_path;
			auto def = sp->GetDefault();
			current_catalog = def.catalog;
			current_schema = def.schema.empty() ? "main" : def.schema;
		}
		// Query the physical default by running SELECT current_database() on the fresh `con`
		// (created above without any USE, so it reflects the DB's true default, not the session).
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
		string default_catalog_schema = KeywordHelper::WriteOptionallyQuoted(default_db) + "." +
		                                KeywordHelper::WriteOptionallyQuoted(default_schema);
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

		auto full_view_name = SqlUtils::ExtractTableName(statement->query);
		bool statement_needs_original_sql_for_lpts = QueryNeedsOriginalSqlForLpts(statement->query);
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

		// Apply the session's active catalog to `con` so unqualified table references in the
		// MV query resolve in the user's catalog (e.g. `dl.main`) rather than the physical
		// default. Without this, `CREATE MATERIALIZED VIEW mv AS SELECT * FROM WAREHOUSE`
		// issued under `USE dl.main` fails during planning with "Table WAREHOUSE does not
		// exist" because the fresh connection resolves against the physical-default catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			con.Query("USE " + current_catalog + "." + current_schema);
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
		try {
			table_names = con.GetTableNames(statement->query);
		} catch (const std::exception &e) {
			OPENIVM_DEBUG_PRINT("[CREATE MV] GetTableNames failed: %s — continuing\n", e.what());
		}

		// Plan the full CREATE TABLE AS SELECT statement (for plan walking)
		Planner planner(*con.context);
		planner.CreatePlan(statement->Copy());
		auto plan = std::move(planner.plan);

		// Inline CTEs in this plan too so AnalyzePlan / find_group_cols / HasLeftJoin
		// walks see the folded structure. DuckDB's binder defaults to
		// CTE_MATERIALIZE_ALWAYS, which makes CTEInlining bail — relax to DEFAULT first.
		// (The SELECT-only `select_plan` below does the same for LPTS serialization.)
		InlineCtesIfPresent(context, *planner.binder, plan);

		unordered_map<string, SourceTableInfo> source_table_info;
		CollectSourceTables(plan.get(), source_table_info);
		if (!source_table_info.empty()) {
			table_names.clear();
			for (const auto &entry : source_table_info) {
				table_names.insert(entry.second.table_name);
			}
		}
		unordered_map<string, DuckLakeSourceTableInfo> dl_table_info_for_classification;
		CollectDuckLakeTables(plan.get(), current_catalog, dl_table_info_for_classification);

		// Plan the raw SELECT query separately for IVM plan rewrite + LPTS conversion
		vector<string> output_names;
		string having_predicate;    // HAVING predicate as SQL (for VIEW WHERE clause, empty if no HAVING)
		bool lpts_fallback = false; // set when LPTS can't serialize the plan and we fall back to SQL
		bool original_has_repeated_cte_ref_under_join = false;
		{
			Parser select_parser;
			select_parser.ParseQuery(original_view_query);
			Planner select_planner(*con.context);
			select_planner.CreatePlan(std::move(select_parser.statements[0]));
			auto select_plan = std::move(select_planner.plan);
			original_has_repeated_cte_ref_under_join = HasRepeatedCteRefUnderJoin(select_plan.get());

			// Inline CTEs so the refresh rewriter sees one maintainable operator tree.
			// Query-bound CTEs become LOGICAL_MATERIALIZED_CTE + LOGICAL_CTE_REF nodes
			// in the bound plan; CTEInlining rewrites small/non-recursive ones into
			// direct subqueries. Running the full DuckDB optimizer here would also
			// reorder joins / push filters, which conflicts with IVM's subsequent plan
			// rewrites — so we run only the CTE-inlining pass, and only when a CTE
			// reference actually appears in the plan (the optimizer isn't always a
			// no-op on CTE-free plans — e.g. it can rewrite DISTINCT subqueries in ways
			// that confuse the downstream structural rewrites).
			// DuckDB's binder sets LogicalMaterializedCTE::materialize to
			// CTE_MATERIALIZE_ALWAYS by default. CTEInlining bails early on ALWAYS
			// and leaves the CTE as a materialized node. The refresh path has no
			// delta-consolidation rule for LOGICAL_CTE_REF, so relax every CTE to
			// CTE_MATERIALIZE_DEFAULT before inlining. Single-ref CTEs always inline;
			// multi-ref CTEs inline when they're cheap and don't end in an aggregate
			// that would be wastefully re-materialized.
			InlineCtesIfPresent(context, *select_planner.binder, select_plan);

			// Apply IVM plan rewrites (DISTINCT → GROUP BY + COUNT, AVG → SUM + COUNT, LEFT JOIN key)
			PlanRewrite(context, *select_planner.binder, select_plan, select_planner.names);

			output_names = PrepareOutputNames(select_plan.get(), select_planner.names);
			// Strip HAVING filter from plan — data table stores all groups.
			// The predicate is extracted as SQL (using output aliases) for the VIEW WHERE clause.
			having_predicate = StripHavingFilter(select_plan, output_names);

			// For aggregate+top-k: extract the ORDER BY/LIMIT suffix, then strip the top-k
			// wrapper(s) so LPTS serializes only the inner aggregate query. The suffix is
			// appended to the CREATE VIEW definition so ORDER BY LIMIT is applied at read
			// time over the fully-maintained data table.
			//
			// Two plan shapes depending on whether top_n optimizer ran:
			//   (A) LOGICAL_TOP_N → child  [top_n disabled by LPTS — never reached here]
			//   (B) LOGICAL_LIMIT → LOGICAL_ORDER_BY → child  [LPTS active — common case]
			//
			// Projection top-k uses the same split: maintain the unlimited projection in
			// the data table and apply ORDER BY ... LIMIT in the user-facing view.
			{
				LogicalOperator *limit_node = nullptr;
				LogicalOperator *order_node = nullptr;

				if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_TOP_N) {
					// Shape (A): fused top-n node
					limit_node = select_plan.get();
					order_node = select_plan.get(); // same node holds both orders + limit
				} else if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_LIMIT &&
				           !select_plan->children.empty() &&
				           select_plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
					// Shape (B): separate LIMIT + ORDER_BY nodes
					limit_node = select_plan.get();
					order_node = select_plan->children[0].get();
				}

				if (limit_node) {
					// Strip top-k unconditionally. The data table stores the unlimited result,
					// while the user-facing view applies ORDER BY ... LIMIT at read time.
					if (limit_node->type == LogicalOperatorType::LOGICAL_TOP_N) {
						auto &top_n = limit_node->Cast<LogicalTopN>();
						top_k_suffix = BuildTopKSuffix(top_n.orders, top_n.limit, top_n.offset, output_names);
						select_plan = std::move(select_plan->children[0]);
					} else {
						// Shape (B): extract from the two separate nodes
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
						// Strip: select_plan = LOGICAL_ORDER_BY's child
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

			if (statement_needs_original_sql_for_lpts || QueryNeedsOriginalSqlForLpts(original_view_query)) {
				view_query = original_view_query;
				lpts_fallback = true;
				OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS can't round-trip this construct — using original SQL: %s\n",
				                    view_query.c_str());
			} else {
				try {
					auto ast = LogicalPlanToAst(*con.context, select_plan);
					auto cte_list = AstToCteList(*ast);
					view_query = cte_list->ToQuery(true, output_names);
					if (!view_query.empty() && view_query.back() == ';') {
						view_query.pop_back();
					}
					StringUtil::Trim(view_query);
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS view query: %s\n", view_query.c_str());
				} catch (const std::exception &e) {
					// LPTS doesn't support all operators (e.g., WINDOW). Fall back to original SQL.
					// This is fine for partition-recompute views that don't need LPTS-rewritten queries.
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS fallback (%s) to original query: %s\n", e.what(),
					                    view_query.c_str());
				} catch (...) {
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS fallback (unknown exception) to original query: %s\n",
					                    view_query.c_str());
				}
			}
			// For views that LPTS silently mis-serializes (GROUPING SETS / ROLLUP / CUBE
			// → plain GROUP BY; STRUCT_PACK field names → tN_col aliases; etc.), detect
			// structurally and prefer the original SQL. Those constructs never need the
			// LPTS-rewritten form anyway — they're maintained by recompute-style paths
			// using the original SQL, so the rewriter-rule path (which needs LPTS) isn't used.
			{
				if (PlanNeedsOriginalSqlForLpts(select_plan.get())) {
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS can't round-trip this construct — using original SQL: %s\n",
					                    view_query.c_str());
				}
			}
		}
		con.Rollback();

		OPENIVM_DEBUG_PRINT("[CREATE MV] View name: %s\n", view_name.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] View query: %s\n", view_query.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Logical plan:\n%s\n", plan->ToString().c_str());

		// Normalize FILTER aggregates in the full plan before analysis so the checker
		// sees CASE expressions instead of raw FILTER and doesn't set incremental_compatible=false.
		// (PlanRewrite already rewrote select_plan for the LPTS view_query above.)
		RewriteAggregateFilters(context, plan);

		// Single-pass plan analysis: validates IVM compatibility AND extracts metadata
		auto analysis = AnalyzePlan(plan.get());
		MVClassificationState classification(analysis);
		if (analysis.found_delim_join && !classification.found_aggregation && !analysis.found_single_join) {
			// Preserve DuckDB's dependent/DELIM_JOIN plan shape for refresh. LPTS can
			// round-trip lateral table functions, but its CTE-normalized SQL lowers them
			// into ordinary joins/table-function scans; that bypasses IncrementalDelimJoinRule
			// and sends the refresh plan through the generic N-way join rule instead.
			view_query = original_view_query;
			lpts_fallback = true;
		}
		if (classification.found_filtered_list) {
			view_query = original_view_query;
			lpts_fallback = true;
			OPENIVM_DEBUG_PRINT("[CREATE MV] LIST FILTER requires original SQL for group-recompute: %s\n",
			                    view_query.c_str());
		}
		// Derive GROUP BY column names by walking the plan's projection above the aggregate.
		// The projection maps GROUP BY bindings (group_index, i) to SELECT-list aliases — these
		// aliases are the actual column names in the data table. Plan-walk aliases on agg.groups
		// are unreliable for CASE/COALESCE expressions.
		// For DISTINCT views, the checker already extracts aggregate_columns from distinct_targets.
		size_t group_count = analysis.group_count;
		idx_t group_index = analysis.group_index;
		vector<string> aggregate_columns;
		bool join_key_group_fallback = false;
		// DISTINCT is only the MV's grouping when its targets actually appear in the MV's
		// output columns. An inner-subquery DISTINCT (e.g. `SELECT COUNT(*) FROM (SELECT
		// DISTINCT x FROM t)`, or a CTE like `WITH cte AS (SELECT DISTINCT x FROM t) ...`)
		// exposes columns that never reach the data table, so its targets can't drive
		// AGGREGATE_GROUP classification or the unique index.
		bool distinct_at_top = false;
		if (classification.found_distinct && !analysis.aggregate_columns.empty() && !output_names.empty()) {
			unordered_set<string> output_lc;
			for (auto &n : output_names) {
				output_lc.insert(StringUtil::Lower(n));
			}
			distinct_at_top = true;
			for (auto &t : analysis.aggregate_columns) {
				string lc = StringUtil::Lower(t);
				if (!output_lc.count(lc)) {
					distinct_at_top = false;
					break;
				}
			}
		}
		// Detect UNION / UNION ALL over aggregates once, shared by the group_cols block below
		// (to skip key extraction) and the classification chain (to route to RECOMPUTE).
		// True only when the UNION node is an ancestor of the aggregate (union-of-aggs pattern).
		// For agg-over-union (e.g. SELECT ... FROM (t1 UNION ALL t2) GROUP BY ...) the
		// UNION is a descendant of the aggregate — that is a normal AGGREGATE_GROUP view.
		bool has_union_over_agg = classification.found_aggregation && HasUnionBeforeAggregate(plan.get());
		bool union_distinct_over_agg = classification.found_distinct && distinct_at_top && has_union_over_agg;

		if (union_distinct_over_agg) {
			aggregate_columns =
			    DeriveGroupColumnNames(plan.get(), group_index, group_count, output_names, has_union_over_agg);
		} else if (distinct_at_top) {
			aggregate_columns = std::move(analysis.aggregate_columns);
		} else if (classification.found_distinct && analysis.aggregate_columns.empty()) {
			// Plain DISTINCT (no explicit targets) — trust the checker.
			aggregate_columns = std::move(analysis.aggregate_columns);
		} else if (group_count > 0 && group_index != DConstants::INVALID_INDEX) {
			auto group_names_list =
			    DeriveGroupColumnNames(plan.get(), group_index, group_count, output_names, has_union_over_agg);
			for (auto &name : group_names_list) {
				aggregate_columns.push_back(name);
			}
		}

		// LATERAL/correlated subquery aggregates are represented by DEPENDENT/DELIM joins.
		// The inner aggregate's group binding is not projected as a top-level GROUP BY, so
		// the normal group-column derivation above leaves aggregate_columns empty and would
		// misclassify the MV as a scalar SIMPLE_AGGREGATE. For this shape, the visible
		// non-aggregate output columns are the affected-key columns that scope
		// GROUP_RECOMPUTE.
		bool delim_aggregate_group_fallback = false;
		if (analysis.found_delim_join && classification.found_aggregation && aggregate_columns.empty() &&
		    output_names.size() > analysis.aggregate_types.size()) {
			idx_t key_count = output_names.size() - analysis.aggregate_types.size();
			for (idx_t i = 0; i < key_count; i++) {
				if (!output_names[i].empty() && !IncrementalTableNames::IsInternalColumn(output_names[i])) {
					aggregate_columns.push_back(output_names[i]);
				}
			}
			delim_aggregate_group_fallback = !aggregate_columns.empty();
			if (delim_aggregate_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] DELIM/DEPENDENT aggregate: using %zu visible key columns for "
				                    "GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		bool scalar_delim_projection_group_fallback = false;
		if (analysis.found_delim_join && classification.found_single_join && !classification.found_aggregation &&
		    aggregate_columns.empty()) {
			auto delim_key_names = DeriveScalarDelimKeyColumnNames(plan.get(), output_names);
			for (auto &name : delim_key_names) {
				aggregate_columns.push_back(name);
			}
			scalar_delim_projection_group_fallback = !aggregate_columns.empty();
			if (scalar_delim_projection_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Scalar DELIM/DEPENDENT projection: using %zu visible key columns "
				                    "for GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		bool nested_aggregate_group_fallback = false;
		if (classification.found_nested_aggregate && aggregate_columns.empty()) {
			auto nested_group_names = DeriveAggregateGroupColumnNames(plan.get(), output_names, false);
			for (auto &name : nested_group_names) {
				if (!IncrementalTableNames::IsInternalColumn(name)) {
					aggregate_columns.push_back(name);
				}
			}
			nested_aggregate_group_fallback = !aggregate_columns.empty();
			if (nested_aggregate_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Nested aggregate: using %zu visible inner group columns for "
				                    "GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		bool repeated_cte_aggregate_group_fallback = false;
		if (classification.found_aggregation && aggregate_columns.empty()) {
			auto cte_group_names = DeriveAggregateGroupColumnNames(plan.get(), output_names, true);
			for (auto &name : cte_group_names) {
				if (!IncrementalTableNames::IsInternalColumn(name)) {
					aggregate_columns.push_back(name);
				}
			}
			repeated_cte_aggregate_group_fallback = !aggregate_columns.empty();
			if (repeated_cte_aggregate_group_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Repeated CTE aggregate under join: using %zu visible group columns "
				                    "for GROUP_RECOMPUTE\n",
				                    aggregate_columns.size());
			}
		}

		// Deduplicate aggregate_columns the same way we deduped output_names above.
		// When the user writes `SELECT DISTINCT w_id, w_id` (or groups twice on the
		// same column), the data table has columns `w_id, w_id_1` after the output_names
		// dedup — the unique index and MERGE ON/UPDATE SET clauses read group names
		// from aggregate_columns, so they must match the data table's deduped names.
		// Without this, the second occurrence gets treated as an aggregate column in
		// `UPDATE SET w_id_1 = v.w_id_1 + d.w_id_1` and values are summed instead of matched.
		{
			unordered_set<string> seen_group;
			for (auto &name : aggregate_columns) {
				if (IncrementalTableNames::IsInternalColumn(name)) {
					continue;
				}
				string candidate = name;
				idx_t suffix = 1;
				while (seen_group.count(candidate)) {
					candidate = name + "_" + to_string(suffix++);
				}
				seen_group.insert(candidate);
				name = candidate;
			}
		}
		auto aggregate_types = std::move(analysis.aggregate_types);
		auto window_partition_columns = std::move(analysis.window_partition_columns);
		ResolveWindowPartitionOutputNames(plan.get(), window_partition_columns, output_names);

		// Fallback group-key extraction for inner-aggregate-in-join patterns.
		// When the top-level SELECT joins against a subquery/CTE that performs GROUP BY,
		// find_group_cols fails (the inner aggregate's group_index isn't referenced by the
		// outer projection — the join key comes from the non-aggregate arm). Recover group
		// keys by matching the outermost JOIN's condition BCRs against the top projection.
		//
		// Use GROUP_RECOMPUTE for these views: AGGREGATE_GROUP would try to SUM pass-through
		// attributes (e.g. W_YTD from WAREHOUSE) as if they were aggregate results.
		if (classification.found_join && group_count > 0 && !has_union_over_agg) {
			// plan root is LOGICAL_CREATE_TABLE; descend to find the SELECT output
			// projection and outermost comparison join.
			auto *top_proj_ptr = FindFirstProjection(plan.get());
			auto *cjoin = FindFirstComparisonJoin(plan.get());
			if (top_proj_ptr && cjoin) {
				// Collect (table_index, column_index) pairs that appear in join conditions.
				unordered_map<idx_t, unordered_set<idx_t>> join_key_cols;
				for (auto &cond : cjoin->conditions) {
					AddJoinKeyColumn(cond.left, join_key_cols);
					AddJoinKeyColumn(cond.right, join_key_cols);
				}
				// Match those bindings against the top projection's output expressions.
				auto &top_proj = *top_proj_ptr;
				for (idx_t expr_i = 0; expr_i < top_proj.expressions.size(); expr_i++) {
					auto &expr = top_proj.expressions[expr_i];
					if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
						continue;
					}
					auto &bcr = expr->Cast<BoundColumnRefExpression>();
					auto it = join_key_cols.find(bcr.binding.table_index);
					if (it == join_key_cols.end() || !it->second.count(bcr.binding.column_index)) {
						continue;
					}
					string col_name;
					if (!expr->alias.empty()) {
						col_name = expr->alias;
					} else if (expr_i < output_names.size() && !output_names[expr_i].empty() &&
					           !IncrementalTableNames::IsInternalColumn(output_names[expr_i])) {
						col_name = output_names[expr_i];
					} else {
						col_name = bcr.GetName();
					}
					if (!IncrementalTableNames::IsInternalColumn(col_name)) {
						bool exists = false;
						for (auto &existing : aggregate_columns) {
							if (StringUtil::CIEquals(existing, col_name)) {
								exists = true;
								break;
							}
						}
						if (!exists) {
							aggregate_columns.push_back(col_name);
							join_key_group_fallback = true;
						}
					}
				}
			}
		}
		if (classification.found_join && classification.found_aggregation && !aggregate_columns.empty()) {
			ResolveAggregateGroupColumnsThroughJoinKeys(plan.get(), aggregate_columns, output_names);
		}

		bool join_aggregate_projection_fallback = false;
		if (classification.found_join && classification.found_aggregation && !aggregate_columns.empty()) {
			idx_t expected_linear_outputs = aggregate_columns.size() + aggregate_types.size();
			join_aggregate_projection_fallback = output_names.size() > expected_linear_outputs;
			if (join_aggregate_projection_fallback) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Join-over-aggregate exposes %zu columns but only %zu are "
				                    "group/aggregate outputs — using GROUP_RECOMPUTE\n",
				                    output_names.size(), expected_linear_outputs);
			}
		}

		// Window over join: non-DuckLake delta tables may not expose the partition key for
		// every changed source, so native window recompute could miss partitions. DuckLake
		// can compare the old/new view result at source snapshots, so it can safely keep
		// partition keys even for multi-source window joins.
		bool all_sources_are_ducklake = !table_names.empty();
		if (all_sources_are_ducklake) {
			for (const auto &table_name : table_names) {
				string table_lc = StringUtil::Lower(table_name);
				bool is_ducklake_scan =
				    dl_table_info_for_classification.find(table_lc) != dl_table_info_for_classification.end();
				// DuckLake views created by OpenIVM expose a DuckLake catalog view over an
				// internal physical openivm_data_* table. When DuckDB expands such a view while
				// planning a chained MV, the scan is physical even though the source's change
				// tracking is still DuckLake-backed.
				bool is_ducklake_mv_backing =
				    !view_catalog_prefix.empty() && StringUtil::StartsWith(table_name, openivm::DATA_TABLE_PREFIX);
				if (!is_ducklake_scan && !is_ducklake_mv_backing) {
					all_sources_are_ducklake = false;
					break;
				}
			}
		}
		bool single_source_window_join =
		    classification.found_window && classification.found_join && table_names.size() == 1;
		if (classification.found_window && classification.found_join && !single_source_window_join &&
		    !all_sources_are_ducklake) {
			window_partition_columns.clear();
		}

		// LEFT/RIGHT/OUTER JOIN aggregate with a non-trivial aggregate argument
		// (e.g. `SUM(COALESCE(h.x, 0))`, `AVG(1)`, `SUM(CASE …)`) or a non-BCR
		// projection wrapping an aggregate output (e.g. `COALESCE(SUM(…), 0)`)
		// breaks the Larson & Zhou MERGE logic: the NULL-semantics shortcut that
		// resets right-side aggregates to NULL when match_count=0 produces the
		// wrong value when the stored expression would evaluate to the COALESCE
		// default (0). Route these to group-recompute by setting found_minmax so
		// the view is compiled with has_minmax=true. Detection runs only when
		// the view already has a LEFT/RIGHT/OUTER JOIN and an AGGREGATE.
		//   query_1502: COALESCE(SUM(ol.OL_QUANTITY), 0) AS ordered
		//   query_1696/1699: SUM(COALESCE(h.H_AMOUNT, 0))
		//   query_1746/1749: SUM(COALESCE(1, 0)), AVG(1)
		if ((classification.found_left_join || classification.found_full_outer) && classification.found_aggregation) {
			if (OuterJoinAggregateNeedsRecompute(plan.get(), analysis.group_index)) {
				OPENIVM_DEBUG_PRINT(
				    "[CREATE MV] LEFT/OUTER JOIN aggregate with computed aggregate or projection wrapper — "
				    "using group-recompute (found_minmax=true)\n");
				classification.found_minmax = true;
			}
		}

		// FULL OUTER JOIN with aggregate (above OR below) cannot be safely maintained
		// via inclusion-exclusion deltas: outer-side null-padded rows don't survive
		// deletion deltas correctly, so SUM/COUNT over them drift. Force RECOMPUTE
		// regardless of how the aggregate's expressions are structured.
		// Catches q1353/q1355 (FULL OUTER JOIN + GROUP BY + SUM).
		bool has_full_outer_aggregate = classification.found_full_outer && classification.found_aggregation;

		// Materialized CTE referenced 2+ times below a JOIN: the planner emits one
		// base-table scan inside the CTE and shares it via CTE_REF nodes, so the
		// IncrementalJoinRule sees a single physical scan instead of an N-way self-join.
		// Inclusion-exclusion can't generate the right delta terms — rows for the
		// shared scan aren't replicated across both join sides. Route to RECOMPUTE.
		// Catches ducklake_0240 (CTE referenced twice on both sides of an INNER JOIN).
		bool has_cte_self_join = HasRepeatedCteRefUnderJoin(plan.get());
		bool has_unsupported_set_operation = HasUnsupportedSetOperation(plan.get());
		bool has_unsupported_incremental_construct = QueryHasUnsupportedIncrementalConstruct(original_view_query);
		if (has_unsupported_set_operation || has_unsupported_incremental_construct) {
			// These views are maintained by full refresh, so store the user's query directly.
			// The CREATE-time IVM rewrites can add hidden columns for incremental paths (e.g.
			// LEFT JOIN match keys) that do not survive SQL set-operation arity rules.
			view_query = original_view_query;
			lpts_fallback = true;
		}

		RefreshType refresh_type;
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
		bool has_filtered_group_count_aux =
		    classification.found_nested_aggregate &&
		    ExtractFilteredGroupCount(original_view_query, output_names, filtered_group_count_extract);

		if (has_unsupported_set_operation || has_unsupported_incremental_construct) {
			refresh_type = RefreshType::FULL_REFRESH;
		} else if (classification.found_window) {
			// Window functions use partition-level recompute (not full IVM, but better than full refresh)
			refresh_type = RefreshType::WINDOW_PARTITION;
		} else if (classification.found_grouping_sets) {
			// ROLLUP / CUBE / GROUPING SETS produce multiple grouping sets per row;
			// maintain them by recomputing the grouping-set keys touched by source
			// deltas.
			refresh_type = aggregate_columns.empty() ? RefreshType::FULL_REFRESH : RefreshType::GROUP_RECOMPUTE;
		} else if (classification.found_semi_anti_join && classification.found_aggregation) {
			// SEMI/ANTI joins are thresholded by match-count transitions. The aux-state path below
			// supports projection/filter stacks over one left base table; aggregates over SEMI/ANTI
			// need a separate transition-to-aggregate compiler. Use full recompute until that lands.
			refresh_type = RefreshType::FULL_REFRESH;
		} else if (classification.found_semi_anti_join && !classification.found_aggregation) {
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
					refresh_type =
					    semi_anti_left_cols.empty() ? RefreshType::FULL_REFRESH : RefreshType::SEMI_ANTI_RECOMPUTE;
				} else if (!col_result->HasError() && col_result->RowCount() > 0) {
					for (idx_t i = 0; i < col_result->RowCount(); i++) {
						add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
					}
					refresh_type = RefreshType::SEMI_ANTI_RECOMPUTE;
				} else {
					refresh_type = RefreshType::FULL_REFRESH;
				}
			} else {
				refresh_type = RefreshType::FULL_REFRESH;
			}
		} else if (!classification.incremental_compatible) {
			refresh_type = RefreshType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' uses constructs not supported for incremental maintenance. "
			               "Full refresh will be used.");
		} else if (classification.found_filtered_list && !aggregate_columns.empty()) {
			refresh_type = RefreshType::GROUP_RECOMPUTE;
		} else if (classification.found_filtered_list) {
			refresh_type = RefreshType::FULL_REFRESH;
		} else if (classification.found_count_distinct && !aggregate_columns.empty()) {
			refresh_type = RefreshType::GROUP_RECOMPUTE;
		} else if (classification.found_distinct && !distinct_at_top && classification.found_aggregation) {
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
				refresh_type = RefreshType::DISTINCT_INCREMENTAL;
			} else {
				refresh_type = RefreshType::GROUP_RECOMPUTE;
			}
			// Try to extract the DISTINCT subquery from the user's original SQL. If the
			// shape isn't recognised (multi-source body, subquery FROM, etc.), demote to
			// GROUP_RECOMPUTE — the aux pipeline is single-source-only in v0.
			if (refresh_type == RefreshType::DISTINCT_INCREMENTAL) {
				vector<string> dcols;
				string d_input_sql, d_source, d_filter;
				if (!ExtractInnerDistinct(original_view_query, dcols, d_input_sql, d_source, d_filter)) {
					refresh_type = RefreshType::GROUP_RECOMPUTE;
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
			if (refresh_type == RefreshType::DISTINCT_INCREMENTAL) {
				LogicalAggregate *outer_agg = FindOuterAggregate(plan.get());
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
					refresh_type = RefreshType::GROUP_RECOMPUTE;
					distinct_sum_arg.clear();
					distinct_sum_out.clear();
					OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL outer-agg not single-SUM — demoting "
					                    "to GROUP_RECOMPUTE\n");
				} else if (distinct_sum_out.empty()) {
					// `SUM(c) AS s` puts the alias `s` on the SELECT-list BCR above the
					// aggregate, not on the BoundAggregateExpression itself. Recover it
					// from output_names: the SUM output column is the first non-group
					// position (group cols come first in the data table layout).
					if (aggregate_columns.size() < output_names.size()) {
						distinct_sum_out = output_names[aggregate_columns.size()];
					}
				}
			}
		} else if (union_distinct_over_agg && !aggregate_columns.empty()) {
			// UNION DISTINCT over grouped aggregate outputs must recompute by the branch
			// aggregate key, not by the full distinct output tuple. The old aggregate value
			// is no longer discoverable from delta-filtered recompute after an update.
			refresh_type = RefreshType::GROUP_RECOMPUTE;
		} else if (classification.found_distinct && distinct_at_top && !aggregate_columns.empty()) {
			refresh_type = RefreshType::AGGREGATE_GROUP;
		} else if (classification.found_having && classification.found_aggregation && !aggregate_columns.empty()) {
			refresh_type = RefreshType::AGGREGATE_HAVING;
		} else if ((has_union_over_agg || join_key_group_fallback || delim_aggregate_group_fallback ||
		            scalar_delim_projection_group_fallback || join_aggregate_projection_fallback ||
		            nested_aggregate_group_fallback || repeated_cte_aggregate_group_fallback ||
		            classification.found_nested_aggregate) &&
		           !aggregate_columns.empty()) {
			// UNION/UNION ALL over aggregates: group keys extracted positionally from output_names.
			// Inner-aggregate-in-join: group keys are the join-condition columns visible in the
			// top projection. DELIM/DEPENDENT aggregate or scalar-subquery projection: group
			// keys are the visible correlated output columns. Nested aggregate (outer COUNT(*)
			// over inner GROUP BY via CTE): outer COUNT counts inner groups, not source rows,
			// so linear delta is incorrect. These use GROUP_RECOMPUTE — not AGGREGATE_GROUP —
			// because non-key output columns may be pass-through attributes or non-linear
			// functions over inner/correlated expressions.
			refresh_type = RefreshType::GROUP_RECOMPUTE;
		} else if (classification.found_aggregation && !aggregate_columns.empty()) {
			refresh_type = RefreshType::AGGREGATE_GROUP;
		} else if (classification.found_aggregation && aggregate_columns.empty()) {
			refresh_type = RefreshType::SIMPLE_AGGREGATE;
		} else if (classification.found_projection && !classification.found_aggregation) {
			refresh_type = RefreshType::SIMPLE_PROJECTION;
		} else {
			refresh_type = RefreshType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' has an unrecognized query pattern. Full refresh will be used.");
		}

		bool ducklake_window_partition = refresh_type == RefreshType::WINDOW_PARTITION &&
		                                 (target_is_ducklake || !dl_table_info_for_classification.empty());
		if (ducklake_window_partition && !lpts_fallback) {
			// Window-partition refresh recomputes affected partitions from the user's query,
			// so the initial data table does not need LPTS-normalized SQL. For DuckLake,
			// the normalized form can duplicate CTE/subplan work around global aggregates
			// and cross joins; executing that shape as CTAS from inside CREATE MATERIALIZED
			// VIEW can leave DuckLake's metadata commit waiting on its own transaction. Use
			// the original SQL here, matching the refresh path and ordinary CTAS behavior.
			view_query = original_view_query;
			lpts_fallback = true;
			OPENIVM_DEBUG_PRINT("[CREATE MV] DuckLake window MV uses original SQL for initial data table: %s\n",
			                    view_query.c_str());
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Detected IVM type: %s (aggregation=%d, projection=%d, group_cols=%zu)\n",
		                    RefreshTypeName(refresh_type), (int)classification.found_aggregation,
		                    (int)classification.found_projection, aggregate_columns.size());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Source tables:");
		for (const auto &t : table_names) {
			OPENIVM_DEBUG_PRINT(" %s", t.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n");

		// Build DDL vector directly in memory
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

		// --- System tables DDL ---
		add_profile_marker("create_mv_system_tables",
		                   "refresh_type=" + string(RefreshTypeName(refresh_type)) +
		                       "; lpts_fallback=" + string(lpts_fallback ? "true" : "false"));
		AppendCreateMVSystemTablesDDL(ddl, view_name, parse_data_ref.is_replace);

		// --- OR REPLACE: drop old MV if it exists ---
		if (parse_data_ref.is_replace) {
			add_profile_marker("create_mv_replace_cleanup");
			string qvn_drop = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
			string qdt_drop = internal_catalog_prefix +
			                  KeywordHelper::WriteOptionallyQuoted(IncrementalTableNames::DataTableName(view_name));
			string qdv_drop =
			    internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
			// Drop the user-facing VIEW, data table, and delta view table
			ddl.push_back("DROP VIEW IF EXISTS " + qvn_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdt_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdv_drop);
			// Clean metadata (the INSERT OR REPLACE below handles openivm_views)
			ddl.push_back("DELETE FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
			              SqlUtils::EscapeSingleQuotes(view_name) + "'");
			ddl.push_back("DELETE FROM " + string(openivm::HISTORY_TABLE) + " WHERE view_name = '" +
			              SqlUtils::EscapeSingleQuotes(view_name) + "'");
		}

		// Store the LPTS query in metadata — it has hidden columns (DISTINCT count, AVG sum/count,
		// LEFT JOIN key) and preserves user column names.
		string refresh_val = parse_data_ref.refresh_interval > 0 ? to_string(parse_data_ref.refresh_interval) : "null";
		// Store GROUP BY or PARTITION BY columns (mutually exclusive in our type system).
		// For WINDOW_PARTITION, store the PARTITION BY columns so the upsert compiler
		// can identify affected partitions from deltas.
		auto &cols_to_store = classification.found_window ? window_partition_columns : aggregate_columns;
		string group_cols_val = SqlCsvLiteralOrNull(cols_to_store);
		// Store per-column aggregate types for insert-only MIN/MAX optimization
		string agg_types_val = SqlCsvLiteralOrNull(aggregate_types);
		string having_val =
		    having_predicate.empty() ? "null" : "'" + SqlUtils::EscapeSingleQuotes(having_predicate) + "'";

		// Extract FULL OUTER JOIN condition: "left_table:left_col,right_table:right_col"
		string full_outer_join_cols_val = "null";
		if (classification.found_full_outer) {
			string foj_cols = ExtractFullOuterJoinMetadata(plan.get());
			if (!foj_cols.empty()) {
				full_outer_join_cols_val = "'" + SqlUtils::EscapeSingleQuotes(foj_cols) + "'";
			}
		}

		// 11 trailing NULLs: 8 matcher metadata columns plus aux/lineage metadata.
		// Matcher metadata is populated by the Stage I block below when
		// openivm_enable_view_matching=true. The aux/lineage columns are populated by
		// follow-up UPDATEs when the corresponding refresh strategy recognizes a shape.
		metadata_ddl.push_back("insert or replace into " + string(openivm::VIEWS_TABLE) + " values ('" + view_name +
		                       "', '" + SqlUtils::EscapeSingleQuotes(view_query) + "', " +
		                       to_string((int)refresh_type) + ", " + (classification.found_minmax ? "true" : "false") +
		                       ", " + (classification.found_left_join ? "true" : "false") + ", now(), " + refresh_val +
		                       ", false, " + group_cols_val + ", " + agg_types_val + ", " + having_val + ", " +
		                       (classification.found_full_outer ? "true" : "false") + ", " + full_outer_join_cols_val +
		                       ", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

		if (refresh_type == RefreshType::WINDOW_PARTITION) {
			string lineage_json = BuildWindowPartitionLineageJson(plan.get(), window_partition_columns);
			if (!lineage_json.empty()) {
				aux_metadata_ddl.push_back(
				    BuildUpdateViewJsonSQL("window_partition_lineage_json", lineage_json, view_name));
			}
		}

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
		if (refresh_type == RefreshType::DISTINCT_INCREMENTAL) {
			add_profile_marker("create_mv_distinct_aux");
			string aux_table = "openivm_distinct_count_" + view_name;
			string cols_csv = StringUtil::Join(distinct_extracted_cols, ", ");
			// CREATE+POPULATE the aux table from the extracted DISTINCT input SQL.
			// _count is a signed BIGINT (deltas can transiently push it negative during
			// concurrent refreshes; the post-update DELETE drops rows whose count <= 0).
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as select " + cols_csv +
			                    ", count(*)::BIGINT as _count from (" + distinct_extracted_input_sql + ") group by " +
			                    cols_csv;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));
			// Build a JSON metadata blob that the refresh-time compile reads back.
			string meta_json = "{\"aux_table\":" + SqlUtils::JsonQuote(aux_table) +
			                   ",\"cols\":" + SqlUtils::JsonArray(distinct_extracted_cols) +
			                   ",\"input_sql\":" + SqlUtils::JsonQuote(distinct_extracted_input_sql) +
			                   ",\"source\":" + SqlUtils::JsonQuote(distinct_extracted_source) +
			                   ",\"filter\":" + SqlUtils::JsonQuote(distinct_extracted_filter) +
			                   ",\"sum_arg\":" + SqlUtils::JsonQuote(distinct_sum_arg) +
			                   ",\"sum_out\":" + SqlUtils::JsonQuote(distinct_sum_out) + "}";
			aux_metadata_ddl.push_back(BuildUpdateViewJsonSQL("distinct_aux_meta_json", meta_json, view_name));
		}

		if (refresh_type == RefreshType::SIMPLE_AGGREGATE && has_filtered_group_count_aux) {
			add_profile_marker("create_mv_filtered_group_count_aux");
			string aux_table = "openivm_filtered_group_count_" + view_name;
			string source_table = QualifyCreateSourceTable(filtered_group_count_extract.source, current_catalog,
			                                               current_schema, default_db);
			string group_q = KeywordHelper::WriteOptionallyQuoted(filtered_group_count_extract.group_col);
			string sum_q = KeywordHelper::WriteOptionallyQuoted(filtered_group_count_extract.sum_col);
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as select " + group_q + ", sum(" +
			                    sum_q + ") as openivm_sum from " + source_table + " group by " + group_q;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));

			string meta_json =
			    "{\"kind\":\"filtered_group_count\",\"aux_table\":" + SqlUtils::JsonQuote(aux_table) + ",\"source\":" +
			    SqlUtils::JsonQuote(SqlUtils::LastIdentifierPart(filtered_group_count_extract.source)) +
			    ",\"group_col\":" + SqlUtils::JsonQuote(filtered_group_count_extract.group_col) +
			    ",\"sum_col\":" + SqlUtils::JsonQuote(filtered_group_count_extract.sum_col) +
			    ",\"output_col\":" + SqlUtils::JsonQuote(filtered_group_count_extract.output_col) +
			    ",\"op\":" + SqlUtils::JsonQuote(filtered_group_count_extract.comparison_op) +
			    ",\"threshold\":" + SqlUtils::JsonQuote(filtered_group_count_extract.threshold_sql) + "}";
			aux_metadata_ddl.push_back(BuildUpdateViewJsonSQL("aggregate_decomposition_json", meta_json, view_name));
		}

		if (refresh_type == RefreshType::SEMI_ANTI_RECOMPUTE) {
			add_profile_marker("create_mv_semi_anti_aux");
			string aux_table = "openivm_semi_anti_state_" + view_name;
			string left_source_table =
			    QualifyCreateSourceTable(semi_anti_extract.left_table, current_catalog, current_schema, default_db);
			string right_source_table =
			    QualifyCreateSourceTable(semi_anti_extract.right_table, current_catalog, current_schema, default_db);
			string left_cols_csv = SqlUtils::JoinQuotedColumns(semi_anti_left_cols);
			string left_source_select;
			string left_cols_qualified =
			    SqlUtils::JoinQualifiedQuotedColumns(semi_anti_left_cols, semi_anti_extract.left_alias);
			string left_cols_lc = SqlUtils::JoinQualifiedQuotedColumns(semi_anti_left_cols, "lc");
			string left_cols_mc = SqlUtils::JoinQualifiedQuotedColumns(semi_anti_left_cols, "mc");
			string lc_mc_match = SqlUtils::BuildNullSafeMatch(semi_anti_left_cols, "lc", "mc");
			vector<string> semi_anti_left_exprs;
			for (size_t i = 0; i < semi_anti_left_cols.size(); i++) {
				if (i > 0) {
					left_source_select += ", ";
				}
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
				left_source_select += source_expr + " AS " + qcol;
			}
			string left_source_filter;
			if (!semi_anti_extract.post_filter.empty()) {
				left_source_filter = " WHERE " + semi_anti_extract.post_filter;
			}
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as with left_source as (select " +
			                    left_source_select + " from " + left_source_table + " " + semi_anti_extract.left_alias +
			                    left_source_filter + "), left_counts as (select " + left_cols_csv +
			                    ", count(*)::BIGINT as _left_count from left_source group by " + left_cols_csv +
			                    "), match_counts as (select " + left_cols_qualified +
			                    ", count(*)::BIGINT as _match_count from (select distinct " + left_cols_csv +
			                    " from left_source) " + semi_anti_extract.left_alias + " join " + right_source_table +
			                    " " + semi_anti_extract.right_alias + " on " + semi_anti_extract.predicate +
			                    " group by " + left_cols_qualified + ") select " + left_cols_lc +
			                    ", lc._left_count, coalesce(mc._match_count, 0)::BIGINT as _match_count from "
			                    "left_counts lc left join match_counts mc on " +
			                    lc_mc_match;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));

			string meta_json = "{\"aux_table\":" + SqlUtils::JsonQuote(aux_table) +
			                   ",\"join_type\":" + SqlUtils::JsonQuote(semi_anti_extract.join_type) +
			                   ",\"left_table\":" + SqlUtils::JsonQuote(semi_anti_extract.left_table) +
			                   ",\"left_alias\":" + SqlUtils::JsonQuote(semi_anti_extract.left_alias) +
			                   ",\"right_table\":" + SqlUtils::JsonQuote(semi_anti_extract.right_table) +
			                   ",\"right_alias\":" + SqlUtils::JsonQuote(semi_anti_extract.right_alias) +
			                   ",\"predicate\":" + SqlUtils::JsonQuote(semi_anti_extract.predicate) +
			                   ",\"post_filter\":" + SqlUtils::JsonQuote(semi_anti_extract.post_filter) +
			                   ",\"left_cols\":" + SqlUtils::JsonArray(semi_anti_left_cols) +
			                   ",\"left_exprs\":" + SqlUtils::JsonArray(semi_anti_left_exprs) +
			                   ",\"output_cols\":" + SqlUtils::JsonArray(semi_anti_extract.output_cols) + "}";
			aux_metadata_ddl.push_back(BuildUpdateViewJsonSQL("semi_anti_aux_meta_json", meta_json, view_name));
		}

		// Classify each base table by catalog type (duckdb vs ducklake).
		// DuckLake tables use native change tracking; DuckDB tables use delta tables.
		//
		// Catalog::GetEntry inside BeginTransaction() cannot see DuckLake entries:
		// DuckLake requires its own transaction protocol. Walk the logical plan's
		// DUCKLAKE_SCAN nodes instead — same approach used in ducklake_join.cpp.
		unordered_map<string, DuckLakeSourceTableInfo> dl_table_info; // keyed by lowercased name
		CollectDuckLakeTables(plan.get(), current_catalog, dl_table_info);

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

		vector<string> source_metadata_ddl;
		unordered_set<string> inserted_meta_table_names;
		for (const auto &table_name : table_names) {
			string catalog_type = "duckdb";
			string snapshot_val = "null";
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
				source_catalog_val = it->second.catalog_name;
				source_schema_val = it->second.schema_name;
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

			source_metadata_ddl.push_back("insert or replace into " + string(openivm::DELTA_TABLES_TABLE) +
			                              " (view_name, table_name, last_update, catalog_type, last_snapshot_id, "
			                              "last_refresh_ts, source_catalog, source_schema) "
			                              "values ('" +
			                              view_name + "', '" + SqlUtils::EscapeSingleQuotes(meta_table_name) +
			                              "', now(), '" + catalog_type + "', " + snapshot_val + ", now(), '" +
			                              SqlUtils::EscapeSingleQuotes(source_catalog_val) + "', '" +
			                              SqlUtils::EscapeSingleQuotes(source_schema_val) + "')");
		}

		// --- Compiled DDL (MV creation, delta tables, delta view) ---
		// Physical data table stores all columns (including openivm_* internal cols).
		// DuckLake-targeted MVs store the data/delta tables in DuckLake and keep only
		// OpenIVM metadata in the physical default catalog. CREATE/REFRESH split metadata
		// writes from DuckLake data writes because DuckDB cannot commit one transaction
		// across both catalogs.
		if (BoolSetting(context, "openivm_explain_initial_load")) {
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
			diagnostic += "lpts_fallback: " + string(lpts_fallback ? "true" : "false") + "\n";
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
			if (BoolSetting(context, "openivm_explain_initial_load_only")) {
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
			if (!lpts_fallback) {
				for (auto &name : output_names) {
					if (IncrementalTableNames::IsInternalColumn(name)) {
						internal_cols.push_back(name);
					}
				}
			}
			string having_where = having_predicate.empty() ? "" : " where " + having_predicate;
			// For aggregate+top-k the VIEW appends ORDER BY … LIMIT k after the HAVING WHERE.
			// The data table stores ALL groups; the ORDER BY LIMIT is applied at read time.
			string view_tail = having_where + (top_k_suffix.empty() ? "" : " " + top_k_suffix);
			if (internal_cols.empty()) {
				ddl.push_back("create view " + qvn + " as select * from " + qdt + view_tail);
			} else {
				ddl.push_back("create view " + qvn + " as select * exclude (" +
				              SqlUtils::JoinQuotedColumns(internal_cols) + ") from " + qdt + view_tail);
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
				auto entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA,
				                                                  table_name, OnEntryNotFound::RETURN_NULL);
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
			              " as select *, 1::INTEGER as " + string(openivm::MULTIPLICITY_COL) +
			              ", now()::timestamp as " + string(openivm::TIMESTAMP_COL) + " from " + catalog_schema +
			              KeywordHelper::WriteOptionallyQuoted(table_name) + " limit 0");
		}

		// Delta table for the MV — based on the DATA table (has all columns)
		add_profile_marker("create_mv_mv_delta_table");
		string qdv = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name));
		ddl.push_back("create table if not exists " + qdv + " as select *, 1::INTEGER as " +
		              string(openivm::MULTIPLICITY_COL) + ", now()::timestamp as " + string(openivm::TIMESTAMP_COL) +
		              " from " + qdt + " limit 0");
		ddl.push_back("alter table " + qdv + " alter " + string(openivm::TIMESTAMP_COL) + " set default now()");
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
		for (const auto &table_name : table_names) {
			string table_lc = StringUtil::Lower(table_name);
			if (ducklake_tables.count(table_name) || ducklake_tables.count(table_lc)) {
				// Use the DuckLake catalog for current_snapshot() — NOT view_catalog_prefix
				// or current_catalog. For cross-system MVs (native MV reading from dl.*),
				// view_catalog_prefix is empty and current_catalog is the physical-default
				// (e.g. the file DB) which doesn't have `current_snapshot()`.
				// table_names entries may be lowercased by the parser; the metadata row was
				// inserted above with the case-preserved DuckLake name (`dl_table_info`).
				string table_lc_for_lookup = StringUtil::Lower(table_name);
				auto info_it = dl_table_info.find(table_lc_for_lookup);
				if (info_it == dl_table_info.end() || info_it->second.catalog_name.empty()) {
					throw CatalogException("Could not resolve DuckLake catalog for source table '" + table_name +
					                       "' while creating materialized view '" + view_name + "'");
				}
				string meta_table_name = info_it->second.table_name;
				string dl_cat_name = info_it->second.catalog_name;
				ddl.push_back("UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
				              " SET last_snapshot_id = (SELECT id FROM " + dl_cat_name +
				              ".current_snapshot()) WHERE view_name = '" + SqlUtils::EscapeSingleQuotes(view_name) +
				              "' AND table_name = '" + SqlUtils::EscapeSingleQuotes(meta_table_name) + "'");
			}
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
	}

	// Return DDL executor table function
	ConfigureDDLExecutorResult(result);
	return result;
}

BoundStatement DDLExecutorBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info,
                               SQLStatement &statement) {
	return BoundStatement();
}
} // namespace duckdb
