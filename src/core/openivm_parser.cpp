#include "core/openivm_parser.hpp"

#include "core/ivm_checker.hpp"
#include "core/ivm_plan_rewrite.hpp"
#include "core/openivm_constants.hpp"
#include "lpts_pipeline.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"

#include "core/openivm_debug.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"

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
			OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Executing DDL: %s\n", q.c_str());
			auto r = conn.Query(q);
			if (r->HasError()) {
				// The unique index on the MV data table is an upsert-time optimization
				// (helps MERGE find the matching row quickly). If the classifier's
				// group_columns don't actually form a unique key for the MV (e.g. a
				// CTE with GROUP BY joined against another table — the outer JOIN
				// duplicates rows), CREATE UNIQUE INDEX fails with "Data contains
				// duplicates". Skip just that statement and continue; the refresh
				// still works via key-based MERGE, just a bit slower.
				bool is_unique_index = StringUtil::Contains(StringUtil::Lower(q), "create unique index") &&
				                       StringUtil::Contains(r->GetError(), "Data contains duplicates");
				if (is_unique_index) {
					Printer::Print("Warning: could not create unique index for MV — group_columns "
					               "are not unique in MV output. Refresh will still work (no index).");
					continue;
				}
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

		// Capture the current catalog/schema from the originating context. IVMDDLBindFunction
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
		{
			auto res = con.Query("SELECT current_database()");
			if (!res->HasError() && res->RowCount() > 0) {
				default_db = res->GetValue(0, 0).ToString();
			}
		}

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
			// When the MV name is unqualified but the session is in a non-default catalog
			// (e.g. USE dl.main), explicitly qualify so data/view tables land in dl rather
			// than the physical default. Metadata tables (unqualified) stay in the physical
			// default — PRAGMA ivm() always uses a fresh connection without USE.
			if (!current_catalog.empty() && current_catalog != default_db) {
				view_catalog_prefix = current_catalog + "." + current_schema + ".";
			}
		}
		string view_query = original_view_query; // will be overwritten by LPTS for DDL

		// Apply the session's active catalog to `con` so unqualified table references in the
		// MV query resolve in the user's catalog (e.g. `dl.main`) rather than the physical
		// default. Without this, `CREATE MATERIALIZED VIEW mv AS SELECT * FROM WAREHOUSE`
		// issued under `USE dl.main` fails during planning with "Table WAREHOUSE does not
		// exist" because the fresh connection resolves against the physical-default catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			con.Query("USE " + current_catalog + "." + current_schema);
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
		{
			std::function<bool(LogicalOperator *)> has_cte = [&](LogicalOperator *op) {
				if (!op) {
					return false;
				}
				if (op->type == LogicalOperatorType::LOGICAL_CTE_REF ||
				    op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
					return true;
				}
				for (auto &c : op->children) {
					if (has_cte(c.get())) {
						return true;
					}
				}
				return false;
			};
			if (has_cte(plan.get())) {
				std::function<void(LogicalOperator *)> relax_cte = [&](LogicalOperator *op) {
					if (!op) {
						return;
					}
					if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
						auto &cte = op->Cast<LogicalMaterializedCTE>();
						if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
							cte.materialize = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
						}
					}
					for (auto &c : op->children) {
						relax_cte(c.get());
					}
				};
				relax_cte(plan.get());
				Optimizer outer_cte_opt(*planner.binder, context);
				CTEInlining outer_cte_inlining(outer_cte_opt);
				plan = outer_cte_inlining.Optimize(std::move(plan));
			}
		}

		// Plan the raw SELECT query separately for IVM plan rewrite + LPTS conversion
		vector<string> output_names;
		string having_predicate;    // HAVING predicate as SQL (for VIEW WHERE clause, empty if no HAVING)
		bool lpts_fallback = false; // set when LPTS can't serialize the plan and we fall back to SQL
		{
			Parser select_parser;
			select_parser.ParseQuery(original_view_query);
			Planner select_planner(*con.context);
			select_planner.CreatePlan(std::move(select_parser.statements[0]));
			auto select_plan = std::move(select_planner.plan);

			// Inline CTEs so LPTS (which doesn't implement LOGICAL_CTE_REF) sees a flat
			// plan. Query-bound CTEs become LOGICAL_MATERIALIZED_CTE + LOGICAL_CTE_REF
			// nodes in the bound plan; CTEInlining rewrites small/non-recursive ones
			// into direct subqueries. Running the full DuckDB optimizer here would
			// also reorder joins / push filters, which conflicts with IVM's subsequent
			// plan rewrites — so we run only the CTE-inlining pass, and only when a
			// CTE reference actually appears in the plan (the optimizer isn't always
			// a no-op on CTE-free plans — e.g. it can rewrite DISTINCT subqueries in
			// ways that confuse the downstream structural rewrites).
			{
				std::function<bool(LogicalOperator *)> has_cte = [&](LogicalOperator *op) {
					if (!op) {
						return false;
					}
					if (op->type == LogicalOperatorType::LOGICAL_CTE_REF ||
					    op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
						return true;
					}
					for (auto &c : op->children) {
						if (has_cte(c.get())) {
							return true;
						}
					}
					return false;
				};
				if (has_cte(select_plan.get())) {
					// DuckDB's binder sets LogicalMaterializedCTE::materialize to
					// CTE_MATERIALIZE_ALWAYS by default. CTEInlining bails early on ALWAYS
					// and leaves the CTE as a materialized node. IVM can't maintain views
					// whose plan still contains LOGICAL_CTE_REF — LPTS has no serializer
					// for it and the refresh path has no delta-consolidation rule. Relax
					// every CTE to CTE_MATERIALIZE_DEFAULT before inlining so CTEInlining
					// folds them into the outer plan. Single-ref CTEs always inline; multi-
					// ref CTEs inline when they're cheap and don't end in an aggregate that
					// would be wastefully re-materialized.
					std::function<void(LogicalOperator *)> relax_cte = [&](LogicalOperator *op) {
						if (!op) {
							return;
						}
						if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
							auto &cte = op->Cast<LogicalMaterializedCTE>();
							if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
								cte.materialize = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
							}
						}
						for (auto &c : op->children) {
							relax_cte(c.get());
						}
					};
					relax_cte(select_plan.get());
					Optimizer cte_opt(*select_planner.binder, context);
					CTEInlining cte_inlining(cte_opt);
					select_plan = cte_inlining.Optimize(std::move(select_plan));
				}
			}

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
			// Append names for these from the top-most node's expression aliases. Walk through
			// ORDER BY / LIMIT / DISTINCT / FILTER wrappers; accept either a PROJECTION or an
			// AGGREGATE at the top. For AGGREGATE the group-count positions reuse existing
			// output_names (which match the original SELECT list); aggregate-expression positions
			// after that use each aggregate's alias (e.g. `_ivm_distinct_count`).
			auto plan_bindings = select_plan->GetColumnBindings();
			LogicalProjection *top_proj = nullptr;
			LogicalAggregate *top_agg = nullptr;
			for (LogicalOperator *walk = select_plan.get(); walk;) {
				if (walk->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					top_proj = &walk->Cast<LogicalProjection>();
					break;
				}
				if (walk->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
					top_agg = &walk->Cast<LogicalAggregate>();
					break;
				}
				if ((walk->type == LogicalOperatorType::LOGICAL_ORDER_BY ||
				     walk->type == LogicalOperatorType::LOGICAL_LIMIT ||
				     walk->type == LogicalOperatorType::LOGICAL_DISTINCT ||
				     walk->type == LogicalOperatorType::LOGICAL_FILTER) &&
				    !walk->children.empty()) {
					walk = walk->children[0].get();
					continue;
				}
				break;
			}
			if (top_proj) {
				while (output_names.size() < plan_bindings.size()) {
					idx_t idx = output_names.size();
					if (idx < top_proj->expressions.size() && !top_proj->expressions[idx]->alias.empty()) {
						output_names.push_back(top_proj->expressions[idx]->alias);
					} else {
						output_names.push_back("_ivm_col_" + to_string(idx));
					}
				}
			} else if (top_agg) {
				idx_t group_count_local = top_agg->groups.size();
				while (output_names.size() < plan_bindings.size()) {
					idx_t idx = output_names.size();
					if (idx >= group_count_local) {
						idx_t expr_idx = idx - group_count_local;
						if (expr_idx < top_agg->expressions.size() && !top_agg->expressions[expr_idx]->alias.empty()) {
							output_names.push_back(top_agg->expressions[expr_idx]->alias);
							continue;
						}
					}
					output_names.push_back("_ivm_col_" + to_string(idx));
				}
			}
			// Deduplicate output names — `SELECT W_ID, W_ID FROM …` or `SELECT col, COUNT(*), col`
			// produce duplicate names that break `CREATE TABLE _ivm_data_mv AS <view_query>`
			// (DuckDB rejects duplicate column names in a CREATE TABLE column list). Internal
			// IVM columns (e.g. `_ivm_left_key`) must keep their canonical name — rewrite rules
			// look them up by exact string.
			{
				unordered_set<string> seen;
				for (auto &name : output_names) {
					if (IVMTableNames::IsInternalColumn(name)) {
						continue;
					}
					string candidate = name;
					idx_t suffix = 1;
					while (seen.count(candidate)) {
						candidate = name + "_" + to_string(suffix++);
					}
					seen.insert(candidate);
					name = candidate;
				}
			}
			// Strip HAVING filter from plan — data table stores all groups.
			// The predicate is extracted as SQL (using output aliases) for the VIEW WHERE clause.
			having_predicate = StripHavingFilter(select_plan, output_names);

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
			// For views that LPTS silently mis-serializes (GROUPING SETS / ROLLUP / CUBE
			// → plain GROUP BY; STRUCT_PACK field names → tN_col aliases; etc.), detect
			// structurally and prefer the original SQL. Those constructs never need the
			// LPTS-rewritten form anyway — they're classified FULL_REFRESH and the
			// rewriter-rule path (which needs LPTS) isn't used.
			{
				bool needs_original = false;
				// Walk the plan looking for constructs LPTS can't round-trip:
				//   - GROUPING SETS / ROLLUP / CUBE (aggregate.grouping_sets.size() > 1)
				//   - Ordered-set aggregates (quantile_cont / quantile_disc / percentile_cont
				//     / percentile_disc / approx_quantile) — their extra "within group" /
				//     second-argument parameters don't survive the plan→SQL conversion.
				std::function<void(LogicalOperator *)> walk = [&](LogicalOperator *op) {
					if (needs_original || !op) {
						return;
					}
					if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
						auto *agg = dynamic_cast<LogicalAggregate *>(op);
						if (agg) {
							if (agg->grouping_sets.size() > 1) {
								needs_original = true;
								return;
							}
							for (auto &expr : agg->expressions) {
								if (expr->type != ExpressionType::BOUND_AGGREGATE) {
									continue;
								}
								auto &bound_agg = expr->Cast<BoundAggregateExpression>();
								const string &fn_name = bound_agg.function.name;
								if (fn_name == "quantile_cont" || fn_name == "quantile_disc" ||
								    fn_name == "percentile_cont" || fn_name == "percentile_disc" ||
								    fn_name == "approx_quantile" || fn_name == "mad" || fn_name == "median" ||
								    fn_name == "mode") {
									needs_original = true;
									return;
								}
							}
						}
					}
					for (auto &child : op->children) {
						walk(child.get());
					}
				};
				walk(select_plan.get());
				if (needs_original) {
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

		// Single-pass plan analysis: validates IVM compatibility AND extracts metadata
		auto analysis = AnalyzePlan(plan.get());
		bool ivm_compatible = analysis.ivm_compatible;
		bool found_aggregation = analysis.found_aggregation;
		bool found_projection = analysis.found_projection;
		bool found_distinct = analysis.found_distinct;
		bool found_having = analysis.found_having;
		bool found_minmax = analysis.found_minmax;
		bool found_left_join = analysis.found_left_join;
		bool found_full_outer = analysis.found_full_outer;
		bool found_window = analysis.found_window;
		bool found_join = analysis.found_join;
		// Derive GROUP BY column names by walking the plan's projection above the aggregate.
		// The projection maps GROUP BY bindings (group_index, i) to SELECT-list aliases — these
		// aliases are the actual column names in the data table. Plan-walk aliases on agg.groups
		// are unreliable for CASE/COALESCE expressions.
		// For DISTINCT views, the checker already extracts aggregate_columns from distinct_targets.
		size_t group_count = analysis.group_count;
		idx_t group_index = analysis.group_index;
		vector<string> aggregate_columns;
		// DISTINCT is only the MV's grouping when its targets actually appear in the MV's
		// output columns. An inner-subquery DISTINCT (e.g. `SELECT COUNT(*) FROM (SELECT
		// DISTINCT x FROM t)`, or a CTE like `WITH cte AS (SELECT DISTINCT x FROM t) ...`)
		// exposes columns that never reach the data table, so its targets can't drive
		// AGGREGATE_GROUP classification or the unique index.
		bool distinct_at_top = false;
		if (analysis.found_distinct && !analysis.aggregate_columns.empty() && !output_names.empty()) {
			unordered_set<string> output_lc;
			for (auto &n : output_names) {
				string lc = n;
				std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
				output_lc.insert(lc);
			}
			distinct_at_top = true;
			for (auto &t : analysis.aggregate_columns) {
				string lc = t;
				std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
				if (!output_lc.count(lc)) {
					distinct_at_top = false;
					break;
				}
			}
		}
		if (distinct_at_top) {
			aggregate_columns = std::move(analysis.aggregate_columns);
		} else if (analysis.found_distinct && analysis.aggregate_columns.empty()) {
			// Plain DISTINCT (no explicit targets) — trust the checker.
			aggregate_columns = std::move(analysis.aggregate_columns);
		} else if (group_count > 0 && group_index != DConstants::INVALID_INDEX) {
			// Walk plan to find the outermost PROJECTION that references (group_index, i) bindings.
			// Stopping at the first PROJECTION found (rather than recursing deeper) ensures we
			// only match the top-level aggregate — aggregates nested inside UNION branches or
			// CTE bodies are correctly ignored.
			// For HAVING the structure is PROJECTION→FILTER→AGGREGATE: the top PROJECTION still
			// holds group_index refs, so this works correctly.
			// For CTE nodes, only the outer-query child (children[1]) is visited, mirroring
			// the same restriction applied in AnalyzeNode.
			// Ordered list of projection expressions that reference (group_index, *).
			// Not keyed by column_index because a column can be referenced multiple times
			// (e.g. `SELECT w_tax, w_tax, COUNT(*) GROUP BY w_tax, w_tax` — DuckDB
			// collapses GROUP BY w_tax, w_tax to a single group, but the projection
			// still outputs the column twice; both positions materialize in the data
			// table and the MERGE needs both as group keys).
			vector<string> group_names_list;
			// Build an index of every LOGICAL_PROJECTION in the plan, keyed by its
			// table_index. This lets a BCR at any level be traced through a chain of
			// pass-through projections (e.g. DecomposeAvgStddev + CTE inlining can
			// stack 2-3 PROJECTIONs between the top SELECT and the AGGREGATE).
			std::unordered_map<idx_t, LogicalProjection *> projections_by_index;
			std::function<void(LogicalOperator *)> collect_projections = [&](LogicalOperator *op) {
				if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &proj = op->Cast<LogicalProjection>();
					projections_by_index[proj.table_index] = &proj;
				}
				for (auto &child : op->children) {
					collect_projections(child.get());
				}
			};
			collect_projections(plan.get());
			// Resolve (tidx, cidx) through BCR pass-throughs in any projection chain.
			// Returns true iff the binding ultimately refers to a raw group column.
			std::function<bool(idx_t, idx_t, int)> resolves_to_group = [&](idx_t tidx, idx_t cidx, int depth) -> bool {
				if (depth > 16) {
					return false;
				}
				if (tidx == group_index) {
					return cidx < (idx_t)group_count;
				}
				auto it = projections_by_index.find(tidx);
				if (it == projections_by_index.end()) {
					return false;
				}
				auto &proj = *it->second;
				if (cidx >= proj.expressions.size()) {
					return false;
				}
				auto &expr = proj.expressions[cidx];
				if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
					return false;
				}
				auto &bcr = expr->Cast<BoundColumnRefExpression>();
				return resolves_to_group(bcr.binding.table_index, bcr.binding.column_index, depth + 1);
			};
			std::function<bool(LogicalOperator *)> find_group_cols = [&](LogicalOperator *op) -> bool {
				if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					// Stop at the first PROJECTION. Return `matched` (true iff any expression
					// references group_index) so the MATERIALIZED_CTE case below can tell
					// "outer matched" from "outer missed — try body". For non-CTE plans a
					// top-projection miss ends the search; we must NOT descend into JOIN
					// branches where the aggregate's group key is a subquery-internal column
					// that isn't in the MV's user-facing output — that would yield a unique
					// index on a column absent from the data table.
					auto &proj = op->Cast<LogicalProjection>();
					bool matched = false;
					for (auto &expr : proj.expressions) {
						if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
							auto &bcr = expr->Cast<BoundColumnRefExpression>();
							// Trace through any stack of pass-through projections — the
							// top-level SELECT may reference the group column indirectly
							// through 1-N intermediate projections (CTE inlining, AVG
							// decomposition, etc.).
							if (resolves_to_group(bcr.binding.table_index, bcr.binding.column_index, 0)) {
								string col_name = expr->alias.empty() ? bcr.GetName() : expr->alias;
								if (!IVMTableNames::IsInternalColumn(col_name)) {
									group_names_list.push_back(col_name);
									matched = true;
								}
							}
						}
					}
					return matched;
				}
				// UNION mixes rows from independent aggregates — the key may repeat across
				// branches. Refuse to populate aggregate_columns from either branch so the
				// view drops down to SIMPLE_AGGREGATE and no unique index is installed.
				if (op->type == LogicalOperatorType::LOGICAL_UNION) {
					return false;
				}
				if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
					// Outer first (handles outer re-aggregate / reshape); if that misses,
					// dive into the CTE body (handles pass-through `SELECT * FROM cte`).
					if (op->children.size() >= 2) {
						if (find_group_cols(op->children[1].get())) {
							return true;
						}
						return find_group_cols(op->children[0].get());
					}
					return false;
				}
				for (auto &child : op->children) {
					if (find_group_cols(child.get())) {
						return true;
					}
				}
				return false;
			};
			// If the plan contains a LOGICAL_UNION above the aggregate(s), the MV's output
			// columns come from multiple independent branches — collecting group keys from
			// a single branch would return names that aren't in the MV's user-facing output
			// (UNION ALL re-aliases them at the top). Classify as SIMPLE_AGGREGATE so the
			// refresh path doesn't generate SQL referencing the inner alias.
			// `has_union` is a simple "contains LOGICAL_UNION anywhere" check: we already
			// gate on `found_aggregation`, so ANY union under the CTAS signals UNION-over-aggregate
			// when the aggregate is reachable via a CTE_REF branch.
			std::function<bool(LogicalOperator *)> has_union = [&](LogicalOperator *op) -> bool {
				if (!op) {
					return false;
				}
				if (op->type == LogicalOperatorType::LOGICAL_UNION) {
					return true;
				}
				for (auto &c : op->children) {
					if (has_union(c.get())) {
						return true;
					}
				}
				return false;
			};
			if (has_union(plan.get())) {
				group_names_list.clear();
				// UNION ALL of per-branch aggregates (e.g.
				//   `SELECT a, SUM(x) FROM t1 GROUP BY a
				//    UNION ALL SELECT a, SUM(x) FROM t2 GROUP BY a`)
				// produces output rows keyed by both (a, branch). SIMPLE_AGGREGATE would
				// treat all rows as one aggregate group (no GROUP BY) and SUM them into
				// a single row — wrong. Incremental per-branch maintenance would need a
				// new AGGREGATE_UNION IVMType with branch tagging. Until that lands, mark
				// the view as FULL_REFRESH so results stay correct.
				ivm_compatible = false;
			} else {
				find_group_cols(plan.get());
			}
			for (auto &name : group_names_list) {
				aggregate_columns.push_back(name);
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
				if (IVMTableNames::IsInternalColumn(name)) {
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

		// Window over join: partition columns may come from a joined table whose delta
		// doesn't have that column. We can't resolve joins at refresh time without LPTS
		// support for WINDOW. Fall back to full recompute for window+join views.
		// Single-table window views keep partition-level recompute.
		if (found_window && found_join) {
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
		if ((found_left_join || found_full_outer) && found_aggregation) {
			bool needs_recompute = false;
			std::function<bool(const Expression *)> is_pure_pass_through = [&](const Expression *expr) -> bool {
				// A pure pass-through is a BCR or a cast of a BCR. Anything else
				// (functions, CASE, COALESCE, arithmetic, constants) is "computed".
				if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
					return true;
				}
				if (expr->expression_class == ExpressionClass::BOUND_CAST) {
					auto &cast = expr->Cast<BoundCastExpression>();
					return is_pure_pass_through(cast.child.get());
				}
				return false;
			};
			std::function<void(LogicalOperator *)> walk = [&](LogicalOperator *op) {
				if (needs_recompute) {
					return;
				}
				if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
					auto &agg = op->Cast<LogicalAggregate>();
					for (auto &expr : agg.expressions) {
						if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
							continue;
						}
						auto &bound = expr->Cast<BoundAggregateExpression>();
						// count_star() has no children — trivially safe.
						for (auto &child : bound.children) {
							if (!is_pure_pass_through(child.get())) {
								needs_recompute = true;
								return;
							}
						}
					}
				}
				if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &proj = op->Cast<LogicalProjection>();
					for (auto &expr : proj.expressions) {
						// A projection expression over an aggregate output is
						// "computed" if it's not a pure BCR. If its BCR refers to
						// the AGGREGATE's table_index, it's a pass-through; any
						// other structure (COALESCE/CASE/function over BCR) is a
						// wrapper that breaks MERGE semantics.
						if (!is_pure_pass_through(expr.get())) {
							// Only flag if this expression references an
							// aggregate output (not just a group column).
							bool refs_agg = false;
							std::function<void(const Expression *)> check = [&](const Expression *e) {
								if (refs_agg) {
									return;
								}
								if (e->type == ExpressionType::BOUND_COLUMN_REF) {
									auto &bcr = e->Cast<BoundColumnRefExpression>();
									// Find the AGGREGATE's binding index to compare.
									// Conservatively flag any non-group BCR as aggregate ref.
									if (bcr.binding.table_index != analysis.group_index) {
										refs_agg = true;
									}
									return;
								}
								ExpressionIterator::EnumerateChildren(
								    const_cast<Expression &>(*e), [&](unique_ptr<Expression> &c) { check(c.get()); });
							};
							check(expr.get());
							if (refs_agg) {
								needs_recompute = true;
								return;
							}
						}
					}
				}
				for (auto &child : op->children) {
					walk(child.get());
				}
			};
			walk(plan.get());
			if (needs_recompute) {
				OPENIVM_DEBUG_PRINT(
				    "[CREATE MV] LEFT/OUTER JOIN aggregate with computed aggregate or projection wrapper — "
				    "using group-recompute (found_minmax=true)\n");
				found_minmax = true;
			}
		}

		IVMType ivm_type;

		if (found_window) {
			// Window functions use partition-level recompute (not full IVM, but better than full refresh)
			ivm_type = IVMType::WINDOW_PARTITION;
		} else if (!ivm_compatible) {
			ivm_type = IVMType::FULL_REFRESH;
			Printer::Print("Warning: materialized view '" + view_name +
			               "' uses constructs not supported for incremental maintenance. "
			               "Full refresh will be used.");
		} else if (found_distinct && distinct_at_top && !aggregate_columns.empty()) {
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
		                    : ivm_type == IVMType::WINDOW_PARTITION  ? "WINDOW_PARTITION"
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
		              " having_predicate varchar default null,"
		              " has_full_outer boolean default false,"
		              " full_outer_join_cols varchar default null)");

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
		// Store GROUP BY or PARTITION BY columns (mutually exclusive in our type system).
		// For WINDOW_PARTITION, store the PARTITION BY columns so the upsert compiler
		// can identify affected partitions from deltas.
		string group_cols_val = "null";
		auto &cols_to_store = found_window ? window_partition_columns : aggregate_columns;
		if (!cols_to_store.empty()) {
			group_cols_val = "'";
			for (size_t i = 0; i < cols_to_store.size(); i++) {
				if (i > 0) {
					group_cols_val += ",";
				}
				group_cols_val += OpenIVMUtils::EscapeSingleQuotes(cols_to_store[i]);
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

		// Extract FULL OUTER JOIN condition: "left_table:left_col,right_table:right_col"
		string full_outer_join_cols_val = "null";
		if (found_full_outer) {
			// Helper: find the table name from a LogicalGet in a subtree
			std::function<string(LogicalOperator *)> find_table_name = [&](LogicalOperator *n) -> string {
				if (n->type == LogicalOperatorType::LOGICAL_GET) {
					auto *get = dynamic_cast<LogicalGet *>(n);
					if (get && get->GetTable().get()) {
						return get->GetTable().get()->name;
					}
				}
				for (auto &child : n->children) {
					string name = find_table_name(child.get());
					if (!name.empty()) {
						return name;
					}
				}
				return "";
			};

			// Index projections and gets by table_index so we can resolve a BCR's binding
			// down the tree. CTE inlining stacks projections between the join and the
			// underlying scan; `BoundColumnRefExpression::GetName()` at the join level
			// returns whatever alias was applied by the nearest projection (e.g. `w`
			// from `SELECT O_W_ID AS w`), not the base column name. The refresh paths
			// need the base name (`O_W_ID`) because they query the delta/base table.
			std::unordered_map<idx_t, LogicalProjection *> foj_proj_by_index;
			std::unordered_map<idx_t, LogicalGet *> foj_get_by_index;
			std::function<void(LogicalOperator *)> foj_index_ops = [&](LogicalOperator *op) {
				if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &proj = op->Cast<LogicalProjection>();
					foj_proj_by_index[proj.table_index] = &proj;
				} else if (op->type == LogicalOperatorType::LOGICAL_GET) {
					auto &get = op->Cast<LogicalGet>();
					foj_get_by_index[get.table_index] = &get;
				}
				for (auto &child : op->children) {
					foj_index_ops(child.get());
				}
			};
			foj_index_ops(plan.get());
			auto resolve_bcr_to_base = [&](BoundColumnRefExpression *bcr) -> string {
				idx_t tidx = bcr->binding.table_index;
				idx_t cidx = bcr->binding.column_index;
				for (int depth = 0; depth < 16; depth++) {
					auto get_it = foj_get_by_index.find(tidx);
					if (get_it != foj_get_by_index.end()) {
						auto *get = get_it->second;
						// column_ids[cidx] maps the scan's cidx-th output to the base column
						// index; fall back to `names[cidx]` when column_ids is unset.
						auto &ids = get->GetColumnIds();
						if (cidx < ids.size() && get->GetTable().get()) {
							auto base_idx = ids[cidx].GetPrimaryIndex();
							auto &cols = get->GetTable().get()->GetColumns();
							if (base_idx < cols.LogicalColumnCount()) {
								return cols.GetColumn(LogicalIndex(base_idx)).Name();
							}
						}
						if (cidx < get->names.size()) {
							return get->names[cidx];
						}
						return "";
					}
					auto proj_it = foj_proj_by_index.find(tidx);
					if (proj_it == foj_proj_by_index.end()) {
						return "";
					}
					auto *proj = proj_it->second;
					if (cidx >= proj->expressions.size()) {
						return "";
					}
					auto &expr = proj->expressions[cidx];
					if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
						// Computed expression — fall back to its alias.
						return expr->alias.empty() ? expr->GetName() : expr->alias;
					}
					auto &next = expr->Cast<BoundColumnRefExpression>();
					tidx = next.binding.table_index;
					cidx = next.binding.column_index;
				}
				return "";
			};

			std::function<void(LogicalOperator *)> extract_foj_cols = [&](LogicalOperator *n) {
				if (full_outer_join_cols_val != "null") {
					return;
				}
				if (n->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
					auto *join = dynamic_cast<LogicalComparisonJoin *>(n);
					if (join && join->join_type == JoinType::OUTER && !join->conditions.empty()) {
						auto &left_expr = join->conditions[0].left;
						auto &right_expr = join->conditions[0].right;
						string left_col_name, right_col_name;
						if (left_expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
							auto *lref = dynamic_cast<BoundColumnRefExpression *>(left_expr.get());
							if (lref) {
								left_col_name = resolve_bcr_to_base(lref);
								if (left_col_name.empty()) {
									left_col_name = lref->GetName();
								}
							}
						}
						if (right_expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
							auto *rref = dynamic_cast<BoundColumnRefExpression *>(right_expr.get());
							if (rref) {
								right_col_name = resolve_bcr_to_base(rref);
								if (right_col_name.empty()) {
									right_col_name = rref->GetName();
								}
							}
						}
						// Get table names from each side of the join
						string left_table = !join->children.empty() ? find_table_name(join->children[0].get()) : "";
						string right_table = join->children.size() > 1 ? find_table_name(join->children[1].get()) : "";
						if (!left_col_name.empty() && !right_col_name.empty() && !left_table.empty() &&
						    !right_table.empty()) {
							string val = left_table + ":" + left_col_name + "," + right_table + ":" + right_col_name;
							full_outer_join_cols_val = "'" + OpenIVMUtils::EscapeSingleQuotes(val) + "'";
						}
					}
				}
				for (auto &child : n->children) {
					extract_foj_cols(child.get());
				}
			};
			extract_foj_cols(plan.get());
		}

		ddl.push_back("insert or replace into " + string(ivm::VIEWS_TABLE) + " values ('" + view_name + "', '" +
		              OpenIVMUtils::EscapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) + ", " +
		              (found_minmax ? "true" : "false") + ", " + (found_left_join ? "true" : "false") + ", now(), " +
		              refresh_val + ", false, " + group_cols_val + ", " + agg_types_val + ", " + having_val + ", " +
		              (found_full_outer ? "true" : "false") + ", " + full_outer_join_cols_val + ")");

		// Classify each base table by catalog type (duckdb vs ducklake).
		// DuckLake tables use native change tracking; DuckDB tables use delta tables.
		//
		// Catalog::GetEntry inside BeginTransaction() cannot see DuckLake entries:
		// DuckLake requires its own transaction protocol. Walk the logical plan's
		// DUCKLAKE_SCAN nodes instead — same approach used in ducklake_join.cpp.
		struct DuckLakeTableInfo {
			string table_name;   // actual name as stored in DuckLake (case-preserved)
			string catalog_name; // DuckLake catalog name (e.g., "dl")
		};
		unordered_map<string, DuckLakeTableInfo> dl_table_info; // keyed by lowercased name
		{
			std::function<void(LogicalOperator *)> collect_dl = [&](LogicalOperator *op) {
				if (!op) {
					return;
				}
				if (op->type == LogicalOperatorType::LOGICAL_GET) {
					auto &get = op->Cast<LogicalGet>();
					if (get.function.name == "ducklake_scan" && get.function.function_info) {
						auto &info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
						string lc = info.table_name;
						std::transform(lc.begin(), lc.end(), lc.begin(),
						               [](unsigned char c) { return std::tolower(c); });
						if (dl_table_info.find(lc) == dl_table_info.end()) {
							// Always pull the catalog from the DuckLakeTableEntry — it's the
							// one the `dl.ORDER_LINE` reference actually resolved to. Falling
							// back to `current_catalog` is wrong for cross-system MVs (native
							// MV reading from dl.*) because `current_catalog` is the physical
							// default, not the DuckLake catalog.
							string cat = info.table.ParentCatalog().GetName();
							if (cat.empty()) {
								cat = current_catalog.empty() ? "dl" : current_catalog;
							}
							dl_table_info[lc] = {info.table_name, cat};
						}
					}
				}
				for (auto &child : op->children) {
					collect_dl(child.get());
				}
			};
			collect_dl(plan.get());
		}

		unordered_set<string> ducklake_tables;
		// Single snapshot query per DuckLake catalog (all tables share the same snapshot).
		string dl_snapshot_val = "null";
		if (!dl_table_info.empty()) {
			// Use the first entry's catalog — all source tables in one MV share one catalog.
			string cat = dl_table_info.begin()->second.catalog_name;
			auto snap_result = con.Query("SELECT id FROM " + cat + ".current_snapshot()");
			if (!snap_result->HasError() && snap_result->RowCount() > 0) {
				dl_snapshot_val = snap_result->GetValue(0, 0).ToString();
			}
		}

		for (const auto &table_name : table_names) {
			string catalog_type = "duckdb";
			string snapshot_val = "null";
			string meta_table_name = OpenIVMUtils::DeltaName(table_name);

			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			auto it = dl_table_info.find(table_lc);
			if (it != dl_table_info.end()) {
				catalog_type = "ducklake";
				meta_table_name = it->second.table_name; // case-preserved name
				ducklake_tables.insert(it->second.table_name);
				ducklake_tables.insert(table_name); // also insert SQL-parsed name
				snapshot_val = dl_snapshot_val;
				OPENIVM_DEBUG_PRINT("[CREATE MV] DuckLake table '%s' → meta_name='%s', snap=%s\n", table_name.c_str(),
				                    meta_table_name.c_str(), snapshot_val.c_str());
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
		// The view_query may contain unqualified base-table references (e.g. `FROM WAREHOUSE`
		// when the user wrote the MV under `USE dl.main`). The DDL executor's fresh
		// Connection starts in the physical-default catalog, so apply USE before CREATE
		// TABLE AS so those unqualified names resolve in the MV's catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			ddl.push_back("use " + current_catalog + "." + current_schema);
		}
		ddl.push_back("create table " + qdt + " as " + view_query);
		if (pac_loaded) {
			ddl.push_back("SET pac_check = false");
			ddl.push_back("SET pac_rewrite = false");
		}

		// User-facing VIEW hides internal _ivm_* columns via EXCLUDE.
		// If LPTS fell back to the original SQL, the data table has only the
		// user-visible columns — no `_ivm_*` columns even if the rewritten plan
		// would have added them via AVG/STDDEV decomposition. Skip the EXCLUDE
		// list in that case; otherwise CREATE VIEW fails on nonexistent columns.
		{
			// Collect internal column names from the LPTS output
			vector<string> internal_cols;
			if (!lpts_fallback) {
				for (auto &name : output_names) {
					if (IVMTableNames::IsInternalColumn(name)) {
						internal_cols.push_back(name);
					}
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
			// DuckLake tables don't need delta tables — change tracking is native.
			// `ducklake_tables` stores the catalog-normalized (lowercase) name, so
			// compare against a normalized copy of the SQL-parsed name.
			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			if (ducklake_tables.count(table_name) || ducklake_tables.count(table_lc)) {
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
				catalog_value = Value(current_catalog.empty() ? "memory" : current_catalog);
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
		// DuckLake does not support indexes. Skip when: (a) any source table is DuckLake, OR
		// (b) view_catalog_prefix is non-empty, meaning the data table lands in a non-default
		// catalog (which is always DuckLake when set via the current_catalog != default_db path).
		if ((ivm_type == IVMType::AGGREGATE_GROUP || ivm_type == IVMType::AGGREGATE_HAVING) &&
		    !aggregate_columns.empty() && ducklake_tables.empty() && view_catalog_prefix.empty()) {
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

		// Restore physical-default catalog so subsequent unqualified references to
		// system tables (_duckdb_ivm_delta_tables, etc.) resolve correctly. The USE
		// inserted before `create table qdt as view_query` routed unqualified base
		// tables through the user's catalog; flip back for the metadata UPDATE below.
		if (!current_catalog.empty() && current_catalog != default_db) {
			ddl.push_back("use " + default_db + ".main");
		}

		// After all tables are created and populated, update DuckLake snapshot IDs
		// to the current snapshot. This ensures the first refresh only sees changes
		// made AFTER the MV was created (not the initial data load).
		for (const auto &table_name : table_names) {
			if (ducklake_tables.count(table_name)) {
				// Use the DuckLake catalog for current_snapshot() — NOT view_catalog_prefix
				// or current_catalog. For cross-system MVs (native MV reading from dl.*),
				// view_catalog_prefix is empty and current_catalog is the physical-default
				// (e.g. the file DB) which doesn't have `current_snapshot()`.
				// table_names entries may be lowercased by the parser; the metadata row was
				// inserted above with the case-preserved DuckLake name (`dl_table_info`).
				string table_lc_for_lookup = table_name;
				std::transform(table_lc_for_lookup.begin(), table_lc_for_lookup.end(), table_lc_for_lookup.begin(),
				               [](unsigned char c) { return std::tolower(c); });
				auto info_it = dl_table_info.find(table_lc_for_lookup);
				string meta_table_name = (info_it != dl_table_info.end()) ? info_it->second.table_name : table_name;
				string dl_cat_name = (info_it != dl_table_info.end()) ? info_it->second.catalog_name : "dl";
				ddl.push_back("UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = (SELECT id FROM " +
				              dl_cat_name + ".current_snapshot()) WHERE view_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(view_name) + "' AND table_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(meta_table_name) + "'");
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
