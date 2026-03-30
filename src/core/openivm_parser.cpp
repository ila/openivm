#include "core/openivm_parser.hpp"

#include "core/ivm_checker.hpp"
#include "core/ivm_plan_rewrite.hpp"
#include "core/openivm_constants.hpp"
#include "logical_plan_to_sql.hpp"
#include "core/openivm_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include "core/openivm_debug.hpp"

#include <regex>
#include <stack>
#include <duckdb/planner/expression/bound_function_expression.hpp>

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

	if (!StringUtil::Contains(query_lower, "create materialized view")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	OpenIVMUtils::ReplaceMaterializedView(query_lower);
	// All other rewrites (DISTINCT, AVG, LEFT JOIN key, aggregate aliases) are done
	// at the plan level in IVMPlanFunction via IVMPlanRewrite + LPTS.
	OPENIVM_DEBUG_PRINT("[CREATE MV] After structural rewrite: %s\n", query_lower.c_str());

	Parser p;
	p.ParseQuery(query_lower);

	return ParserExtensionParseResult(
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(p.statements[0]), true));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	ParserExtensionPlanResult result;

	if (ivm_parse_data.plan) {
		Connection con(*context.db.get());

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

		auto view_name = OpenIVMUtils::ExtractTableName(statement->query);
		auto original_view_query = OpenIVMUtils::ExtractViewQuery(statement->query);
		string view_query = original_view_query; // will be overwritten by LPTS for DDL

		// Use con for planning — sees all committed state from previous bind-phase DDL
		con.BeginTransaction();
		auto table_names = con.GetTableNames(statement->query);

		// Plan the full CREATE TABLE AS SELECT statement (for plan walking)
		Planner planner(*con.context);
		planner.CreatePlan(statement->Copy());
		auto plan = std::move(planner.plan);

		// Plan the raw SELECT query separately for IVM plan rewrite + LPTS conversion
		{
			Parser select_parser;
			select_parser.ParseQuery(original_view_query);
			Planner select_planner(*con.context);
			select_planner.CreatePlan(std::move(select_parser.statements[0]));
			auto select_plan = std::move(select_planner.plan);

			// Apply IVM plan rewrites (DISTINCT → GROUP BY + COUNT, AVG → SUM + COUNT, LEFT JOIN key)
			IVMPlanRewrite(context, select_plan, select_planner.names);

			// Sanitize column names: replace special chars with underscores, collapse runs, trim.
			// "min(val)" → "min_val", "count_star()" → "count_star", "SUM(x) AS total" → "total"
			vector<string> output_names = select_planner.names;
			for (auto &name : output_names) {
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
			LogicalPlanToSql lpts(*con.context, select_plan, output_names);
			auto cte_list = lpts.LogicalPlanToCteList();
			view_query = LogicalPlanToSql::CteListToSql(cte_list);
			// Strip trailing semicolon — the query is embedded in other SQL statements
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

		// Check if the plan is fully IVM-compatible
		bool ivm_compatible = ValidateIVMPlan(plan.get());

		std::stack<LogicalOperator *> node_stack;
		node_stack.push(plan.get());

		bool found_aggregation = false;
		bool found_projection = false;
		bool found_distinct = false;
		bool found_having = false;
		vector<string> aggregate_columns;

		while (!node_stack.empty()) {
			auto current = node_stack.top();
			node_stack.pop();

			if (current->type == LogicalOperatorType::LOGICAL_DISTINCT) {
				found_distinct = true;
				// DISTINCT columns become group-by keys after IVM rewrite
				auto *distinct_node = dynamic_cast<LogicalDistinct *>(current);
				if (!distinct_node->distinct_targets.empty()) {
					for (auto &target : distinct_node->distinct_targets) {
						aggregate_columns.emplace_back(target->GetName());
					}
				} else {
					// Plain DISTINCT: all child output columns are keys
					auto child_bindings = current->children[0]->GetColumnBindings();
					for (idx_t i = 0; i < current->children[0]->types.size(); i++) {
						aggregate_columns.emplace_back("col" + to_string(i));
					}
				}
			} else if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				found_aggregation = true;
				auto node = dynamic_cast<LogicalAggregate *>(current);
				for (auto &group : node->groups) {
					if (group->type == ExpressionType::BOUND_COLUMN_REF) {
						auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
						aggregate_columns.emplace_back(column->alias);
					} else if (group->type == ExpressionType::BOUND_FUNCTION) {
						auto column = dynamic_cast<BoundFunctionExpression *>(group.get());
						// Use alias if available (e.g., ABS(val) AS abs_val → use "abs_val").
						// GROUP BY expressions don't carry aliases — check the alias field anyway,
						// then fall back to the return_type column name from the output.
						if (!column->alias.empty()) {
							aggregate_columns.emplace_back(column->alias);
						} else {
							// The output column name for this expression comes from the AS clause.
							// Use GetName() but sanitize: "abs(val)" → quoted identifier.
							auto function = column->GetName();
							function = StringUtil::Replace(function, "\"", "\"\"");
							function = "\"" + function + "\"";
							aggregate_columns.emplace_back(function);
						}
					}
				}
			}

			if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				found_projection = true;
			}

			// Detect HAVING: a FILTER above an AGGREGATE
			if (current->type == LogicalOperatorType::LOGICAL_FILTER && !current->children.empty() &&
			    current->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				found_having = true;
			}

			if (!current->children.empty()) {
				for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
					node_stack.push(it->get());
				}
			}
		}

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
		              " (view_name varchar primary key, sql_string "
		              "varchar, type tinyint, last_update timestamp)");

		ddl.push_back("create table if not exists " + string(ivm::DELTA_TABLES_TABLE) +
		              " (view_name varchar, table_name "
		              "varchar, last_update timestamp, primary key(view_name, table_name))");

		// Store the LPTS query in metadata — it has hidden columns (DISTINCT count, AVG sum/count,
		// LEFT JOIN key) and preserves user column names.
		ddl.push_back("insert or replace into " + string(ivm::VIEWS_TABLE) + " values ('" + view_name + "', '" +
		              OpenIVMUtils::EscapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) + ", now())");

		for (const auto &table_name : table_names) {
			ddl.push_back("insert into " + string(ivm::DELTA_TABLES_TABLE) + " values ('" + view_name + "', '" +
			              OpenIVMUtils::DeltaName(table_name) + "', now())");
		}

		// --- Compiled DDL (MV creation, delta tables, delta view) ---
		// MV creation query (with PAC enabled if loaded)
		ddl.push_back("create table " + view_name + " as " + view_query);
		if (pac_loaded) {
			ddl.push_back("SET pac_check = false");
			ddl.push_back("SET pac_rewrite = false");
		}

		for (const auto &table_name : table_names) {
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

		ddl.push_back("create table if not exists " + OpenIVMUtils::DeltaName(view_name) + " as select *, true as " +
		              string(ivm::MULTIPLICITY_COL) + ", now()::timestamp as " + string(ivm::TIMESTAMP_COL) + " from " +
		              view_name + " limit 0");
		ddl.push_back("alter table " + OpenIVMUtils::DeltaName(view_name) + " alter " + string(ivm::TIMESTAMP_COL) +
		              " set default now()");

		// --- Index DDL (for aggregate group queries) ---
		if (ivm_type == IVMType::AGGREGATE_GROUP || ivm_type == IVMType::AGGREGATE_HAVING) {
			string index_query_view = "create unique index " + view_name + "_ivm_index on " + view_name + "(";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				index_query_view += aggregate_columns[i];
				if (i != aggregate_columns.size() - 1) {
					index_query_view += ", ";
				}
			}
			index_query_view += ")";
			ddl.push_back(index_query_view);
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Compiled %lu DDL queries for bind phase\n", (unsigned long)ddl.size());

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
