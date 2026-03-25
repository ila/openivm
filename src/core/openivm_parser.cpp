#include "core/openivm_parser.hpp"

#include "core/openivm_utils.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"

#include "core/openivm_debug.hpp"

#include <iostream>
#include <stack>
#include <duckdb/planner/expression/bound_function_expression.hpp>

namespace duckdb {

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	auto query_lower = OpenIVMUtils::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);

	query_lower.erase(remove(query_lower.begin(), query_lower.end(), '\n'), query_lower.end());

	if (!StringUtil::Contains(query_lower, "create materialized view")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	OpenIVMUtils::ReplaceMaterializedView(query_lower);

	OpenIVMUtils::ReplaceCount(query_lower);
	OpenIVMUtils::ReplaceSum(query_lower);
	OpenIVMUtils::ReplaceMin(query_lower);
	OpenIVMUtils::ReplaceMax(query_lower);
	OPENIVM_DEBUG_PRINT("[CREATE MV] After rewrite: %s\n", query_lower.c_str());

	Parser p;
	p.ParseQuery(query_lower);

	return ParserExtensionParseResult(
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(move(p.statements[0]), true));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

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
		auto view_query = OpenIVMUtils::ExtractViewQuery(statement->query);

		string db_path = context.db->GetFileSystem().GetWorkingDirectory();
		Value db_path_value;
		context.TryGetCurrentSetting("ivm_files_path", db_path_value);
		if (!db_path_value.IsNull()) {
			db_path = db_path_value.ToString();
		}
		string compiled_file_path = db_path + "/ivm_compiled_queries_" + view_name + ".sql";
		string system_tables_path = db_path + "/ivm_system_tables.sql";
		auto index_file_path = db_path + "/ivm_index_" + view_name + ".sql";

		auto table_names = con.GetTableNames(statement->query);

		Planner planner(context);

		planner.CreatePlan(statement->Copy());
		auto plan = move(planner.plan);

		OPENIVM_DEBUG_PRINT("[CREATE MV] View name: %s\n", view_name.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] View query: %s\n", view_query.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Logical plan:\n%s\n", plan->ToString().c_str());

		std::stack<LogicalOperator *> node_stack;
		node_stack.push(plan.get());

		bool found_aggregation = false;
		bool found_projection = false;
		vector<string> aggregate_columns;

		while (!node_stack.empty()) {
			auto current = node_stack.top();
			node_stack.pop();

			if (current->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				found_aggregation = true;
				auto node = dynamic_cast<LogicalAggregate *>(current);
				for (auto &group : node->groups) {
					if (group->type == ExpressionType::BOUND_COLUMN_REF) {
						auto column = dynamic_cast<BoundColumnRefExpression *>(group.get());
						aggregate_columns.emplace_back(column->alias);
					} else if (group->type == ExpressionType::BOUND_FUNCTION) {
						auto column = dynamic_cast<BoundFunctionExpression *>(group.get());
						auto function = column->GetName();
						function = StringUtil::Replace(function, "\"", "\"\"");
						function = "\"" + function + "\"";
						aggregate_columns.emplace_back(function);
					}
				}
			}

			if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				found_projection = true;
			}

			if (!current->children.empty()) {
				for (auto it = current->children.rbegin(); it != current->children.rend(); ++it) {
					node_stack.push(it->get());
				}
			}
		}

		IVMType ivm_type;

		if (found_aggregation && !aggregate_columns.empty()) {
			ivm_type = IVMType::AGGREGATE_GROUP;
		} else if (found_aggregation && aggregate_columns.empty()) {
			ivm_type = IVMType::SIMPLE_AGGREGATE;
		} else if (found_projection && !found_aggregation) {
			ivm_type = IVMType::SIMPLE_PROJECTION;
		} else {
			throw NotImplementedException("IVM does not support this query type yet");
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Detected IVM type: %s (aggregation=%d, projection=%d, group_cols=%zu)\n",
		                    ivm_type == IVMType::AGGREGATE_GROUP     ? "AGGREGATE_GROUP"
		                    : ivm_type == IVMType::SIMPLE_AGGREGATE  ? "SIMPLE_AGGREGATE"
		                    : ivm_type == IVMType::SIMPLE_PROJECTION ? "SIMPLE_PROJECTION"
		                                                             : "UNKNOWN",
		                    (int)found_aggregation, (int)found_projection, aggregate_columns.size());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Source tables:");
		for (const auto &t : table_names) {
			OPENIVM_DEBUG_PRINT(" %s", t.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n");

		auto system_table = "create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string "
		                    "varchar, type tinyint, last_update timestamp);\n";
		OpenIVMUtils::WriteFile(system_tables_path, false, system_table);

		auto delta_tables_table = "create table if not exists _duckdb_ivm_delta_tables (view_name varchar, table_name "
		                          "varchar, last_update timestamp, primary key(view_name, table_name));\n";
		OpenIVMUtils::WriteFile(system_tables_path, true, delta_tables_table);

		auto ivm_table_insert = "insert or replace into _duckdb_ivm_views values ('" + view_name + "', '" +
		                        OpenIVMUtils::EscapeSingleQuotes(view_query) + "', " + to_string((int)ivm_type) +
		                        ", now());\n";
		OpenIVMUtils::WriteFile(system_tables_path, true, ivm_table_insert);

		auto table = "create table " + view_name + " as " + view_query + ";\n";
		OpenIVMUtils::WriteFile(compiled_file_path, false, table);

		for (const auto &table_name : table_names) {
			Value catalog_value;
			Value schema_value;

			if (catalog_value.IsNull() && !context.db->config.options.database_path.empty()) {
				auto catalog_name =
				    con.Query("select table_catalog from information_schema.tables where table_name = '" + table_name +
				              "';")
				        ->Fetch()
				        ->GetValue(0, 0);
				catalog_value = catalog_name;
			} else if (catalog_value.IsNull()) {
				catalog_value = Value("memory");
			}

			if (schema_value.IsNull()) {
				schema_value = Value("main");
			}

			auto catalog_schema = catalog_value.ToString() + "." + schema_value.ToString() + ".";

			auto delta_table =
			    "create table if not exists " + catalog_schema + "delta_" + table_name +
			    " as select *, true as _duckdb_ivm_multiplicity, now()::timestamp as _duckdb_ivm_timestamp from " +
			    catalog_schema + table_name + " limit 0;\n";
			OpenIVMUtils::WriteFile(compiled_file_path, true, delta_table);

			auto delta_table_insert = "insert into _duckdb_ivm_delta_tables values ('" + view_name + "', 'delta_" +
			                          table_name + "', now());\n";
			OpenIVMUtils::WriteFile(system_tables_path, true, delta_table_insert);
		}

		string delta_view = "create table if not exists delta_" + view_name +
		                    " as select *, true as _duckdb_ivm_multiplicity, now()::timestamp as "
		                    "_duckdb_ivm_timestamp from " +
		                    view_name + " limit 0;\n";
		delta_view += "alter table delta_" + view_name + " alter _duckdb_ivm_timestamp set default now();\n";
		OpenIVMUtils::WriteFile(compiled_file_path, true, delta_view);

		if (ivm_type == IVMType::AGGREGATE_GROUP) {
			string index_query_view = "create unique index " + view_name + "_ivm_index on " + view_name + "(";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				index_query_view += aggregate_columns[i];
				if (i != aggregate_columns.size() - 1) {
					index_query_view += ", ";
				}
			}
			index_query_view += ");\n";
			OpenIVMUtils::WriteFile(index_file_path, false, index_query_view);
		}

		auto system_queries = duckdb::OpenIVMUtils::ReadFile(system_tables_path);
		OPENIVM_DEBUG_PRINT("[CREATE MV] Executing system table queries:\n%s\n", system_queries.c_str());
		for (auto &query : StringUtil::Split(system_queries, '\n')) {
			auto r = con.Query(query);
			if (r->HasError()) {
				throw Exception(ExceptionType::PARSER, "Could not create system tables: " + r->GetError());
			}
		}

		// Execute the view creation query first (with PAC enabled, so the PAC
		// optimizer can rewrite it), then disable PAC for the delta table queries
		// which use SELECT * and need to project protected columns outside aggregates.
		auto queries = duckdb::OpenIVMUtils::ReadFile(compiled_file_path);
		OPENIVM_DEBUG_PRINT("[CREATE MV] Executing compiled queries:\n%s\n", queries.c_str());
		auto query_lines = StringUtil::Split(queries, '\n');
		bool first_query = true;
		for (auto &query : query_lines) {
			auto r = con.Query(query);
			if (r->HasError()) {
				throw Exception(ExceptionType::PARSER, "Could not create materialized view: " + r->GetError());
			}
			if (first_query && pac_loaded) {
				con.Query("SET pac_check = false");
				con.Query("SET pac_rewrite = false");
				first_query = false;
			}
		}

		if (ivm_type == IVMType::AGGREGATE_GROUP) {
			auto index = duckdb::OpenIVMUtils::ReadFile(index_file_path);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Executing index query: %s\n", index.c_str());
			auto r = con.Query(index);
			if (r->HasError()) {
				throw Exception(ExceptionType::PARSER, "Could not create index: " + r->GetError());
			}
		}
		OPENIVM_DEBUG_PRINT("[CREATE MV] Materialized view '%s' created successfully\n", view_name.c_str());
	}

	ParserExtensionPlanResult result;
	result.function = IVMFunction();
	result.parameters.push_back(true);
	result.modified_databases = {};
	result.requires_valid_transaction = false;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}
} // namespace duckdb
