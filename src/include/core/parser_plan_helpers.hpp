#ifndef OPENIVM_PARSER_PLAN_HELPERS_HPP
#define OPENIVM_PARSER_PLAN_HELPERS_HPP

#include "core/incremental_checker.hpp"
#include "core/parser.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

struct DuckLakeSourceTableInfo {
	string table_name;
	string catalog_name;
	string schema_name;
};

struct SourceTableInfo {
	string table_name;
	string catalog_name;
	string schema_name;
};

struct MVClassificationState {
	bool incremental_compatible;
	bool found_aggregation;
	bool found_projection;
	bool found_distinct;
	bool found_having;
	bool found_minmax;
	bool found_left_join;
	bool found_full_outer;
	bool found_semi_anti_join;
	bool found_window;
	bool found_join;
	bool found_top_k;
	bool found_count_distinct;
	bool found_grouping_sets;
	bool found_nested_aggregate;
	bool found_filtered_list;
	bool found_single_join;

	explicit MVClassificationState(const PlanAnalysis &analysis)
	    : incremental_compatible(analysis.incremental_compatible), found_aggregation(analysis.found_aggregation),
	      found_projection(analysis.found_projection), found_distinct(analysis.found_distinct),
	      found_having(analysis.found_having),
	      found_minmax(analysis.found_minmax || analysis.found_count_distinct || analysis.found_list),
	      found_left_join(analysis.found_left_join), found_full_outer(analysis.found_full_outer),
	      found_semi_anti_join(analysis.found_semi_anti_join), found_window(analysis.found_window),
	      found_join(analysis.found_join), found_top_k(analysis.found_top_k),
	      found_count_distinct(analysis.found_count_distinct), found_grouping_sets(analysis.found_grouping_sets),
	      found_nested_aggregate(analysis.found_nested_aggregate), found_filtered_list(analysis.found_filtered_list),
	      found_single_join(analysis.found_single_join) {
	}
};

string SqlCsvLiteralOrNull(const vector<string> &values);
void ConfigureDDLExecutorResult(ParserExtensionPlanResult &result);
string BuildTopKSuffix(const vector<BoundOrderByNode> &orders, idx_t limit_val, idx_t offset_val,
                       const vector<string> &output_col_names);
void InlineCtesIfPresent(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan);
bool BoolSetting(ClientContext &context, const string &name);
string QualifyCreateSourceTable(const string &table_name, const string &current_catalog, const string &current_schema,
                                const string &default_db);
string ExplainInitialLoadQuery(Connection &con, const string &label, const string &query);
bool HasUnionBeforeAggregate(const LogicalOperator *op, bool seen_agg_above = false);
bool HasUnsupportedSetOperation(const LogicalOperator *op);
LogicalProjection *FindFirstProjection(LogicalOperator *op);
LogicalComparisonJoin *FindFirstComparisonJoin(LogicalOperator *op);
void AddJoinKeyColumn(const unique_ptr<Expression> &expr, unordered_map<idx_t, unordered_set<idx_t>> &join_key_cols);
bool OuterJoinAggregateNeedsRecompute(LogicalOperator *op, idx_t group_index);
bool HasRepeatedCteRefUnderJoin(const LogicalOperator *plan);
void CollectDuckLakeTables(LogicalOperator *op, const string &current_catalog,
                           unordered_map<string, DuckLakeSourceTableInfo> &dl_table_info);
void CollectSourceTables(LogicalOperator *op, unordered_map<string, SourceTableInfo> &source_table_info);
bool RelationExists(Connection &con, const string &qualified_name);
vector<string> DeriveGroupColumnNames(LogicalOperator *plan, idx_t group_index, size_t group_count,
                                      const vector<string> &output_names, bool has_union_over_agg);
vector<string> DeriveScalarDelimKeyColumnNames(LogicalOperator *plan, const vector<string> &output_names);
vector<string> DeriveAggregateGroupColumnNames(LogicalOperator *plan, const vector<string> &output_names,
                                               bool include_first_aggregate);
void ResolveWindowPartitionOutputNames(LogicalOperator *plan, vector<string> &partition_columns,
                                       const vector<string> &output_names);
void ResolveAggregateGroupColumnsThroughJoinKeys(LogicalOperator *plan, vector<string> &aggregate_columns,
                                                 const vector<string> &output_names);
string ExtractFullOuterJoinMetadata(LogicalOperator *plan);
vector<string> PrepareOutputNames(LogicalOperator *select_plan, const vector<string> &planner_names);
bool QueryNeedsOriginalSqlForLpts(const string &query);
bool QueryHasUnsupportedIncrementalConstruct(const string &query);
bool PlanNeedsOriginalSqlForLpts(LogicalOperator *op);
LogicalAggregate *FindOuterAggregate(LogicalOperator *op);
bool IsPacLoaded(ClientContext &context);
void ForwardPacSettingsIfLoaded(ClientContext &context, Connection &con);
void AppendCreateMVSystemTablesDDL(vector<string> &ddl, const string &view_name, bool is_replace);
string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_PARSER_PLAN_HELPERS_HPP
