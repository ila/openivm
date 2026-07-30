#ifndef OPENIVM_PARSER_PLAN_HELPERS_HPP
#define OPENIVM_PARSER_PLAN_HELPERS_HPP

#include "core/incremental_checker.hpp"
#include "core/refresh_metadata.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

struct DuckLakeSourceTableInfo {
	string table_name;
	string catalog_name;
	string schema_name;
	int64_t table_id = -1;
};

struct SourceTableInfo {
	string table_name;
	string catalog_name;
	string schema_name;
};

struct BaseColumnRef {
	string table;
	string column;
};

struct OccurrenceColumnRef {
	string table;
	idx_t occurrence = 0;
	string column;
};

struct ProjectionSourceOccurrence {
	string table;
	idx_t occurrence = 0;
	idx_t table_index = 0;
};

struct ProjectionLineageEdge {
	OccurrenceColumnRef from;
	OccurrenceColumnRef to;
};

struct CreateMVPlanFacts {
	LogicalOperator *root = nullptr;
	PlanAnalysis analysis;
	unordered_map<string, SourceTableInfo> source_table_info;
	unordered_map<string, DuckLakeSourceTableInfo> ducklake_table_info;
	LogicalProjection *first_projection = nullptr;
	LogicalComparisonJoin *first_comparison_join = nullptr;
	vector<LogicalProjection *> projections;
	vector<LogicalAggregate *> aggregates;
	vector<LogicalComparisonJoin *> comparison_joins;
	vector<LogicalWindow *> windows;
	unordered_map<idx_t, LogicalProjection *> projections_by_index;
	unordered_map<idx_t, LogicalGet *> gets_by_index;
	unordered_map<idx_t, LogicalSetOperation *> setops_by_index;
	unordered_map<idx_t, LogicalCTERef *> cte_refs_by_table_index;
	unordered_map<idx_t, LogicalOperator *> cte_defs_by_index;
	unordered_map<string, string> join_parents;
	vector<BoundColumnRefExpression *> join_refs;
	vector<pair<BaseColumnRef, BaseColumnRef>> inner_join_edges;
	unordered_set<string> single_delim_key_bindings;
	unordered_map<idx_t, ProjectionSourceOccurrence> occurrence_by_index;
	vector<ProjectionSourceOccurrence> source_occurrences;
	vector<ProjectionLineageEdge> projection_lineage_edges;
	unordered_map<const LogicalOperator *, string> first_table_name;
	unordered_map<idx_t, int> cte_refs_under_join_count;
	bool has_union_before_aggregate = false;
	bool has_unsupported_set_operation = false;
	bool has_repeated_cte_ref_under_join = false;
	bool has_pivot = false;
	bool has_filter_above_aggregate = false;
	bool has_bound_aggregate_filter = false;
	bool has_hidden_minmax_having_column = false;
	bool has_computed_minmax_aggregate_projection = false;
	bool has_computed_sum_aggregate_projection = false;
	bool has_top_level_redundant_scalar_distinct = false;
};

string BuildTopKSuffix(const vector<BoundOrderByNode> &orders, idx_t limit_val, idx_t offset_val,
                       const vector<string> &output_col_names, bool include_limit = true);
void InlineCtesIfPresent(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan);
string QualifyCreateSourceTable(const string &table_name, const string &current_catalog, const string &current_schema,
                                const string &default_db);
string ExplainInitialLoadQuery(Connection &con, const string &label, const string &query);
CreateMVPlanFacts BuildCreateMVPlanFacts(LogicalOperator *plan, const string &current_catalog);
bool ProducesAtMostOneRow(LogicalOperator &node);
void AddJoinKeyColumn(const unique_ptr<Expression> &expr, unordered_map<idx_t, unordered_set<idx_t>> &join_key_cols);
bool OuterJoinAggregateNeedsRecompute(const CreateMVPlanFacts &facts, idx_t group_index);
// delta_view_catalog_prefix must be the same prefix the view's delta table was created with
// (internal_catalog_prefix). Without it the generated INSERT targets an unqualified delta table,
// which fails for MVs whose state lives in another catalog -- e.g. DuckLake-backed views, where the
// delta table is dl.main.openivm_delta_<view>.
// The returned SQL carries LJSEC_INNER_DELTA_PLACEHOLDER / LJSEC_PRES_DELTA_PLACEHOLDER where each
// side's pending changes belong; refresh substitutes them (see openivm_constants.hpp). The out_*
// params report the two sides' identities so refresh can pick the right row source per backend.
string BuildLeftJoinSecondaryDeltaSQL(ClientContext &context, const CreateMVPlanFacts &facts,
                                      const vector<string> &output_names, const string &view_name,
                                      vector<string> &preserved_cols, const string &delta_view_catalog_prefix,
                                      vector<string> &out_inner_tables, vector<string> &out_inner_keys,
                                      vector<string> &out_pres_tables, vector<string> &out_pres_keys);
bool OuterJoinPreservedSideHasTableFunction(const CreateMVPlanFacts &facts);
vector<string> DeriveGroupColumnNames(const CreateMVPlanFacts &facts, idx_t group_index, size_t group_count,
                                      const vector<string> &output_names);
vector<string> DeriveScalarDelimKeyColumnNames(const CreateMVPlanFacts &facts, const vector<string> &output_names);
vector<string> DeriveAggregateGroupColumnNames(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                                               bool include_first_aggregate);
void ResolveWindowPartitionOutputNames(const CreateMVPlanFacts &facts, vector<string> &partition_columns,
                                       const vector<string> &output_names);
string BuildRefreshLineageJson(const vector<string> &entries);
bool BuildWindowPartitionLineageOps(const CreateMVPlanFacts &facts, const vector<string> &partition_columns,
                                    vector<RefreshMetadata::WindowPartitionLineageOp> &out,
                                    vector<RefreshMetadata::WindowPartitionLineageOp> *direct_out = nullptr);
bool BuildProjectionKeyLineage(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                               RefreshMetadata::ProjectionKeyLineage &out);
bool BuildLeftJoinKeySource(const CreateMVPlanFacts &facts, RefreshMetadata::LeftJoinKeySource &out);
bool BuildLeftJoinNullableSources(const CreateMVPlanFacts &facts, RefreshMetadata::LeftJoinNullableSources &out);
bool QueryNeedsOriginalSqlForLpts(const string &query);
bool PlanNeedsOriginalSqlForLpts(LogicalOperator *op);
void ResolveAggregateGroupColumnsThroughJoinKeys(const CreateMVPlanFacts &facts, vector<string> &aggregate_columns,
                                                 const vector<string> &output_names);
string ExtractFullOuterJoinMetadata(const CreateMVPlanFacts &facts);
vector<string> PrepareOutputNames(LogicalOperator *select_plan, const vector<string> &planner_names);
LogicalAggregate *FindOuterAggregate(LogicalOperator *op);
bool IsPacLoaded(ClientContext &context);
void ForwardPacSettingsIfLoaded(ClientContext &context, Connection &con);

} // namespace duckdb

#endif // OPENIVM_PARSER_PLAN_HELPERS_HPP
