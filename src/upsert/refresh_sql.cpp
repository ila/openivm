#include "upsert/refresh_internal.hpp"

#include "compile_facts.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/scoped_optimizer_settings.hpp"
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
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/planner.hpp"

#include <regex>

namespace duckdb {

namespace {

constexpr const char *DUCKLAKE_SYNTHETIC_DELTA_TS = "1970-01-02 00:00:00";
constexpr const char *DUCKLAKE_SYNTHETIC_LAST_UPDATE = "1970-01-01 00:00:00";

static string SparkPortableRefreshSQL(string sql) {
	static const std::regex quoted_cast_regex(R"(('[^']*(?:''[^']*)*')::(TIMESTAMP|DATE))",
	                                          std::regex_constants::icase);
	static const std::regex bigint_cast_regex(R"(([A-Za-z_][A-Za-z0-9_\.]*)::BIGINT)", std::regex_constants::icase);
	static const std::regex now_regex(R"(\bnow\(\))", std::regex_constants::icase);
	static const std::regex null_safe_regex(R"(\s+IS\s+NOT\s+DISTINCT\s+FROM\s+)", std::regex_constants::icase);
	sql = std::regex_replace(sql, quoted_cast_regex, "CAST($1 AS $2)");
	sql = std::regex_replace(sql, bigint_cast_regex, "CAST($1 AS BIGINT)");
	sql = std::regex_replace(sql, now_regex, "current_timestamp()");
	sql = std::regex_replace(sql, null_safe_regex, " <=> ");
	return sql;
}

struct SemiAntiSourceInput {
	string table_sql;
	string delta_sql;
	string last_update;
};

struct RefreshPlan {
	// TODO: Fill this from a persisted create-time DeltaViewModel/DeltaStrategyPlan once the model is stored in
	// metadata. This v0 plan is intentionally a typed facade over the existing metadata so generated SQL order stays
	// unchanged while refresh dispatch moves out of raw RefreshType conditionals.
	RefreshType refresh_type = RefreshType::FULL_REFRESH;
	bool force_full_refresh = false;
	bool metadata_requires_full_refresh = false;
	bool adaptive_recompute = false;
	bool skip_projection_key_delta = false;
	DeltaFastPathFlags delta_flags;

	bool RequiresFullRecompute() const {
		return force_full_refresh || metadata_requires_full_refresh || refresh_type == RefreshType::FULL_REFRESH ||
		       adaptive_recompute;
	}

	bool SkipsDeltaProduction() const {
		return skip_projection_key_delta || refresh_type == RefreshType::WINDOW_PARTITION ||
		       refresh_type == RefreshType::GROUP_RECOMPUTE || refresh_type == RefreshType::DISTINCT_INCREMENTAL ||
		       refresh_type == RefreshType::COUNT_DISTINCT_INCREMENTAL ||
		       refresh_type == RefreshType::SEMI_ANTI_RECOMPUTE || refresh_type == RefreshType::CURRENT_DIFF_RECOMPUTE;
	}

	const char *DeltaProductionSkipReason() const {
		if (skip_projection_key_delta) {
			return "SIMPLE_PROJECTION_KEY";
		}
		switch (refresh_type) {
		case RefreshType::DISTINCT_INCREMENTAL:
			return "DISTINCT_INCREMENTAL";
		case RefreshType::COUNT_DISTINCT_INCREMENTAL:
			return "COUNT_DISTINCT_INCREMENTAL";
		case RefreshType::SEMI_ANTI_RECOMPUTE:
			return "SEMI_ANTI_RECOMPUTE";
		case RefreshType::GROUP_RECOMPUTE:
			return "GROUP_RECOMPUTE";
		case RefreshType::WINDOW_PARTITION:
			return "WINDOW_PARTITION";
		case RefreshType::CURRENT_DIFF_RECOMPUTE:
			return "CURRENT_DIFF_RECOMPUTE";
		default:
			return "UNKNOWN";
		}
	}
};

static void
ApplyGroupRecomputeSourceOccurrences(vector<GroupRecomputeDeltaSpec> &delta_specs,
                                     const vector<RefreshMetadata::GroupRecomputeSourceOccurrence> &occurrences) {
	for (auto &spec : delta_specs) {
		for (auto &occurrence : occurrences) {
			if (StringUtil::CIEquals(spec.base_table, occurrence.table_name)) {
				spec.source_occurrences = occurrence.count == 0 ? 1 : occurrence.count;
				break;
			}
		}
	}
}

static string ResolveDeltaMetadataKey(const string &table_name, const vector<string> &delta_table_names) {
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

static string ResolveSourceTableSQL(RefreshMetadata &metadata, const string &view_name, const string &metadata_key,
                                    const string &table_name, const string &view_catalog_name,
                                    const string &view_schema_name, const string &attached_db_catalog_name,
                                    const string &attached_db_schema_name) {
	string fallback_catalog = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
	string fallback_schema = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
	auto loc = metadata.GetSourceLocation(view_name, metadata_key, fallback_catalog, fallback_schema);
	if (loc.catalog_name.empty()) {
		return table_name;
	}
	if (loc.schema_name.empty()) {
		loc.schema_name = "main";
	}
	return SqlUtils::FullName(loc.catalog_name, loc.schema_name, SqlUtils::LastIdentifierPart(table_name));
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
	string metadata_key = ResolveDeltaMetadataKey(table_name, delta_table_names);
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

	input.table_sql = ResolveSourceTableSQL(metadata, view_name, metadata_key, table_name, view_catalog_name,
	                                        view_schema_name, attached_db_catalog_name, attached_db_schema_name);
	input.delta_sql = metadata.ResolveDeltaQualifiedName(view_name, metadata_key, view_catalog_name, view_schema_name);
	input.last_update = metadata.GetLastUpdate(view_name, metadata_key);
	return input;
}

static bool IsSqlSpace(char c) {
	return c == ' ' || c == '\n' || c == '\r' || c == '\t' || c == '\f';
}

static bool IsSqlIdentChar(char c) {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_';
}

static string TrimCopy(const string &input) {
	idx_t begin = 0;
	while (begin < input.size() && IsSqlSpace(input[begin])) {
		begin++;
	}
	idx_t end = input.size();
	while (end > begin && IsSqlSpace(input[end - 1])) {
		end--;
	}
	return input.substr(begin, end - begin);
}

static string StripIdentifierQuotes(string input) {
	input = TrimCopy(input);
	if (input.size() >= 2 && input.front() == '"' && input.back() == '"') {
		input = input.substr(1, input.size() - 2);
	}
	return StringUtil::Lower(input);
}

static bool IsSimpleIdentifierExpression(const string &expr) {
	string trimmed = TrimCopy(expr);
	if (trimmed.empty()) {
		return false;
	}
	for (auto c : trimmed) {
		if (!(IsSqlIdentChar(c) || c == '"')) {
			return false;
		}
	}
	return true;
}

static vector<string> SplitTopLevelCommaList(const string &input) {
	vector<string> result;
	idx_t start = 0;
	idx_t depth = 0;
	bool in_quote = false;
	for (idx_t i = 0; i < input.size(); i++) {
		char c = input[i];
		if (c == '"') {
			in_quote = !in_quote;
		} else if (!in_quote && c == '(') {
			depth++;
		} else if (!in_quote && c == ')' && depth > 0) {
			depth--;
		} else if (!in_quote && c == ',' && depth == 0) {
			result.push_back(TrimCopy(input.substr(start, i - start)));
			start = i + 1;
		}
	}
	result.push_back(TrimCopy(input.substr(start)));
	return result;
}

static idx_t FindMatchingParen(const string &input, idx_t open_pos) {
	idx_t depth = 0;
	bool in_quote = false;
	for (idx_t i = open_pos; i < input.size(); i++) {
		char c = input[i];
		if (c == '"') {
			in_quote = !in_quote;
		} else if (!in_quote && c == '(') {
			depth++;
		} else if (!in_quote && c == ')') {
			depth--;
			if (depth == 0) {
				return i;
			}
		}
	}
	return string::npos;
}

static idx_t FindCaseInsensitive(const string &haystack, const string &needle, idx_t start = 0) {
	return StringUtil::Lower(haystack).find(StringUtil::Lower(needle), start);
}

struct RefreshCteInfo {
	string name;
	idx_t body_start;
	idx_t body_end;
	vector<string> columns;
	vector<string> select_exprs;
	string relation;
	bool has_where;
};

static vector<RefreshCteInfo> ParseRefreshCtes(const string &sql) {
	vector<RefreshCteInfo> ctes;
	idx_t pos = FindCaseInsensitive(sql, "WITH ");
	if (pos == string::npos) {
		return ctes;
	}
	pos += 5;
	while (pos < sql.size()) {
		while (pos < sql.size() && IsSqlSpace(sql[pos])) {
			pos++;
		}
		idx_t name_start = pos;
		while (pos < sql.size() && IsSqlIdentChar(sql[pos])) {
			pos++;
		}
		if (pos == name_start) {
			break;
		}
		RefreshCteInfo cte;
		cte.name = sql.substr(name_start, pos - name_start);
		while (pos < sql.size() && IsSqlSpace(sql[pos])) {
			pos++;
		}
		if (pos >= sql.size() || sql[pos] != '(') {
			break;
		}
		idx_t cols_end = FindMatchingParen(sql, pos);
		if (cols_end == string::npos) {
			break;
		}
		cte.columns = SplitTopLevelCommaList(sql.substr(pos + 1, cols_end - pos - 1));
		idx_t as_pos = FindCaseInsensitive(sql, " AS (", cols_end);
		if (as_pos == string::npos) {
			break;
		}
		cte.body_start = as_pos + 5;
		cte.body_end = FindMatchingParen(sql, cte.body_start - 1);
		if (cte.body_end == string::npos) {
			break;
		}
		string body = sql.substr(cte.body_start, cte.body_end - cte.body_start);
		idx_t from_pos = FindCaseInsensitive(body, " FROM ");
		if (from_pos != string::npos && FindCaseInsensitive(body, "SELECT ") == 0) {
			cte.select_exprs = SplitTopLevelCommaList(body.substr(7, from_pos - 7));
			idx_t relation_start = from_pos + 6;
			idx_t where_pos = FindCaseInsensitive(body, " WHERE ", relation_start);
			idx_t relation_end = where_pos == string::npos ? body.size() : where_pos;
			cte.relation = TrimCopy(body.substr(relation_start, relation_end - relation_start));
			cte.has_where = where_pos != string::npos;
		}
		ctes.push_back(std::move(cte));
		pos = ctes.back().body_end + 1;
		if (pos < sql.size() && sql[pos] == ',') {
			pos++;
			continue;
		}
		break;
	}
	return ctes;
}

struct ResolvedRefreshColumn {
	ResolvedRefreshColumn() : ok(false), cte_index(DConstants::INVALID_INDEX) {
	}
	ResolvedRefreshColumn(bool ok, idx_t cte_index, string relation, string source_column)
	    : ok(ok), cte_index(cte_index), relation(std::move(relation)), source_column(std::move(source_column)) {
	}
	bool ok;
	idx_t cte_index;
	string relation;
	string source_column;
};

static ResolvedRefreshColumn ResolveRefreshColumnAlias(const vector<RefreshCteInfo> &ctes, const string &alias,
                                                       idx_t depth = 0) {
	if (depth > ctes.size()) {
		return {};
	}
	for (idx_t cte_idx = 0; cte_idx < ctes.size(); cte_idx++) {
		auto &cte = ctes[cte_idx];
		for (idx_t col_idx = 0; col_idx < cte.columns.size() && col_idx < cte.select_exprs.size(); col_idx++) {
			if (!StringUtil::CIEquals(TrimCopy(cte.columns[col_idx]), alias)) {
				continue;
			}
			string expr = TrimCopy(cte.select_exprs[col_idx]);
			if (!IsSimpleIdentifierExpression(expr)) {
				return {};
			}
			for (auto &candidate : ctes) {
				if (StringUtil::CIEquals(cte.relation, candidate.name)) {
					return ResolveRefreshColumnAlias(ctes, expr, depth + 1);
				}
			}
			return {true, cte_idx, cte.relation, StripIdentifierQuotes(expr)};
		}
	}
	return {};
}

static bool ContainsRangePredicate(const string &sql, const string &effective_alias, const string &end_alias,
                                   const string &ts_alias) {
	string lower = StringUtil::Lower(sql);
	string effective = StringUtil::Lower(effective_alias);
	string end = StringUtil::Lower(end_alias);
	string ts = StringUtil::Lower(ts_alias);
	bool lower_bound = lower.find("(" + effective + " <= " + ts + ")") != string::npos ||
	                   lower.find("(" + ts + " >= " + effective + ")") != string::npos;
	bool upper_bound = lower.find("(" + end + " > " + ts + ")") != string::npos ||
	                   lower.find("(" + ts + " < " + end + ")") != string::npos;
	return lower_bound && upper_bound;
}

static string ApplyScd2RangeJoinAccel(const string &sql) {
	auto ctes = ParseRefreshCtes(sql);
	if (ctes.empty()) {
		return sql;
	}

	struct Injection {
		idx_t pos;
		string text;
	};
	vector<Injection> injections;
	unordered_set<idx_t> injected_ctes;

	for (auto &effective_cte : ctes) {
		for (auto &effective_alias : effective_cte.columns) {
			auto effective = ResolveRefreshColumnAlias(ctes, effective_alias);
			if (!effective.ok || effective.source_column != "effective_timestamp" ||
			    effective.relation.find("openivm_delta_") != string::npos) {
				continue;
			}
			for (auto &end_alias : ctes[effective.cte_index].columns) {
				auto end = ResolveRefreshColumnAlias(ctes, end_alias);
				if (!end.ok || end.cte_index != effective.cte_index || end.source_column != "end_timestamp") {
					continue;
				}
				for (auto &delta_cte : ctes) {
					for (auto &ts_alias : delta_cte.columns) {
						auto ts = ResolveRefreshColumnAlias(ctes, ts_alias);
						if (!ts.ok || ts.source_column != "ts" || ts.relation.find("openivm_delta_") == string::npos) {
							continue;
						}
						if (!ContainsRangePredicate(sql, effective_alias, end_alias, ts_alias)) {
							continue;
						}
						if (injected_ctes.count(effective.cte_index)) {
							continue;
						}
						string delta_body = sql.substr(ctes[ts.cte_index].body_start,
						                               ctes[ts.cte_index].body_end - ctes[ts.cte_index].body_start);
						idx_t where_pos = FindCaseInsensitive(delta_body, " WHERE ");
						if (where_pos == string::npos) {
							continue;
						}
						string delta_where = TrimCopy(delta_body.substr(where_pos + 7));
						string filter = "(" + end.source_column + " > (SELECT MIN(" + ts.source_column + ") FROM " +
						                ts.relation + " WHERE " + delta_where + ")) AND (" + effective.source_column +
						                " <= (SELECT MAX(" + ts.source_column + ") FROM " + ts.relation + " WHERE " +
						                delta_where + "))";
						injections.push_back(
						    {ctes[effective.cte_index].body_end,
						     string(ctes[effective.cte_index].has_where ? " AND " : " WHERE ") + filter});
						injected_ctes.insert(effective.cte_index);
					}
				}
			}
		}
	}

	if (injections.empty()) {
		return sql;
	}
	string result = sql;
	std::sort(injections.begin(), injections.end(),
	          [](const Injection &a, const Injection &b) { return a.pos > b.pos; });
	for (auto &injection : injections) {
		result.insert(injection.pos, injection.text);
	}
	return result;
}

static void CopyOpenIvmSetting(ClientContext &from, ClientContext &to, const string &name) {
	auto &db_config = DBConfig::GetConfig(to);
	ExtensionOption option;
	if (!db_config.TryGetExtensionOption(name, option) || !option.setting_index.IsValid()) {
		return;
	}
	Value value;
	if (from.TryGetCurrentSetting(name, value) && !value.IsNull()) {
		to.config.user_settings.SetUserSetting(option.setting_index.GetIndex(), value);
	} else {
		to.config.user_settings.ClearSetting(option.setting_index.GetIndex());
	}
}

static void PropagateRefreshPlanningSettings(ClientContext &from, ClientContext &to) {
	// Per-call compile context (target dialect, compile_only, cascade hints) is
	// carried by CompileFacts — see compile_facts.hpp. Only the remaining
	// session-scoped planning settings still need to be mirrored onto the fresh
	// planning connection.
	static const char *PLANNING_SETTINGS[] = {
	    "openivm_skip_empty_deltas",
	    "openivm_fk_pruning",
	    "openivm_ducklake_nterm",
	    "openivm_scd2_range_join_accel",
	};
	for (auto setting_name : PLANNING_SETTINGS) {
		CopyOpenIvmSetting(from, to, setting_name);
	}
}

static bool HasPendingDeltaRows(RefreshMetadata &metadata, const string &view_name, const string &delta_table,
                                const string &view_catalog_name, const string &view_schema_name) {
	string last_update = metadata.GetLastUpdate(view_name, delta_table);
	if (last_update.empty()) {
		return false;
	}
	string delta_table_sql =
	    metadata.ResolveDeltaQualifiedName(view_name, delta_table, view_catalog_name, view_schema_name);
	auto stats = metadata.GetStandardDeltaChangeStats(delta_table_sql, last_update);
	return !stats.ok || stats.total > 0;
}

static void RequireNoPendingAuxRepairDeltas(RefreshMetadata &metadata, const string &view_name,
                                            const vector<string> &delta_tables, const string &aux_table,
                                            const string &view_catalog_name, const string &view_schema_name) {
	for (auto &delta_table : delta_tables) {
		if (HasPendingDeltaRows(metadata, view_name, delta_table, view_catalog_name, view_schema_name)) {
			throw CatalogException("Cannot repair auxiliary state table '" + aux_table + "' for materialized view '" +
			                       view_name +
			                       "' while source deltas are pending. Recreate the view or restore the aux table.");
		}
	}
}

template <class BuildSQL>
static void EnsureAuxState(RefreshMetadata &metadata, Connection &con, const string &view_name, const string &aux_table,
                           const vector<string> &expected_columns, const string &internal_catalog_name,
                           const string &internal_schema_name, const vector<string> &pending_delta_tables,
                           const string &view_catalog_name, const string &view_schema_name, BuildSQL build_sql) {
	if (metadata.TableColumnsMatch(internal_catalog_name, internal_schema_name, aux_table, expected_columns)) {
		return;
	}
	RequireNoPendingAuxRepairDeltas(metadata, view_name, pending_delta_tables, aux_table, view_catalog_name,
	                                view_schema_name);
	auto result = con.Query(build_sql());
	if (result->HasError()) {
		throw CatalogException("Could not repair auxiliary state table '" + aux_table + "' for materialized view '" +
		                       view_name + "': " + result->GetError());
	}
}

static void EnsureDistinctAuxState(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const RefreshMetadata::DistinctAuxMeta &meta,
                                   const vector<string> &delta_table_names, const string &internal_catalog_name,
                                   const string &internal_schema_name, const string &catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name) {
	string delta_source = ResolveDeltaMetadataKey(meta.source, delta_table_names);
	EnsureAuxState(metadata, con, view_name, meta.aux_table, RefreshMetadata::ExpectedDistinctAuxColumns(meta),
	               internal_catalog_name, internal_schema_name, vector<string> {delta_source}, view_catalog_name,
	               view_schema_name, [&]() {
		               string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(meta.aux_table);
		               string source_table =
		                   ResolveSourceTableSQL(metadata, view_name, delta_source, meta.source, view_catalog_name,
		                                         view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		               return BuildDistinctAuxStateCreateSQL(aux_q, meta.cols, meta.source_exprs, source_table,
		                                                     meta.filter, /*replace=*/true);
	               });
}

static void EnsureCountDistinctAuxState(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                        const RefreshMetadata::CountDistinctAuxMeta &meta,
                                        const vector<string> &delta_table_names, const string &internal_catalog_name,
                                        const string &internal_schema_name, const string &catalog_prefix,
                                        const string &view_catalog_name, const string &view_schema_name,
                                        const string &attached_db_catalog_name, const string &attached_db_schema_name) {
	string delta_source = ResolveDeltaMetadataKey(meta.source, delta_table_names);
	EnsureAuxState(metadata, con, view_name, meta.aux_table, RefreshMetadata::ExpectedCountDistinctAuxColumns(meta),
	               internal_catalog_name, internal_schema_name, vector<string> {delta_source}, view_catalog_name,
	               view_schema_name, [&]() {
		               string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(meta.aux_table);
		               string source_table =
		                   ResolveSourceTableSQL(metadata, view_name, delta_source, meta.source, view_catalog_name,
		                                         view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		               return BuildCountDistinctAuxStateCreateSQL(aux_q, source_table, meta.group_cols,
		                                                          meta.group_source_exprs, meta.distinct_col,
		                                                          meta.distinct_expr, meta.filter, /*replace=*/true);
	               });
}

static void EnsureFilteredGroupCountAuxState(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                             const RefreshMetadata::FilteredGroupCountAuxMeta &meta,
                                             const vector<string> &delta_table_names,
                                             const string &internal_catalog_name, const string &internal_schema_name,
                                             const string &catalog_prefix, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name) {
	string delta_source = ResolveDeltaMetadataKey(meta.source, delta_table_names);
	EnsureAuxState(metadata, con, view_name, meta.aux_table,
	               RefreshMetadata::ExpectedFilteredGroupCountAuxColumns(meta), internal_catalog_name,
	               internal_schema_name, vector<string> {delta_source}, view_catalog_name, view_schema_name, [&]() {
		               string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(meta.aux_table);
		               string source_table =
		                   ResolveSourceTableSQL(metadata, view_name, delta_source, meta.source, view_catalog_name,
		                                         view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		               return BuildFilteredGroupCountAuxStateCreateSQL(aux_q, source_table, meta.group_col,
		                                                               meta.sum_col, meta.source_group_expr,
		                                                               meta.source_sum_expr,
		                                                               /*replace=*/true);
	               });
}

static void EnsureSemiAntiAuxState(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const RefreshMetadata::SemiAntiAuxMeta &meta,
                                   const vector<string> &delta_table_names, const string &internal_catalog_name,
                                   const string &internal_schema_name, const string &catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name) {
	string left_delta = ResolveDeltaMetadataKey(meta.left_table, delta_table_names);
	string right_delta = ResolveDeltaMetadataKey(meta.right_table, delta_table_names);
	EnsureAuxState(metadata, con, view_name, meta.aux_table, RefreshMetadata::ExpectedSemiAntiAuxColumns(meta),
	               internal_catalog_name, internal_schema_name, vector<string> {left_delta, right_delta},
	               view_catalog_name, view_schema_name, [&]() {
		               string left_source =
		                   ResolveSourceTableSQL(metadata, view_name, left_delta, meta.left_table, view_catalog_name,
		                                         view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		               string right_source =
		                   ResolveSourceTableSQL(metadata, view_name, right_delta, meta.right_table, view_catalog_name,
		                                         view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		               string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(meta.aux_table);
		               return BuildSemiAntiAuxStateCreateSQL(aux_q, left_source, meta.left_alias, right_source,
		                                                     meta.right_alias, meta.predicate, meta.post_filter,
		                                                     meta.left_cols, meta.left_exprs, /*replace=*/true);
	               });
}

} // namespace

string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                          const string &view_name, bool cross_system, const string &attached_db_catalog_name,
                          const string &attached_db_schema_name, string *out_pre_meta, string *out_post_meta,
                          RefreshCompileProfile *compile_profile, const DeltaActivityResult *precomputed_delta_activity,
                          RefreshCostEstimate *out_adaptive_estimate, const openivm::CompileFacts *facts_in) {
	// Resolve the active CompileFacts. Three sources, in priority order:
	//   1. Explicit `facts_in` (set by direct C++ callers that own a facts
	//      instance — e.g. the openivm_compile_with_facts table function
	//      passes its parsed JSON facts here in a future commit).
	//   2. The ClientContext slot installed by CompileFactsContextSlot.
	//   3. A default-constructed CompileFacts (compile_only=false,
	//      target_dialect=DUCKDB). This preserves native PRAGMA refresh
	//      behaviour for callers that never construct a slot.
	openivm::CompileFacts active_facts = facts_in ? *facts_in : openivm::CompileFactsContextSlot::Get(context);
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
	PropagateRefreshPlanningSettings(context, *con.context);
	// Mirror the active CompileFacts onto the inner connection's ClientContext
	// so the optimizer rules invoked via `Optimizer(*con.context)` below
	// (cost estimator + main IVM rewriter) see the same facts as the outer
	// caller. Without this, ClientContext::registered_state lookups in
	// `join.cpp` / `refresh_insert_rule.cpp` would silently fall back to
	// `CompileFacts::Default()` and lose `compile_only` semantics, producing
	// `SELECT NULL ... WHERE false` placeholders for views compiled with
	// no pending deltas.
	auto inner_facts_slot_facts = make_shared_ptr<openivm::CompileFacts>(active_facts);
	openivm::CompileFactsContextSlot inner_facts_slot(*con.context, inner_facts_slot_facts);
	con.Query("SET max_expression_depth = 10000");
	bool skip_empty_enabled = SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);
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
	bool emit_cascade_delta_for_recompute =
	    active_facts.force_view_delta_cascade &&
	    (view_query_type == RefreshType::WINDOW_PARTITION || view_query_type == RefreshType::GROUP_RECOMPUTE ||
	     view_query_type == RefreshType::CURRENT_DIFF_RECOMPUTE);
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
	if (source_has_left_join && !source_has_full_outer) {
		left_join_merge = SqlUtils::GetBoolSetting(context, "openivm_left_join_merge", false);
	}
	bool full_outer_merge = false;
	if (source_has_full_outer) {
		full_outer_merge = SqlUtils::GetBoolSetting(context, "openivm_full_outer_merge", false);
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
	bool metadata_requires_full_refresh =
	    precomputed_delta_activity && precomputed_delta_activity->requires_full_refresh;
	DeltaActivityResult local_delta_activity;
	const DeltaActivityResult *refresh_delta_activity = precomputed_delta_activity;
	if (!refresh_delta_activity && !force_full_refresh && view_query_type != RefreshType::FULL_REFRESH) {
		auto activity_start = profile_now();
		auto activity_provider =
		    DeltaActivityProvider::Build(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
		                                 view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		local_delta_activity = activity_provider.Summary();
		refresh_delta_activity = &local_delta_activity;
		metadata_requires_full_refresh = local_delta_activity.requires_full_refresh;
		add_profile_step(
		    "generate_refresh_sql.delta_activity", activity_start,
		    "active_sources=" + to_string(local_delta_activity.active_delta_table_names.size()) +
		        "; requires_full_refresh=" + string(local_delta_activity.requires_full_refresh ? "true" : "false"));
	}

	bool adaptive_refresh = SqlUtils::GetBoolSetting(context, "openivm_adaptive_refresh", false);
	bool adaptive_recompute = false;
	if (adaptive_refresh) {
		auto adaptive_start = profile_now();
		con.BeginTransaction();
		Parser cost_parser;
		cost_parser.ParseQuery(view_query_sql);
		Planner cost_planner(*con.context);
		cost_planner.CreatePlan(cost_parser.statements[0]->Copy());
		Optimizer cost_optimizer(*cost_planner.binder, *con.context);
		auto cost_plan = cost_optimizer.Optimize(std::move(cost_planner.plan));

		auto cost_estimate = EstimateRefreshCost(*con.context, *cost_plan, view_name, refresh_delta_activity);
		con.Rollback();
		if (out_adaptive_estimate) {
			*out_adaptive_estimate = cost_estimate;
		}
		adaptive_recompute = cost_estimate.ShouldRecompute();
		if (adaptive_recompute) {
			OPENIVM_DEBUG_PRINT("[ADAPTIVE] Full recompute is cheaper — skipping IVM\n");
		}
		add_profile_step("generate_refresh_sql.adaptive_cost", adaptive_start,
		                 adaptive_recompute ? "strategy=full" : "strategy=incremental");
	}

	RefreshPlan refresh_plan;
	refresh_plan.refresh_type = view_query_type;
	refresh_plan.force_full_refresh = force_full_refresh;
	refresh_plan.metadata_requires_full_refresh = metadata_requires_full_refresh;
	refresh_plan.adaptive_recompute = adaptive_recompute;

	if (refresh_plan.RequiresFullRecompute()) {
		auto full_refresh_start = profile_now();
		auto recompute_query =
		    BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
		                        attached_db_schema_name, internal_catalog_prefix, out_post_meta);
		add_profile_step("generate_refresh_sql.dispatch", full_refresh_start,
		                 "full_recompute=true; metadata_requires_full_refresh=" +
		                     string(metadata_requires_full_refresh ? "true" : "false") +
		                     "; adaptive_recompute=" + string(adaptive_recompute ? "true" : "false") +
		                     "; sql_bytes=" + to_string(recompute_query.size()));
		return recompute_query;
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
	string delta_ts_filter = BuildDeltaTimestampFilter(con, view_name, has_ts_col);
	bool has_left_join =
	    std::find(column_names.begin(), column_names.end(), openivm::LEFT_KEY_COL) != column_names.end();
	bool has_full_outer =
	    std::find(column_names.begin(), column_names.end(), openivm::RIGHT_KEY_COL) != column_names.end();
	OPENIVM_DEBUG_PRINT("[UPSERT] has_left_join=%d has_full_outer=%d\n", has_left_join, has_full_outer);

	auto fast_path_start = profile_now();
	auto fast_paths = ResolveDeltaFastPathFlags(
	    context, metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name, view_schema_name,
	    attached_db_catalog_name, attached_db_schema_name, cross_system, refresh_delta_activity, &active_facts);
	add_profile_step("generate_refresh_sql.delta_fast_paths", fast_path_start,
	                 "insert_only=" + string(fast_paths.insert_only ? "true" : "false") +
	                     "; skip_agg_delete=" + string(fast_paths.skip_agg_delete ? "true" : "false") +
	                     "; skip_proj_delete=" + string(fast_paths.skip_proj_delete ? "true" : "false") +
	                     "; minmax_incremental=" + string(fast_paths.minmax_incremental ? "true" : "false"));
	bool insert_only = fast_paths.insert_only;
	bool skip_agg_delete = fast_paths.skip_agg_delete;
	bool skip_proj_delete = fast_paths.skip_proj_delete;
	bool minmax_incremental = fast_paths.minmax_incremental;
	bool running_window_incremental =
	    (active_facts.running_window_incremental && active_facts.assume_insert_only) ||
	    (insert_only && SqlUtils::GetBoolSetting(context, "openivm_running_window_incremental", false));
	refresh_plan.delta_flags = fast_paths;
	auto group_cols = metadata.GetGroupColumns(view_name);
	auto agg_types = metadata.GetAggregateTypes(view_name);
	bool has_unstripped_having =
	    view_query_type == RefreshType::AGGREGATE_HAVING && metadata.GetHavingPredicate(view_name).empty();
	bool has_argminmax = std::any_of(agg_types.begin(), agg_types.end(),
	                                 [](const string &t) { return t == "arg_min" || t == "arg_max"; });

	// AGGREGATE_GROUP cascade-delta dispatch.
	//
	// When force_view_delta_cascade=true, CompileAggregateGroups's recompute
	// branch emits the cascade-delta via the LPTS ComputeDelta query plus a
	// NULL-padded retract/add-back companion. For MIN/MAX (and other
	// recompute-driving aggregates) the LPTS sum-delta produces rows with
	// stale non-key values, and the NULL companion cancels out when a
	// downstream SIMPLE_PROJECTION aggregates by (key, non-key) — so
	// downstream INNER JOINs on the non-key columns evaluate to UNKNOWN and
	// the stale row is never deleted from the downstream MV.
	//
	// Route through CompileGroupRecompute with emit_cascade_delta=true so the
	// cascade-delta is built from real openivm_old_<view> / openivm_new_<view>
	// snapshots + signed-multiset insert.
	//
	// Excluded:
	//   - source_has_full_outer — handled by BuildFullOuterAffectedGroupRefresh
	//     with its own semantics.
	//   - AGGREGATE_HAVING — CompileGroupRecompute substitutes delta-only
	//     scans into the view query and applies HAVING to partial aggregates
	//     (e.g. filters SUM(delta) > 100 instead of SUM(base + delta) > 100),
	//     missing groups whose full sum crosses the threshold via the delta.
	//     AGGREGATE_HAVING also stores hidden helper columns (openivm_count_star)
	//     in <mv>__ivm_data that the bare view_query_sql does not project.
	bool aggregate_recompute_emits_cascade_delta = false;
	vector<GroupRecomputeDeltaSpec> aggregate_recompute_delta_specs;
	string aggregate_recompute_lpts_prefix;
	const vector<GroupRecomputeDeltaSpec> *aggregate_cascade_specs_ptr = nullptr;
	{
		bool force_view_delta_cascade = active_facts.force_view_delta_cascade;
		bool eligible_refresh_type = (view_query_type == RefreshType::AGGREGATE_GROUP);
		if (force_view_delta_cascade && eligible_refresh_type && !source_has_full_outer && !group_cols.empty()) {
			auto active_delta_table_names = fast_paths.active_delta_table_names;
			if (active_delta_table_names.empty() && active_facts.compile_only) {
				active_delta_table_names = delta_table_names;
			}
			if (!active_delta_table_names.empty()) {
				bool has_ducklake_source = false;
				for (auto &dt : active_delta_table_names) {
					if (metadata.IsDuckLakeTable(view_name, dt)) {
						has_ducklake_source = true;
						break;
					}
				}
				string dl_cat;
				if (has_ducklake_source) {
					dl_cat = ResolveDuckLakeCatalogName(con, view_catalog_name, attached_db_catalog_name);
				}
				string dl_sch = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
				aggregate_recompute_delta_specs =
				    BuildGroupRecomputeDeltaSpecs(metadata, view_name, con, active_delta_table_names, dl_cat, dl_sch);
				string lpts_cat = view_catalog_name.empty() ? "memory" : view_catalog_name;
				string lpts_sch = view_schema_name.empty() ? "main" : view_schema_name;
				aggregate_recompute_lpts_prefix = SqlUtils::QualifiedPrefix(lpts_cat, lpts_sch);
				if (!aggregate_recompute_delta_specs.empty()) {
					aggregate_cascade_specs_ptr = &aggregate_recompute_delta_specs;
				}
			}
		}
	}

	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: %s\n", RefreshTypeName(view_query_type));
	auto dispatch_start = profile_now();
	switch (view_query_type) {
	case RefreshType::AGGREGATE_HAVING: {
		bool having_merge = SqlUtils::GetBoolSetting(context, "openivm_having_merge", true);
		if (having_merge) {
			bool effective_insert_only =
			    has_argminmax ? false : (has_minmax ? (insert_only && minmax_incremental) : skip_agg_delete);
			upsert_query =
			    CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql,
			                           has_minmax, list_mode, delta_ts_filter, group_cols, internal_catalog_prefix,
			                           effective_insert_only, agg_types, column_types, has_unstripped_having);
		} else {
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql,
			    /*has_minmax=*/true, list_mode, delta_ts_filter, group_cols, internal_catalog_prefix,
			    /*insert_only=*/false, agg_types, column_types, has_unstripped_having);
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
			bool effective_insert_only =
			    has_argminmax ? false : (has_minmax ? (insert_only && minmax_incremental) : skip_agg_delete);
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, has_minmax, list_mode,
			    delta_ts_filter, group_cols, internal_catalog_prefix, effective_insert_only, agg_types, column_types,
			    /*use_current_diff_affected_keys=*/false, aggregate_cascade_specs_ptr, aggregate_recompute_lpts_prefix,
			    /*emit_cascade_delta=*/aggregate_cascade_specs_ptr != nullptr,
			    /*inline_cascade_delta=*/active_facts.force_view_delta_cascade,
			    &aggregate_recompute_emits_cascade_delta);
		}
		break;
	}
	case RefreshType::SIMPLE_PROJECTION: {
		if (!has_full_outer && !has_left_join &&
		    TryBuildDuckLakeProjectionKeyRefresh(metadata, con, view_name, delta_table_names, data_table,
		                                         view_query_sql, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name, upsert_query)) {
			refresh_plan.skip_projection_key_delta = true;
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
			if (!active_facts.compile_only) {
				EnsureFilteredGroupCountAuxState(metadata, con, view_name, aux_meta, delta_table_names,
				                                 internal_catalog_name, internal_schema_name, internal_catalog_prefix,
				                                 view_catalog_name, view_schema_name, attached_db_catalog_name,
				                                 attached_db_schema_name);
			}
			string delta_source = ResolveDeltaMetadataKey(aux_meta.source, delta_table_names);
			string delta_source_sql =
			    metadata.ResolveDeltaQualifiedName(view_name, delta_source, view_catalog_name, view_schema_name);
			string ts = metadata.GetLastUpdate(view_name, delta_source);
			upsert_query = CompileFilteredGroupCount(
			    view_name, aux_meta.aux_table, delta_source_sql, ts, aux_meta.group_col, aux_meta.sum_col,
			    aux_meta.source_group_expr, aux_meta.source_sum_expr, aux_meta.output_col, aux_meta.comparison_op,
			    aux_meta.threshold_sql, internal_catalog_prefix);
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
		upsert_query = BuildWindowPartitionRefresh(
		    metadata, con, view_name, view_query_sql, delta_table_names, column_names, data_table, delta_ts_filter,
		    internal_catalog_prefix, view_catalog_name, view_schema_name, attached_db_catalog_name,
		    attached_db_schema_name, cross_system, emit_cascade_delta_for_recompute, running_window_incremental);
		break;
	}
	case RefreshType::COUNT_DISTINCT_INCREMENTAL: {
		RefreshMetadata::CountDistinctAuxMeta aux_meta;
		if (!metadata.GetCountDistinctAuxMeta(view_name, aux_meta)) {
			throw InternalException("COUNT_DISTINCT_INCREMENTAL view '%s' has no aux metadata", view_name);
		} else {
			if (!active_facts.compile_only) {
				EnsureCountDistinctAuxState(metadata, con, view_name, aux_meta, delta_table_names,
				                            internal_catalog_name, internal_schema_name, internal_catalog_prefix,
				                            view_catalog_name, view_schema_name, attached_db_catalog_name,
				                            attached_db_schema_name);
			}
			string delta_source = ResolveDeltaMetadataKey(aux_meta.source, delta_table_names);
			string delta_source_sql =
			    metadata.ResolveDeltaQualifiedName(view_name, delta_source, view_catalog_name, view_schema_name);
			string ts = metadata.GetLastUpdate(view_name, delta_source);
			string count_star_col;
			if (std::find(column_names.begin(), column_names.end(), string(openivm::COUNT_STAR_COL)) !=
			    column_names.end()) {
				count_star_col = string(openivm::COUNT_STAR_COL);
			}
			upsert_query = CompileCountDistinctIncremental(
			    view_name, aux_meta.aux_table, delta_source_sql, ts, aux_meta.group_cols, aux_meta.group_source_exprs,
			    aux_meta.distinct_col, aux_meta.distinct_expr, aux_meta.output_col, count_star_col, aux_meta.filter,
			    internal_catalog_prefix);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: COUNT_DISTINCT_INCREMENTAL (%zu group cols, "
			                    "distinct=%s, out=%s)\n",
			                    aux_meta.group_cols.size(), aux_meta.distinct_expr.c_str(),
			                    aux_meta.output_col.c_str());
			break;
		}
	}
	case RefreshType::DISTINCT_INCREMENTAL: {
		RefreshMetadata::DistinctAuxMeta aux_meta;
		if (metadata.GetDistinctAuxMeta(view_name, aux_meta)) {
			if (!active_facts.compile_only) {
				EnsureDistinctAuxState(metadata, con, view_name, aux_meta, delta_table_names, internal_catalog_name,
				                       internal_schema_name, internal_catalog_prefix, view_catalog_name,
				                       view_schema_name, attached_db_catalog_name, attached_db_schema_name);
			}
			auto group_columns = metadata.GetGroupColumns(view_name);
			string delta_source = ResolveDeltaMetadataKey(aux_meta.source, delta_table_names);
			string delta_source_sql =
			    metadata.ResolveDeltaQualifiedName(view_name, delta_source, view_catalog_name, view_schema_name);
			string ts = metadata.GetLastUpdate(view_name, delta_source);
			upsert_query =
			    CompileDistinctIncremental(view_name, aux_meta.aux_table, aux_meta.cols, aux_meta.source_exprs,
			                               delta_source_sql, ts, aux_meta.filter, group_columns, aux_meta.sum_arg,
			                               aux_meta.sum_out, string(openivm::COUNT_STAR_COL), internal_catalog_prefix);
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
			if (!active_facts.compile_only) {
				EnsureSemiAntiAuxState(metadata, con, view_name, aux_meta, delta_table_names, internal_catalog_name,
				                       internal_schema_name, internal_catalog_prefix, view_catalog_name,
				                       view_schema_name, attached_db_catalog_name, attached_db_schema_name);
			}
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
		auto active_delta_table_names = refresh_plan.delta_flags.active_delta_table_names;
		bool compile_only_active = active_facts.compile_only;
		if (active_delta_table_names.empty()) {
			if (!compile_only_active) {
				upsert_query = "";
				OPENIVM_DEBUG_PRINT("[UPSERT] GROUP_RECOMPUTE has no active deltas after filtering\n");
				break;
			}
			// Compile-only callers receive the refresh SQL template and
			// re-run it against their own deltas downstream, so the
			// empty-active-deltas short-circuit (which only applies when
			// openivm itself executes the refresh) is bypassed here.
			active_delta_table_names = delta_table_names;
		}
		if (TryBuildGroupMeasureUpdateRefresh(metadata, con, view_name, view_query_sql, active_delta_table_names,
		                                      column_names, column_types, data_table, view_catalog_name,
		                                      view_schema_name, upsert_query)) {
			break;
		}
		bool group_recompute_has_ducklake_source = false;
		for (auto &dt : active_delta_table_names) {
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
		auto delta_specs = BuildGroupRecomputeDeltaSpecs(metadata, view_name, con, active_delta_table_names,
		                                                 recompute_ducklake_catalog, recompute_ducklake_schema);
		ApplyGroupRecomputeSourceOccurrences(delta_specs, metadata.GetGroupRecomputeSourceOccurrences(view_name));
		auto affected_mode = metadata.GetGroupRecomputeAffectedMode(view_name);
		string lpts_cat = view_catalog_name.empty() ? "memory" : view_catalog_name;
		string lpts_sch = view_schema_name.empty() ? "main" : view_schema_name;
		string lpts_table_prefix = SqlUtils::QualifiedPrefix(lpts_cat, lpts_sch);
		upsert_query =
		    CompileGroupRecompute(view_name, view_query_sql, group_columns, delta_specs, internal_catalog_prefix,
		                          lpts_table_prefix, emit_cascade_delta_for_recompute, affected_mode);
		OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: GROUP_RECOMPUTE (%zu group cols, %zu sources, "
		                    "affected_mode=%s)\n",
		                    group_columns.size(), delta_specs.size(), GroupRecomputeAffectedModeName(affected_mode));
		break;
	}
	case RefreshType::CURRENT_DIFF_RECOMPUTE: {
		if (skip_empty_enabled && refresh_plan.delta_flags.active_delta_table_names.empty() &&
		    !active_facts.compile_only) {
			upsert_query = "";
			OPENIVM_DEBUG_PRINT("[UPSERT] CURRENT_DIFF_RECOMPUTE has no active deltas after filtering\n");
			break;
		}
		upsert_query = CompileFullRecompute(view_name, view_query_sql, internal_catalog_prefix);
		OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: CURRENT_DIFF_RECOMPUTE\n");
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
	// force_view_delta_cascade biases has_downstream=true for AGGREGATE_GROUP
	// and AGGREGATE_HAVING so the per-key retract companion is emitted even
	// when no downstream MV is currently registered in openivm_delta_tables.
	//
	// Scope: AGGREGATE_GROUP and AGGREGATE_HAVING only. The SIMPLE_AGGREGATE
	// snapshot companion relies on CREATE TEMP TABLE / DROP TABLE pre/post
	// pairs that not all dialects can carry across statement boundaries.
	// WINDOW_PARTITION and GROUP_RECOMPUTE go through the
	// emit_cascade_delta_for_recompute path below.
	{
		if (active_facts.force_view_delta_cascade &&
		    (view_query_type == RefreshType::AGGREGATE_GROUP || view_query_type == RefreshType::AGGREGATE_HAVING)) {
			has_downstream = true;
		}
	}
	// emit_cascade_delta_for_recompute: WINDOW_PARTITION / GROUP_RECOMPUTE
	// compile their own signed-multiset view-delta into openivm_delta_<view>.
	// Treat the MV as cascade-capable so cleanup retains the emitted rows even
	// in compile-for-cascade sessions with no registered downstream metadata.
	if (emit_cascade_delta_for_recompute) {
		has_downstream = true;
	}
	bool recompute_handles_own_cascade_delta =
	    aggregate_recompute_emits_cascade_delta ||
	    (emit_cascade_delta_for_recompute &&
	     (view_query_type == RefreshType::WINDOW_PARTITION || view_query_type == RefreshType::GROUP_RECOMPUTE));
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
	auto build_affected_snapshot_companion = [&](const vector<string> &keys) {
		string col_list = SqlUtils::JoinQuotedColumns(column_names);
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
				select_old += "openivm_old." + SqlUtils::QuoteIdentifier(col);
				select_new += "openivm_new." + SqlUtils::QuoteIdentifier(col);
			}
		}

		string qdvn = delta_view_name.find('.') == string::npos ? KeywordHelper::WriteOptionallyQuoted(delta_view_name)
		                                                        : delta_view_name;
		string affected_temp =
		    KeywordHelper::WriteOptionallyQuoted(string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name);
		string old_temp =
		    KeywordHelper::WriteOptionallyQuoted(string(openivm::TEMP_TABLE_PREFIX) + "snapshot_" + view_name);
		string key_cols = SqlUtils::JoinQuotedColumns(keys);
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		string target_match = SqlUtils::BuildNullSafeMatch(keys, "openivm_aff", "openivm_data");
		string new_match = SqlUtils::BuildNullSafeMatch(keys, "openivm_aff", "openivm_new");

		companion_query = "CREATE TEMP TABLE " + affected_temp + " AS SELECT DISTINCT " + key_cols + " FROM " + qdvn +
		                  delta_where + ";\n";
		companion_query += "CREATE TEMP TABLE " + old_temp + " AS SELECT openivm_data.* FROM " + data_table +
		                   " openivm_data WHERE EXISTS (SELECT 1 FROM " + affected_temp + " openivm_aff WHERE " +
		                   target_match + ");\n";

		post_companion = "DELETE FROM " + qdvn + " WHERE 1=1";
		if (!delta_ts_filter.empty()) {
			post_companion += " AND " + delta_ts_filter;
		}
		post_companion += ";\n";
		post_companion += "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_old + " FROM " + old_temp +
		                  " openivm_old;\n";
		post_companion += "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_new + " FROM " + data_table +
		                  " openivm_new WHERE EXISTS (SELECT 1 FROM " + affected_temp + " openivm_aff WHERE " +
		                  new_match + ");\n";
		post_companion += "DROP TABLE " + old_temp + ";\nDROP TABLE " + affected_temp + ";\n";
		OPENIVM_DEBUG_PRINT("[UPSERT] Affected companion query:\n%s\n", companion_query.c_str());
		OPENIVM_DEBUG_PRINT("[UPSERT] Affected post-companion:\n%s\n", post_companion.c_str());
	};

	if (refresh_plan.SkipsDeltaProduction() || aggregate_recompute_emits_cascade_delta) {
		OPENIVM_DEBUG_PRINT("[UPSERT] Skipping ComputeDelta for %s\n", aggregate_recompute_emits_cascade_delta
		                                                                   ? "AGGREGATE_GROUP_RECOMPUTE_CASCADE"
		                                                                   : refresh_plan.DeltaProductionSkipReason());
		delta_query = "";
		if (has_downstream && !recompute_handles_own_cascade_delta) {
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
		ScopedDisabledOptimizers disabled_optimizers(con_ctx, string(openivm::DISABLED_OPTIMIZERS) + ", deliminator");
		Optimizer optimizer(*planner.binder, con_ctx);
		plan = optimizer.Optimize(std::move(plan)); // this transforms the plan into an incremental plan
		OPENIVM_DEBUG_PRINT("[UPSERT] Optimizer done.\n");
		con.Rollback();
		add_profile_step("generate_refresh_sql.compute_delta_plan", compute_delta_plan_start);
		string raw_refresh_sql;
		if (IsEmptyDeltaPlan(plan.get())) {
			raw_refresh_sql = BuildEmptyDeltaInsert(view_name, column_names, column_types, active_facts.target_dialect);
			OPENIVM_DEBUG_PRINT("[UPSERT] Delta plan is empty; generated no-op delta insert for '%s'\n",
			                    view_name.c_str());
		} else {
			try {
				auto lpts_start = profile_now();
				SqlDialect dialect = active_facts.target_dialect;
				auto ast = LogicalPlanToAst(con_ctx, plan, dialect);
				bool emit_spark_hints = dialect == SqlDialect::SPARK &&
				                        (active_facts.emit_spark_hints ||
				                         SqlUtils::GetBoolSetting(con_ctx, "openivm_emit_spark_hints", false));
				auto cte_list = AstToCteList(*ast, dialect, emit_spark_hints);
				raw_refresh_sql = cte_list->ToQuery(false);
				if (active_facts.scd2_range_join_accel ||
				    SqlUtils::GetBoolSetting(context, "openivm_scd2_range_join_accel", false)) {
					raw_refresh_sql = ApplyScd2RangeJoinAccel(raw_refresh_sql);
				}
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

			// Dispatch on force_view_delta_cascade:
			//   false (default): build_affected_snapshot_companion(keys)
			//     uses TEMP TABLEs to snapshot the affected groups and
			//     emits signed (-1 old, +1 new) rows. Correctness path for
			//     downstream MVs that consume the cascade delta in the same
			//     session.
			//   true: emit inline per-key NULL retract/add-back rows. For
			//     callers whose downstream executor splits the refresh
			//     program statement-by-statement and recreates each delta
			//     table fresh per refresh, so the TEMP TABLE scratch does
			//     not survive across the split. NULLs in non-key columns
			//     rely on SUM(NULL)=NULL preservation downstream.
			bool inline_companion = active_facts.force_view_delta_cascade;

			if (!inline_companion) {
				build_affected_snapshot_companion(keys);
			} else {
				string col_list, val_list_neg, val_list_pos;
				bool has_count_star_col = false;
				for (auto &col : column_names) {
					if (!col_list.empty()) {
						col_list += ", ";
						val_list_neg += ", ";
						val_list_pos += ", ";
					}
					col_list += col;
					if (col == string(openivm::COUNT_STAR_COL)) {
						has_count_star_col = true;
					}
					if (keys_set.count(col)) {
						val_list_neg += "d." + col;
						val_list_pos += "d." + col;
					} else if (col == openivm::MULTIPLICITY_COL) {
						val_list_neg += "-1";
						val_list_pos += "1";
					} else {
						val_list_neg += "NULL";
						val_list_pos += "NULL";
					}
				}
				string join_cond = SqlUtils::BuildNullSafeMatch(keys, "d", "m");

				companion_query = "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + val_list_neg +
				                  " FROM " + delta_view_name + " d WHERE d." + string(openivm::MULTIPLICITY_COL) +
				                  " > 0";
				if (!delta_ts_filter.empty()) {
					companion_query += " AND d." + delta_ts_filter;
				}
				companion_query += " AND EXISTS (SELECT 1 FROM " + data_table + " m WHERE " + join_cond + ");\n";

				if (has_count_star_col) {
					companion_query += "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + val_list_pos +
					                   " FROM " + delta_view_name + " d WHERE d." + string(openivm::MULTIPLICITY_COL) +
					                   " < 0 AND d." + string(openivm::COUNT_STAR_COL) + " > 0";
					if (!delta_ts_filter.empty()) {
						companion_query += " AND d." + delta_ts_filter;
					}
					companion_query += " AND EXISTS (SELECT 1 FROM " + data_table + " m WHERE " + join_cond +
					                   " AND m." + string(openivm::COUNT_STAR_COL) + " + d." +
					                   string(openivm::MULTIPLICITY_COL) + " * d." + string(openivm::COUNT_STAR_COL) +
					                   " > 0);\n";
				}
				OPENIVM_DEBUG_PRINT("[UPSERT] Companion query:\n%s\n", companion_query.c_str());
			}
		}
	}

	auto assembly_start = profile_now();
	if (has_downstream) {
		if (skip_empty_enabled && !recompute_handles_own_cascade_delta) {
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
			continue;
		}
		string resolved = metadata.ResolveDeltaQualifiedName(view_name, dt, view_catalog_name, view_schema_name);
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
			snapshot_update_query += RefreshMetadata::BuildDuckLakeRefreshMetadataSQL(view_name, dt, dl_snapshot_expr);
		}
	}

	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		if (cross_system) {
			continue;
		}
		auto resolved = metadata.ResolveDeltaQualifiedName(view_name, dt, view_catalog_name, view_schema_name);
		delete_from_delta_table_query += RefreshMetadata::BuildDeltaCleanupSQL(resolved, dt);
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
	if (active_facts.target_dialect == SqlDialect::SPARK) {
		data_sql = SparkPortableRefreshSQL(data_sql);
		meta_post_sql = SparkPortableRefreshSQL(meta_post_sql);
	}

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
