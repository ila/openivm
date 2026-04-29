// MV catalog index for fast view-matching candidate lookup.
//
// Goldstein-Larson §3.1 filter-tree implementation. Indexes registered MVs by
// (source-table set, group-by columns, output columns) so the matcher can
// prune the candidate set before doing full subsumption.
//
// Backed by `_duckdb_ivm_views` (signature_hash, source_tables_json,
// output_columns_json) plus `_duckdb_ivm_delta_tables.pending_row_estimate`
// for freshness-aware ranking.
//
// Built lazily on first query; maintained on CREATE / DROP / REPLACE /
// refresh events.

#ifndef OPENIVM_MATCH_MV_CATALOG_INDEX_HPP
#define OPENIVM_MATCH_MV_CATALOG_INDEX_HPP

#include "duckdb.hpp"

namespace duckdb {
namespace ivm {

struct MVCandidate {
	string view_name;
	hash_t signature_hash = 0;
	vector<string> source_tables;
	timestamp_t last_refresh {};
	int64_t pending_row_estimate = 0;
};

class MVCatalogIndex {
public:
	MVCatalogIndex() = default;

	vector<MVCandidate> CandidatesForQuery(const vector<string> &query_source_tables,
	                                       const vector<string> &query_group_columns,
	                                       const vector<string> &query_output_columns);

	void OnViewCreated(const string &view_name);
	void OnViewDropped(const string &view_name);
	void OnViewRefreshed(const string &view_name);

private:
	bool dirty_ = true;
};

} // namespace ivm
} // namespace duckdb

#endif
