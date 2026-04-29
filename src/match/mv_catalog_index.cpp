#include "match/mv_catalog_index.hpp"

namespace duckdb {
namespace ivm {

vector<MVCandidate> MVCatalogIndex::CandidatesForQuery(const vector<string> &query_source_tables,
                                                       const vector<string> &query_group_columns,
                                                       const vector<string> &query_output_columns) {
	(void)query_source_tables;
	(void)query_group_columns;
	(void)query_output_columns;
	// TODO: when dirty_, rebuild the filter tree from `_duckdb_ivm_views`;
	// descend by (source-table bitmap → group cols → output cols) to a small
	// leaf set; populate pending_row_estimate from `_duckdb_ivm_delta_tables`
	// (refreshed if older than `ivm_match_estimate_ttl_ms`).
	return {};
}

void MVCatalogIndex::OnViewCreated(const string &view_name) {
	(void)view_name;
	dirty_ = true;
}

void MVCatalogIndex::OnViewDropped(const string &view_name) {
	(void)view_name;
	dirty_ = true;
}

void MVCatalogIndex::OnViewRefreshed(const string &view_name) {
	(void)view_name;
	// pending_row_estimate changes; tree structure unchanged.
}

} // namespace ivm
} // namespace duckdb
