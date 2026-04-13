#ifndef DUCKLAKE_JOIN_HPP
#define DUCKLAKE_JOIN_HPP

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct PlanWrapper;
struct JoinLeafInfo;

/// Build N join delta terms using DuckLake time-travel (AT VERSION).
///
/// Instead of inclusion-exclusion (2^N - 1 terms), produces exactly N terms
/// via the telescoping delta product:
///   Term i: ΔTᵢ ⋈ (T₁_new ⋈ ... ⋈ Tᵢ₋₁_new ⋈ Tᵢ₊₁_old ⋈ ... ⋈ Tₙ_old)
///
/// Tables before index i read current state; tables after index i read old
/// state via AT VERSION => last_snapshot_id. This captures all cross-terms
/// and is provably equivalent to inclusion-exclusion.
vector<unique_ptr<LogicalOperator>> BuildDuckLakeJoinTerms(PlanWrapper &pw, ClientContext &context, Binder &binder,
                                                           const vector<JoinLeafInfo> &leaves, bool has_left_join);

} // namespace duckdb

#endif // DUCKLAKE_JOIN_HPP
