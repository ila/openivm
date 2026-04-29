// Plan canonicalization for view matching.
//
// Walks a bound LogicalOperator tree, applies semantic normalizations
// (predicate CNF, equivalence-class rewrites, inner-join commutative reorder
// by smallest-leaf-id, GROUP BY key sort, projection sort, CSE), and
// produces a 64-bit signature hash + a serialized canonical blob.
//
// Outer joins are NOT freely reordered — only under Galindo-Legaria/
// Rosenthal TODS'97 null-rejection conditions.

#ifndef OPENIVM_MATCH_PLAN_CANONICAL_HPP
#define OPENIVM_MATCH_PLAN_CANONICAL_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "match/equivalence_classes.hpp"

namespace duckdb {
namespace ivm {

struct CanonicalPlanResult {
	hash_t signature_hash = 0;
	vector<data_t> canonical_blob;
	EquivalenceClassMap eq_classes;
};

class PlanCanonicalizer {
public:
	explicit PlanCanonicalizer(ClientContext &context);

	CanonicalPlanResult Canonicalize(const LogicalOperator &plan);

	// Hash a previously-serialized canonical blob.
	static hash_t HashCanonical(const vector<data_t> &blob);

private:
	ClientContext &context_;

	unique_ptr<LogicalOperator> Normalize(unique_ptr<LogicalOperator> plan);
};

} // namespace ivm
} // namespace duckdb

#endif
