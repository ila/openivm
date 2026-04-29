#include "match/plan_canonical.hpp"

#include "duckdb/common/types/hash.hpp"

namespace duckdb {
namespace ivm {

PlanCanonicalizer::PlanCanonicalizer(ClientContext &context) : context_(context) {
}

CanonicalPlanResult PlanCanonicalizer::Canonicalize(const LogicalOperator &plan) {
	(void)plan;
	// TODO: see Normalize() for the pass list; serialize the normalized tree
	// to result.canonical_blob and hash it via HashCanonical().
	return CanonicalPlanResult();
}

hash_t PlanCanonicalizer::HashCanonical(const vector<data_t> &blob) {
	if (blob.empty()) {
		return 0;
	}
	return Hash(reinterpret_cast<const char *>(blob.data()), blob.size());
}

unique_ptr<LogicalOperator> PlanCanonicalizer::Normalize(unique_ptr<LogicalOperator> plan) {
	// TODO:
	//   1. CNF predicate normalization (FilterCombiner)
	//   2. EC build from join equalities
	//   3. Inner-join commutative reorder by smallest-leaf-id
	//   4. GROUP BY key sort + projection sort
	//   5. CSE via cse_optimizer
	return plan;
}

} // namespace ivm
} // namespace duckdb
