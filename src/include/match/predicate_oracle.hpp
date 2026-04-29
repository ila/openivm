// Predicate implication oracle.
//
// Default mode 'interval' covers Goldstein-Larson range subsumption (<, <=,
// >, >=, BETWEEN, IN on numeric/date/timestamp). Mode 'sat' is a stub for
// future SAT/SMT integration. Mode selected via the `ivm_predicate_oracle`
// setting.
//
// Returns a residual predicate when implication holds with extra view rows;
// callers apply it on top of the MV scan to filter view-side rows the query
// doesn't want.

#ifndef OPENIVM_MATCH_PREDICATE_ORACLE_HPP
#define OPENIVM_MATCH_PREDICATE_ORACLE_HPP

#include "duckdb.hpp"
#include "duckdb/planner/expression.hpp"
#include "match/equivalence_classes.hpp"

namespace duckdb {
namespace ivm {

enum class ImplicationResult : uint8_t {
	IMPLIED,          // p_q → p_v with no extra rows
	IMPLIED_RESIDUAL, // p_q → p_v but view has extra rows; residual filters them
	NOT_IMPLIED,
	UNDECIDED // oracle gave up; treat as NOT_IMPLIED conservatively
};

struct ImplicationCheck {
	ImplicationResult result;
	unique_ptr<Expression> residual_predicate;
};

class PredicateOracle {
public:
	explicit PredicateOracle(ClientContext &context);

	ImplicationCheck Check(const vector<reference<Expression>> &query_preds,
	                       const vector<reference<Expression>> &view_preds, const EquivalenceClassMap &eq_classes);

private:
	ClientContext &context_;
	// Mode read from `ivm_predicate_oracle`: 'syntactic' | 'interval'
	// (default) | 'sat' (stub).
	string oracle_mode_;
};

} // namespace ivm
} // namespace duckdb

#endif
