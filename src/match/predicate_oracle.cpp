#include "match/predicate_oracle.hpp"

#include "duckdb/main/config.hpp"

namespace duckdb {
namespace ivm {

PredicateOracle::PredicateOracle(ClientContext &context) : context_(context), oracle_mode_("interval") {
	Value v;
	if (context.TryGetCurrentSetting("ivm_predicate_oracle", v)) {
		auto s = v.ToString();
		if (s == "syntactic" || s == "interval" || s == "sat") {
			oracle_mode_ = s;
		}
	}
}

ImplicationCheck PredicateOracle::Check(const vector<reference<Expression>> &query_preds,
                                        const vector<reference<Expression>> &view_preds,
                                        const EquivalenceClassMap &eq_classes) {
	(void)query_preds;
	(void)view_preds;
	(void)eq_classes;
	// TODO:
	//   syntactic: pairwise equality on canonicalized form.
	//   interval: FilterCombiner + StatisticsPropagator-backed range
	//             arithmetic with EC-aware column rewrites; IN-list
	//             subsumption; residual predicate derivation.
	//   sat:      stub — fall through to NOT_IMPLIED.
	return ImplicationCheck {ImplicationResult::UNDECIDED, nullptr};
}

} // namespace ivm
} // namespace duckdb
