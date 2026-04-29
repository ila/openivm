#include "match/equivalence_classes.hpp"

namespace duckdb {
namespace ivm {

EquivalenceClassMap EquivalenceClassMap::FromEqualities(const vector<reference<Expression>> &equalities) {
	(void)equalities;
	// TODO: walk equalities; for each (a = b) union ECs of a and b; for each
	// (a = literal) attach constant to a's EC.
	return EquivalenceClassMap();
}

optional_idx EquivalenceClassMap::FindClass(const ColumnBinding &binding) const {
	auto it = class_id_.find(binding);
	if (it == class_id_.end()) {
		return optional_idx();
	}
	return optional_idx(it->second);
}

bool EquivalenceClassMap::IsCompatibleWith(const EquivalenceClassMap &other) const {
	(void)other;
	// TODO: subset/equality check on class partitions plus constant
	// compatibility.
	return false;
}

optional_ptr<const Value> EquivalenceClassMap::GetConstantForClass(idx_t class_id) const {
	if (class_id >= constants_.size() || !constants_[class_id]) {
		return nullptr;
	}
	return constants_[class_id].get();
}

} // namespace ivm
} // namespace duckdb
