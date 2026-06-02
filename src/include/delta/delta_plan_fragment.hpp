#ifndef OPENIVM_DELTA_PLAN_FRAGMENT_HPP
#define OPENIVM_DELTA_PLAN_FRAGMENT_HPP

#include "duckdb.hpp"

namespace duckdb {

struct DeltaPlanFragment {
	DeltaPlanFragment(unique_ptr<LogicalOperator> op_, ColumnBinding mul_binding_)
	    : op(std::move(op_)), mul_binding(mul_binding_) {
	}

	unique_ptr<LogicalOperator> op;
	ColumnBinding mul_binding;
};

} // namespace duckdb

#endif // OPENIVM_DELTA_PLAN_FRAGMENT_HPP
