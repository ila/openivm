#ifndef REFRESH_INDEX_REGEN_HPP
#define REFRESH_INDEX_REGEN_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

using old_idx = TableIndex;
using new_idx = TableIndex;
using col_idx = ProjectionIndex;

struct RenumberWrapper {
	unique_ptr<LogicalOperator> op;
	std::unordered_map<old_idx, new_idx> idx_map;
	std::vector<ColumnBinding> column_bindings;
	bool contains_delim_operator;
};

RenumberWrapper renumber_and_rebind_subtree(unique_ptr<LogicalOperator> plan, Binder &binder);

} // namespace duckdb

#endif // REFRESH_INDEX_REGEN_HPP
