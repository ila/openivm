#ifndef OPENIVM_INDEX_REGEN_HPP
#define OPENIVM_INDEX_REGEN_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

using old_idx = idx_t;
using new_idx = idx_t;
using col_idx = idx_t;

struct RenumberWrapper {
	unique_ptr<LogicalOperator> op;
	std::unordered_map<old_idx, new_idx> idx_map;
	std::vector<ColumnBinding> column_bindings;
};

RenumberWrapper renumber_table_indices(unique_ptr<LogicalOperator> plan, Binder &binder);
RenumberWrapper renumber_and_rebind_subtree(unique_ptr<LogicalOperator> plan, Binder &binder);
ColumnBindingReplacer vec_to_replacer(const std::vector<ColumnBinding> &bindings,
                                      const std::unordered_map<old_idx, new_idx> &table_mapping);

} // namespace duckdb

#endif // OPENIVM_INDEX_REGEN_HPP
