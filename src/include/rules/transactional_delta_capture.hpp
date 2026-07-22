#ifndef TRANSACTIONAL_DELTA_CAPTURE_HPP
#define TRANSACTIONAL_DELTA_CAPTURE_HPP

#include "duckdb/planner/operator/logical_extension_operator.hpp"

namespace duckdb {

class TableCatalogEntry;

enum class DeltaCaptureMode : uint8_t { INSERT, DELETE, UPDATE };

// Streaming plan operator that writes the rows affected by a base-table DML statement
// to its delta table through the caller's transaction before passing the input onward.
class LogicalTransactionalDeltaCapture : public LogicalExtensionOperator {
public:
	LogicalTransactionalDeltaCapture(TableCatalogEntry &base_table, TableCatalogEntry &delta_table,
	                                 DeltaCaptureMode mode, vector<unique_ptr<Expression>> update_expressions = {},
	                                 vector<PhysicalIndex> update_columns = {}, optional_idx row_id_index = {});

	TableCatalogEntry &base_table;
	TableCatalogEntry &delta_table;
	DeltaCaptureMode mode;
	vector<PhysicalIndex> update_columns;
	optional_idx row_id_index;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override;
	vector<ColumnBinding> GetColumnBindings() override;
	string GetName() const override;
	string GetExtensionName() const override;
	bool SupportSerialization() const override {
		return false;
	}

protected:
	void ResolveTypes() override;
};

} // namespace duckdb

#endif // TRANSACTIONAL_DELTA_CAPTURE_HPP
