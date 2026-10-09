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

// Transparent wrapper around a LogicalMergeInto. At physical-plan creation it decorates
// DuckDB's resolved action sinks, so only rows that actually INSERT/UPDATE/DELETE are captured.
class LogicalTransactionalMergeDeltaCapture : public LogicalExtensionOperator {
public:
	LogicalTransactionalMergeDeltaCapture(TableCatalogEntry &base_table, TableCatalogEntry &delta_table);

	TableCatalogEntry &base_table;
	TableCatalogEntry &delta_table;

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

// Whether a physical plan writes a tracked base table through OpenIVM delta capture.
// Cached plans (prepared statements) execute without the optimizer rule that inserts it.
bool PlanCapturesTrackedDeltas(PhysicalOperator &op);

} // namespace duckdb

#endif // TRANSACTIONAL_DELTA_CAPTURE_HPP
