#include "rules/transactional_delta_capture.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

namespace {

static constexpr const char *DELTA_WRITE_STATE_KEY = "openivm_transactional_delta_write";

class TransactionalDeltaWriteState : public ClientContextState {
public:
	void Acquire(const string &catalog_name) {
		lock_guard<mutex> guard(lock);
		if (catalog_guard) {
			if (catalog_name != locked_catalog) {
				throw InternalException("OpenIVM delta transaction attempted to modify multiple catalogs");
			}
			return;
		}
		catalog_guard = make_uniq<DeltaCatalogLockGuard>(catalog_name);
		locked_catalog = catalog_name;
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		Release();
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		Release();
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context,
	                         optional_ptr<ErrorData> error) override {
		Release();
	}

private:
	void Release() {
		lock_guard<mutex> guard(lock);
		catalog_guard.reset();
		locked_catalog.clear();
	}

	mutex lock;
	string locked_catalog;
	unique_ptr<DeltaCatalogLockGuard> catalog_guard;
};

static void AcquireDeltaWriteLock(ClientContext &context, const string &catalog_name) {
	auto state = context.registered_state->GetOrCreate<TransactionalDeltaWriteState>(DELTA_WRITE_STATE_KEY);
	state->Acquire(catalog_name);
}

class TransactionalDeltaCaptureGlobalState : public GlobalOperatorState {
public:
	mutex lock;
	unordered_set<row_t> captured_row_ids;
};

class TransactionalDeltaCaptureLocalState : public OperatorState {
public:
	TransactionalDeltaCaptureLocalState(ExecutionContext &context,
	                                    const vector<unique_ptr<Expression>> &update_expressions,
	                                    const vector<unique_ptr<Expression>> &generated_expressions)
	    : update_executor(context.client, update_expressions),
	      generated_executor(context.client, generated_expressions) {
		vector<LogicalType> update_types;
		update_types.reserve(update_expressions.size());
		for (auto &expression : update_expressions) {
			update_types.push_back(expression->return_type);
		}
		update_values.Initialize(Allocator::Get(context.client), update_types);

		vector<LogicalType> generated_types;
		generated_types.reserve(generated_expressions.size());
		for (auto &expression : generated_expressions) {
			generated_types.push_back(expression->return_type);
		}
		generated_values.Initialize(Allocator::Get(context.client), generated_types);
	}

	ExpressionExecutor update_executor;
	DataChunk update_values;
	ExpressionExecutor generated_executor;
	DataChunk generated_values;
};

class PhysicalTransactionalDeltaCapture : public PhysicalOperator {
public:
	PhysicalTransactionalDeltaCapture(PhysicalPlan &physical_plan, PhysicalOperator &child,
	                                  TableCatalogEntry &base_table_p, TableCatalogEntry &delta_table_p,
	                                  DeltaCaptureMode mode_p, vector<unique_ptr<Expression>> update_expressions_p,
	                                  vector<unique_ptr<Expression>> generated_expressions_p,
	                                  vector<PhysicalIndex> update_columns_p, optional_idx row_id_index_p,
	                                  idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.GetTypes(), estimated_cardinality),
	      base_table(base_table_p), delta_table(delta_table_p), mode(mode_p),
	      update_expressions(std::move(update_expressions_p)),
	      generated_expressions(std::move(generated_expressions_p)), update_columns(std::move(update_columns_p)),
	      row_id_index(row_id_index_p) {
		children.push_back(child);
		BuildDeltaColumnMap();
	}

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
		return make_uniq<TransactionalDeltaCaptureGlobalState>();
	}

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override {
		return make_uniq<TransactionalDeltaCaptureLocalState>(context, update_expressions, generated_expressions);
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate_p, OperatorState &state_p) const override {
		AcquireDeltaWriteLock(context.client, base_table.catalog.GetName());
		auto &gstate = gstate_p.Cast<TransactionalDeltaCaptureGlobalState>();
		auto &state = state_p.Cast<TransactionalDeltaCaptureLocalState>();
		lock_guard<mutex> guard(gstate.lock);

		if (mode == DeltaCaptureMode::INSERT) {
			AppendDelta(context.client, input, 1, state);
		} else {
			CaptureDeleteOrUpdate(context, input, gstate, state);
		}
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	bool ParallelOperator() const override {
		return true;
	}

	string GetName() const override {
		return "OPENIVM_TRANSACTIONAL_DELTA_CAPTURE";
	}

private:
	void BuildDeltaColumnMap() {
		case_insensitive_map_t<idx_t> base_column_index;
		idx_t base_index = 0;
		for (auto &column : base_table.GetColumns().Physical()) {
			base_column_index[column.Name()] = base_index++;
		}

		idx_t generated_index = 0;
		for (auto &column : delta_table.GetColumns().Physical()) {
			generated_column_map.push_back(DConstants::INVALID_INDEX);
			if (column.Name() == openivm::MULTIPLICITY_COL) {
				delta_column_map.push_back(DConstants::INVALID_INDEX);
				multiplicity_index = column.Physical().index;
				continue;
			}
			if (column.Name() == openivm::TIMESTAMP_COL) {
				delta_column_map.push_back(DConstants::INVALID_INDEX);
				timestamp_index = column.Physical().index;
				continue;
			}
			auto entry = base_column_index.find(column.Name());
			if (entry == base_column_index.end()) {
				if (generated_index >= generated_expressions.size()) {
					throw InternalException("OpenIVM could not bind generated delta column '%s'", column.Name());
				}
				delta_column_map.push_back(DConstants::INVALID_INDEX);
				generated_column_map.back() = generated_index++;
				continue;
			}
			delta_column_map.push_back(entry->second);
		}
		if (generated_index != generated_expressions.size()) {
			throw InternalException("OpenIVM generated delta column count does not match bound expressions");
		}
		if (!multiplicity_index.IsValid() || !timestamp_index.IsValid()) {
			throw InternalException("OpenIVM delta table is missing multiplicity or timestamp metadata");
		}
	}

	void AppendDelta(ClientContext &context, DataChunk &base_rows, int32_t multiplicity,
	                 TransactionalDeltaCaptureLocalState &state) const {
		if (base_rows.size() == 0) {
			return;
		}
		auto delta_types = delta_table.GetTypes();
		DataChunk delta_rows;
		delta_rows.Initialize(Allocator::Get(context), delta_types);
		if (!generated_expressions.empty()) {
			state.generated_values.Reset();
			state.generated_executor.Execute(base_rows, state.generated_values);
		}
		for (idx_t delta_index = 0; delta_index < delta_column_map.size(); delta_index++) {
			if (delta_index == multiplicity_index.GetIndex()) {
				delta_rows.data[delta_index].Reference(Value::INTEGER(multiplicity));
			} else if (delta_index == timestamp_index.GetIndex()) {
				delta_rows.data[delta_index].Reference(Value::TIMESTAMP(Timestamp::GetCurrentTimestamp()));
			} else {
				auto generated_index = generated_column_map[delta_index];
				if (generated_index != DConstants::INVALID_INDEX) {
					delta_rows.data[delta_index].Reference(state.generated_values.data[generated_index]);
				} else {
					auto base_index = delta_column_map[delta_index];
					D_ASSERT(base_index != DConstants::INVALID_INDEX);
					delta_rows.data[delta_index].Reference(base_rows.data[base_index]);
				}
			}
		}
		delta_rows.SetCardinality(base_rows);
		vector<unique_ptr<BoundConstraint>> no_constraints;
		delta_table.GetStorage().LocalAppend(delta_table, context, delta_rows, no_constraints);
	}

	void CaptureDeleteOrUpdate(ExecutionContext &context, DataChunk &input,
	                           TransactionalDeltaCaptureGlobalState &gstate,
	                           TransactionalDeltaCaptureLocalState &state) const {
		D_ASSERT(row_id_index.IsValid());
		input.Flatten();
		auto &row_ids = input.data[row_id_index.GetIndex()];
		auto row_id_data = FlatVector::GetData<row_t>(row_ids);
		SelectionVector selection(input.size());
		idx_t selected_count = 0;
		for (idx_t row = 0; row < input.size(); row++) {
			if (gstate.captured_row_ids.insert(row_id_data[row]).second) {
				selection.set_index(selected_count++, row);
			}
		}
		if (selected_count == 0) {
			return;
		}

		DataChunk selected_input;
		selected_input.InitializeEmpty(input.GetTypes());
		selected_input.Reference(input);
		if (selected_count != input.size()) {
			selected_input.Slice(selection, selected_count);
		}
		selected_input.Flatten();

		vector<StorageIndex> column_ids;
		auto base_types = base_table.GetTypes();
		column_ids.reserve(base_types.size());
		for (idx_t column = 0; column < base_types.size(); column++) {
			column_ids.emplace_back(column);
		}
		DataChunk old_rows;
		old_rows.Initialize(Allocator::Get(context.client), base_types);
		auto fetch_state = ColumnFetchState();
		auto &transaction = DuckTransaction::Get(context.client, base_table.catalog);
		base_table.GetStorage().Fetch(transaction, old_rows, column_ids, selected_input.data[row_id_index.GetIndex()],
		                              selected_count, fetch_state);
		AppendDelta(context.client, old_rows, -1, state);

		if (mode != DeltaCaptureMode::UPDATE) {
			return;
		}
		state.update_values.Reset();
		state.update_executor.Execute(selected_input, state.update_values);
		DataChunk new_rows;
		new_rows.Initialize(Allocator::Get(context.client), base_types);
		for (idx_t column = 0; column < base_types.size(); column++) {
			new_rows.data[column].Reference(old_rows.data[column]);
		}
		for (idx_t update_index = 0; update_index < update_columns.size(); update_index++) {
			new_rows.data[update_columns[update_index].index].Reference(state.update_values.data[update_index]);
		}
		new_rows.SetCardinality(old_rows);
		AppendDelta(context.client, new_rows, 1, state);
	}

	TableCatalogEntry &base_table;
	TableCatalogEntry &delta_table;
	DeltaCaptureMode mode;
	vector<unique_ptr<Expression>> update_expressions;
	vector<unique_ptr<Expression>> generated_expressions;
	vector<PhysicalIndex> update_columns;
	optional_idx row_id_index;
	vector<idx_t> delta_column_map;
	vector<idx_t> generated_column_map;
	optional_idx multiplicity_index;
	optional_idx timestamp_index;
};

} // namespace

LogicalTransactionalDeltaCapture::LogicalTransactionalDeltaCapture(TableCatalogEntry &base_table_p,
                                                                   TableCatalogEntry &delta_table_p,
                                                                   DeltaCaptureMode mode_p,
                                                                   vector<unique_ptr<Expression>> update_expressions,
                                                                   vector<PhysicalIndex> update_columns_p,
                                                                   optional_idx row_id_index_p)
    : LogicalExtensionOperator(std::move(update_expressions)), base_table(base_table_p), delta_table(delta_table_p),
      mode(mode_p), update_columns(std::move(update_columns_p)), row_id_index(row_id_index_p) {
}

PhysicalOperator &LogicalTransactionalDeltaCapture::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	auto &child = planner.CreatePlan(*children[0]);
	planner.dependencies.AddDependency(base_table);
	planner.dependencies.AddDependency(delta_table);

	case_insensitive_set_t physical_columns;
	for (auto &column : base_table.GetColumns().Physical()) {
		physical_columns.insert(column.Name());
	}
	vector<unique_ptr<Expression>> generated_expressions;
	auto binder = Binder::CreateBinder(context);
	physical_index_set_t bound_columns;
	CheckBinder generated_binder(*binder, context, base_table.name, base_table.GetColumns(), bound_columns);
	for (auto &column : delta_table.GetColumns().Physical()) {
		if (column.Name() == openivm::MULTIPLICITY_COL || column.Name() == openivm::TIMESTAMP_COL ||
		    physical_columns.find(column.Name()) != physical_columns.end()) {
			continue;
		}
		auto &base_column = base_table.GetColumns().GetColumn(column.Name());
		if (!base_column.Generated()) {
			throw InternalException("OpenIVM delta column '%s' is absent from the base table storage", column.Name());
		}
		generated_binder.target_type = base_column.Type();
		auto generated_expression = base_column.GeneratedExpression().Copy();
		generated_expressions.push_back(generated_binder.Bind(generated_expression));
	}
	return planner.Make<PhysicalTransactionalDeltaCapture>(child, base_table, delta_table, mode, std::move(expressions),
	                                                       std::move(generated_expressions), std::move(update_columns),
	                                                       row_id_index, estimated_cardinality);
}

vector<ColumnBinding> LogicalTransactionalDeltaCapture::GetColumnBindings() {
	D_ASSERT(children.size() == 1);
	return children[0]->GetColumnBindings();
}

string LogicalTransactionalDeltaCapture::GetName() const {
	return "OPENIVM_TRANSACTIONAL_DELTA_CAPTURE";
}

string LogicalTransactionalDeltaCapture::GetExtensionName() const {
	return "openivm_transactional_delta_capture";
}

void LogicalTransactionalDeltaCapture::ResolveTypes() {
	D_ASSERT(children.size() == 1);
	types = children[0]->types;
}

} // namespace duckdb
