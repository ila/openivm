#include "rules/transactional_delta_capture.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/persistent/physical_delete.hpp"
#include "duckdb/execution/operator/persistent/physical_insert.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "duckdb/execution/operator/persistent/physical_update.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/check_binder.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

namespace {

static constexpr const char *DELTA_WRITE_STATE_KEY = "openivm_transactional_delta_write";

class TransactionalDeltaWriteState : public ClientContextState {
public:
	void Acquire(Catalog &catalog) {
		lock_guard<mutex> guard(lock);
		if (catalog_guard) {
			if (&catalog != locked_catalog) {
				throw TransactionException(
				    "OpenIVM cannot capture delta rows for catalogs '%s' and '%s' in one transaction",
				    locked_catalog->GetName(), catalog.GetName());
			}
			return;
		}
		catalog_guard = make_uniq<DeltaCatalogWriteGuard>(catalog);
		locked_catalog = &catalog;
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
		locked_catalog = nullptr;
	}

	mutex lock;
	Catalog *locked_catalog = nullptr;
	unique_ptr<DeltaCatalogWriteGuard> catalog_guard;
};

static void EnterDeltaWritePhase(ClientContext &context, Catalog &catalog) {
	auto state = context.registered_state->GetOrCreate<TransactionalDeltaWriteState>(DELTA_WRITE_STATE_KEY);
	state->Acquire(catalog);
}

class TransactionalDeltaAppendState {
public:
	TransactionalDeltaAppendState(ExecutionContext &context,
	                              const vector<unique_ptr<Expression>> &generated_expressions)
	    : generated_executor(context.client, generated_expressions) {
		vector<LogicalType> generated_types;
		generated_types.reserve(generated_expressions.size());
		for (auto &expression : generated_expressions) {
			generated_types.push_back(expression->return_type);
		}
		generated_values.Initialize(Allocator::Get(context.client), generated_types);
	}

	ExpressionExecutor generated_executor;
	DataChunk generated_values;
	bool entered_write_phase = false;
};

class TransactionalDeltaAppender {
public:
	TransactionalDeltaAppender(TableCatalogEntry &base_table_p, TableCatalogEntry &delta_table_p,
	                           vector<unique_ptr<Expression>> generated_expressions_p)
	    : base_table(base_table_p), delta_table(delta_table_p),
	      generated_expressions(std::move(generated_expressions_p)) {
		BuildDeltaColumnMap();
	}

	void EnterWritePhase(ClientContext &context, TransactionalDeltaAppendState &state) const {
		if (state.entered_write_phase) {
			return;
		}
		EnterDeltaWritePhase(context, delta_table.catalog);
		state.entered_write_phase = true;
	}

	void Append(ClientContext &context, DataChunk &base_rows, int32_t multiplicity,
	            TransactionalDeltaAppendState &state) const {
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

	const vector<unique_ptr<Expression>> &GeneratedExpressions() const {
		return generated_expressions;
	}

	mutex capture_lock;

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

	TableCatalogEntry &base_table;
	TableCatalogEntry &delta_table;
	vector<unique_ptr<Expression>> generated_expressions;
	vector<idx_t> delta_column_map;
	vector<idx_t> generated_column_map;
	optional_idx multiplicity_index;
	optional_idx timestamp_index;
};

class TransactionalDeltaCaptureGlobalState : public GlobalOperatorState {
public:
	unordered_set<row_t> captured_row_ids;
};

class TransactionalDeltaCaptureLocalState : public OperatorState {
public:
	TransactionalDeltaCaptureLocalState(ExecutionContext &context,
	                                    const vector<unique_ptr<Expression>> &update_expressions,
	                                    const vector<unique_ptr<Expression>> &generated_expressions)
	    : append_state(context, generated_expressions), update_executor(context.client, update_expressions) {
		vector<LogicalType> update_types;
		update_types.reserve(update_expressions.size());
		for (auto &expression : update_expressions) {
			update_types.push_back(expression->return_type);
		}
		update_values.Initialize(Allocator::Get(context.client), update_types);
	}

	TransactionalDeltaAppendState append_state;
	ExpressionExecutor update_executor;
	DataChunk update_values;
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
	      update_expressions(std::move(update_expressions_p)), update_columns(std::move(update_columns_p)),
	      row_id_index(row_id_index_p), appender(make_shared_ptr<TransactionalDeltaAppender>(
	                                        base_table, delta_table, std::move(generated_expressions_p))) {
		children.push_back(child);
	}

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
		return make_uniq<TransactionalDeltaCaptureGlobalState>();
	}

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override {
		return make_uniq<TransactionalDeltaCaptureLocalState>(context, update_expressions,
		                                                      appender->GeneratedExpressions());
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate_p, OperatorState &state_p) const override {
		auto &gstate = gstate_p.Cast<TransactionalDeltaCaptureGlobalState>();
		auto &state = state_p.Cast<TransactionalDeltaCaptureLocalState>();
		appender->EnterWritePhase(context.client, state.append_state);
		lock_guard<mutex> guard(appender->capture_lock);

		if (mode == DeltaCaptureMode::INSERT) {
			appender->Append(context.client, input, 1, state.append_state);
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
		appender->Append(context.client, old_rows, -1, state.append_state);

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
		appender->Append(context.client, new_rows, 1, state.append_state);
	}

	TableCatalogEntry &base_table;
	TableCatalogEntry &delta_table;
	DeltaCaptureMode mode;
	vector<unique_ptr<Expression>> update_expressions;
	vector<PhysicalIndex> update_columns;
	optional_idx row_id_index;
	shared_ptr<TransactionalDeltaAppender> appender;
};

class MergeActionDeltaCaptureGlobalState : public GlobalSinkState {
public:
	explicit MergeActionDeltaCaptureGlobalState(unique_ptr<GlobalSinkState> child_state_p)
	    : child_state(std::move(child_state_p)) {
	}

	unique_ptr<GlobalSinkState> child_state;
	unordered_set<row_t> captured_row_ids;
};

class MergeActionDeltaCaptureLocalState : public LocalSinkState {
public:
	MergeActionDeltaCaptureLocalState(ExecutionContext &context, PhysicalOperator &action_op,
	                                  const vector<unique_ptr<Expression>> &generated_expressions,
	                                  const vector<LogicalType> &delegated_types, bool has_update_defaults)
	    : child_state(action_op.GetLocalSinkState(context)), append_state(context, generated_expressions) {
		if (action_op.type != PhysicalOperatorType::UPDATE) {
			return;
		}
		auto &update = action_op.Cast<PhysicalUpdate>();
		vector<LogicalType> update_types;
		update_types.reserve(update.expressions.size());
		for (auto &expression : update.expressions) {
			update_types.push_back(expression->return_type);
		}
		update_values.Initialize(Allocator::Get(context.client), update_types);
		if (has_update_defaults) {
			default_executor = make_uniq<ExpressionExecutor>(context.client, update.bound_defaults);
			delegated_input.Initialize(Allocator::Get(context.client), delegated_types);
		}
	}

	unique_ptr<LocalSinkState> child_state;
	TransactionalDeltaAppendState append_state;
	unique_ptr<ExpressionExecutor> default_executor;
	DataChunk delegated_input;
	DataChunk update_values;
	bool capture_complete_for_input = false;
	bool delegated_input_prepared = false;
};

class PhysicalMergeActionDeltaCapture : public PhysicalOperator {
public:
	PhysicalMergeActionDeltaCapture(PhysicalPlan &physical_plan, PhysicalOperator &action_op_p,
	                                TableCatalogEntry &base_table_p, shared_ptr<TransactionalDeltaAppender> appender_p,
	                                const vector<LogicalType> &action_input_types, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, action_op_p.GetTypes(),
	                       estimated_cardinality),
	      action_op(action_op_p), base_table(base_table_p), appender(std::move(appender_p)) {
		if (action_op.type != PhysicalOperatorType::INSERT && action_op.type != PhysicalOperatorType::UPDATE &&
		    action_op.type != PhysicalOperatorType::DELETE_OPERATOR) {
			throw InternalException("OpenIVM cannot capture unsupported MERGE action operator %s", action_op.GetName());
		}
		NormalizeUpdateDefaults(action_input_types);
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<MergeActionDeltaCaptureGlobalState>(action_op.GetGlobalSinkState(context));
	}

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override {
		return make_uniq<MergeActionDeltaCaptureLocalState>(context, action_op, appender->GeneratedExpressions(),
		                                                    delegated_types, !default_update_indexes.empty());
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &gstate = input.global_state.Cast<MergeActionDeltaCaptureGlobalState>();
		auto &lstate = input.local_state.Cast<MergeActionDeltaCaptureLocalState>();
		auto &action_input = PrepareActionInput(chunk, lstate);
		if (!lstate.capture_complete_for_input) {
			appender->EnterWritePhase(context.client, lstate.append_state);
			lock_guard<mutex> guard(appender->capture_lock);
			Capture(context, action_input, gstate, lstate);
			lstate.capture_complete_for_input = true;
		}

		OperatorSinkInput child_input {*gstate.child_state, *lstate.child_state, input.interrupt_state};
		auto result = action_op.Sink(context, action_input, child_input);
		if (result != SinkResultType::BLOCKED) {
			lstate.capture_complete_for_input = false;
			lstate.delegated_input_prepared = false;
		}
		return result;
	}

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override {
		auto &gstate = input.global_state.Cast<MergeActionDeltaCaptureGlobalState>();
		auto &lstate = input.local_state.Cast<MergeActionDeltaCaptureLocalState>();
		OperatorSinkCombineInput child_input {*gstate.child_state, *lstate.child_state, input.interrupt_state};
		return action_op.Combine(context, child_input);
	}

	void PrepareFinalize(ClientContext &context, GlobalSinkState &gstate_p) const override {
		auto &gstate = gstate_p.Cast<MergeActionDeltaCaptureGlobalState>();
		action_op.PrepareFinalize(context, *gstate.child_state);
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		auto &gstate = input.global_state.Cast<MergeActionDeltaCaptureGlobalState>();
		OperatorSinkFinalizeInput child_input {*gstate.child_state, input.interrupt_state};
		auto result = action_op.Finalize(pipeline, event, context, child_input);
		if (result != SinkFinalizeType::BLOCKED) {
			action_op.sink_state = std::move(gstate.child_state);
		}
		return result;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		D_ASSERT(action_op.sink_state);
		return action_op.GetGlobalSourceState(context);
	}

	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override {
		return action_op.GetLocalSourceState(context, gstate);
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		return action_op.GetData(context, chunk, input);
	}

	bool IsSink() const override {
		return true;
	}

	bool IsSource() const override {
		return action_op.IsSource();
	}

	bool ParallelSink() const override {
		return action_op.ParallelSink();
	}

	bool SinkOrderDependent() const override {
		return action_op.SinkOrderDependent();
	}

	string GetName() const override {
		return "OPENIVM_MERGE_ACTION_DELTA_CAPTURE";
	}

private:
	void NormalizeUpdateDefaults(const vector<LogicalType> &action_input_types) {
		if (action_op.type != PhysicalOperatorType::UPDATE) {
			return;
		}
		auto &update = action_op.Cast<PhysicalUpdate>();
		for (idx_t index = 0; index < update.expressions.size(); index++) {
			if (update.expressions[index]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
				default_update_indexes.push_back(index);
			}
		}
		if (default_update_indexes.empty()) {
			return;
		}
		if (action_input_types.empty()) {
			throw InternalException("OpenIVM MERGE UPDATE DEFAULT capture is missing its row-id input");
		}
		delegated_types.reserve(action_input_types.size() + default_update_indexes.size());
		for (idx_t index = 0; index + 1 < action_input_types.size(); index++) {
			delegated_types.push_back(action_input_types[index]);
		}
		for (auto update_index : default_update_indexes) {
			auto &expression = update.expressions[update_index];
			const auto reference_index = delegated_types.size();
			delegated_types.push_back(expression->return_type);
			expression = make_uniq<BoundReferenceExpression>(expression->return_type, reference_index);
		}
		delegated_types.push_back(action_input_types.back());
	}

	DataChunk &PrepareActionInput(DataChunk &input, MergeActionDeltaCaptureLocalState &state) const {
		if (default_update_indexes.empty()) {
			return input;
		}
		if (state.delegated_input_prepared) {
			return state.delegated_input;
		}
		D_ASSERT(state.default_executor);
		state.default_executor->SetChunk(input);
		state.delegated_input.Reset();
		for (idx_t index = 0; index + 1 < input.ColumnCount(); index++) {
			state.delegated_input.data[index].Reference(input.data[index]);
		}
		auto &update = action_op.Cast<PhysicalUpdate>();
		idx_t delegated_index = input.ColumnCount() - 1;
		for (auto update_index : default_update_indexes) {
			state.default_executor->ExecuteExpression(update.columns[update_index].index,
			                                          state.delegated_input.data[delegated_index++]);
		}
		state.delegated_input.data[delegated_index].Reference(input.data.back());
		state.delegated_input.SetCardinality(input);
		state.delegated_input_prepared = true;
		return state.delegated_input;
	}

	void Capture(ExecutionContext &context, DataChunk &input, MergeActionDeltaCaptureGlobalState &gstate,
	             MergeActionDeltaCaptureLocalState &lstate) const {
		if (action_op.type == PhysicalOperatorType::INSERT) {
			appender->Append(context.client, input, 1, lstate.append_state);
			return;
		}

		const auto row_id_index = action_op.type == PhysicalOperatorType::DELETE_OPERATOR
		                              ? action_op.Cast<PhysicalDelete>().row_id_index
		                              : input.ColumnCount() - 1;
		input.Flatten();
		auto row_id_data = FlatVector::GetData<row_t>(input.data[row_id_index]);
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

		auto base_types = base_table.GetTypes();
		vector<StorageIndex> column_ids;
		column_ids.reserve(base_types.size());
		for (idx_t column = 0; column < base_types.size(); column++) {
			column_ids.emplace_back(column);
		}
		DataChunk old_rows;
		old_rows.Initialize(Allocator::Get(context.client), base_types);
		auto fetch_state = ColumnFetchState();
		auto &transaction = DuckTransaction::Get(context.client, base_table.catalog);
		base_table.GetStorage().Fetch(transaction, old_rows, column_ids, selected_input.data[row_id_index],
		                              selected_count, fetch_state);
		appender->Append(context.client, old_rows, -1, lstate.append_state);

		if (action_op.type != PhysicalOperatorType::UPDATE) {
			return;
		}
		auto &update = action_op.Cast<PhysicalUpdate>();
		lstate.update_values.Reset();
		lstate.update_values.SetCardinality(selected_input);
		for (idx_t index = 0; index < update.expressions.size(); index++) {
			auto &expression = *update.expressions[index];
			D_ASSERT(expression.GetExpressionType() == ExpressionType::BOUND_REF);
			auto &reference = expression.Cast<BoundReferenceExpression>();
			lstate.update_values.data[index].Reference(selected_input.data[reference.index]);
		}

		DataChunk new_rows;
		new_rows.Initialize(Allocator::Get(context.client), base_types);
		for (idx_t column = 0; column < base_types.size(); column++) {
			new_rows.data[column].Reference(old_rows.data[column]);
		}
		for (idx_t index = 0; index < update.columns.size(); index++) {
			new_rows.data[update.columns[index].index].Reference(lstate.update_values.data[index]);
		}
		new_rows.SetCardinality(old_rows);
		appender->Append(context.client, new_rows, 1, lstate.append_state);
	}

	PhysicalOperator &action_op;
	TableCatalogEntry &base_table;
	shared_ptr<TransactionalDeltaAppender> appender;
	vector<idx_t> default_update_indexes;
	vector<LogicalType> delegated_types;
};

static vector<unique_ptr<Expression>> BindGeneratedExpressions(ClientContext &context, TableCatalogEntry &base_table,
                                                               TableCatalogEntry &delta_table) {
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
	return generated_expressions;
}

} // namespace

LogicalTransactionalDeltaCapture::LogicalTransactionalDeltaCapture(TableCatalogEntry &base_table_p,
                                                                   TableCatalogEntry &delta_table_p,
                                                                   DeltaCaptureMode mode_p,
                                                                   vector<unique_ptr<Expression>> update_expressions,
                                                                   vector<PhysicalIndex> update_columns_p,
                                                                   optional_idx row_id_index_p)
    : LogicalExtensionOperator(std::move(update_expressions)), base_table(base_table_p), delta_table(delta_table_p),
      mode(mode_p), update_columns(std::move(update_columns_p)), row_id_index(row_id_index_p) {
	if (mode == DeltaCaptureMode::INSERT) {
		if (!expressions.empty() || !update_columns.empty() || row_id_index.IsValid()) {
			throw InternalException("OpenIVM INSERT delta capture received UPDATE/DELETE state");
		}
	} else if (!row_id_index.IsValid()) {
		throw InternalException("OpenIVM DELETE/UPDATE delta capture is missing its row-id column");
	} else if (mode == DeltaCaptureMode::DELETE && (!expressions.empty() || !update_columns.empty())) {
		throw InternalException("OpenIVM DELETE delta capture received UPDATE state");
	} else if (mode == DeltaCaptureMode::UPDATE && expressions.size() != update_columns.size()) {
		throw InternalException("OpenIVM UPDATE delta capture expression/column counts do not match");
	}
}

PhysicalOperator &LogicalTransactionalDeltaCapture::CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	auto &child = planner.CreatePlan(*children[0]);
	planner.dependencies.AddDependency(base_table);
	planner.dependencies.AddDependency(delta_table);

	auto generated_expressions = BindGeneratedExpressions(context, base_table, delta_table);
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

LogicalTransactionalMergeDeltaCapture::LogicalTransactionalMergeDeltaCapture(TableCatalogEntry &base_table_p,
                                                                             TableCatalogEntry &delta_table_p)
    : base_table(base_table_p), delta_table(delta_table_p) {
}

PhysicalOperator &LogicalTransactionalMergeDeltaCapture::CreatePlan(ClientContext &context,
                                                                    PhysicalPlanGenerator &planner) {
	D_ASSERT(children.size() == 1);
	auto &child = planner.CreatePlan(*children[0]);
	if (child.type != PhysicalOperatorType::MERGE_INTO) {
		throw InternalException("OpenIVM MERGE delta capture expected a physical MERGE operator");
	}
	planner.dependencies.AddDependency(base_table);
	planner.dependencies.AddDependency(delta_table);
	auto generated_expressions = BindGeneratedExpressions(context, base_table, delta_table);
	auto appender =
	    make_shared_ptr<TransactionalDeltaAppender>(base_table, delta_table, std::move(generated_expressions));
	auto &merge = child.Cast<PhysicalMergeInto>();
	for (auto &action : merge.actions) {
		if (!action->op) {
			continue;
		}
		action->op = planner.Make<PhysicalMergeActionDeltaCapture>(
		    *action->op, base_table, appender, merge.children[0].get().types, estimated_cardinality);
	}
	return child;
}

vector<ColumnBinding> LogicalTransactionalMergeDeltaCapture::GetColumnBindings() {
	D_ASSERT(children.size() == 1);
	return children[0]->GetColumnBindings();
}

string LogicalTransactionalMergeDeltaCapture::GetName() const {
	return "OPENIVM_TRANSACTIONAL_MERGE_DELTA_CAPTURE";
}

string LogicalTransactionalMergeDeltaCapture::GetExtensionName() const {
	return "openivm_transactional_merge_delta_capture";
}

void LogicalTransactionalMergeDeltaCapture::ResolveTypes() {
	D_ASSERT(children.size() == 1);
	types = children[0]->types;
}

} // namespace duckdb
