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
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/transaction/transaction_context.hpp"

#include <condition_variable>

namespace duckdb {

namespace {

static constexpr const char *DELTA_WRITE_STATE_KEY = "openivm_transactional_delta_write";

class TransactionalDeltaWriteState : public ClientContextState {
public:
	void Acquire(ClientContext &context, Catalog &catalog) {
		lock_guard<mutex> guard(lock);
		if (catalog_guard) {
			if (&catalog != locked_catalog) {
				throw TransactionException(
				    "OpenIVM cannot capture delta rows for catalogs '%s' and '%s' in one transaction",
				    locked_catalog->GetName(), catalog.GetName());
			}
			return;
		}
		catalog_guard = make_uniq<DeltaCatalogWriteGuard>(context, catalog);
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

static void EnterDeltaWritePhase(ClientContext &context, Catalog &catalog, const string &delta_table_name) {
	auto refresh_state = TransactionalMVLockState::TryGet(context);
	if (refresh_state && refresh_state->OwnsRefreshDelta(catalog, delta_table_name)) {
		return;
	}
	auto state = context.registered_state->GetOrCreate<TransactionalDeltaWriteState>(DELTA_WRITE_STATE_KEY);
	state->Acquire(context, catalog);
}

class TransactionalDeltaAppendState {
public:
	TransactionalDeltaAppendState(ClientContext &context, const vector<unique_ptr<Expression>> &generated_expressions,
	                              const vector<LogicalType> &delta_types)
	    : generated_executor(context, generated_expressions) {
		vector<LogicalType> generated_types;
		generated_types.reserve(generated_expressions.size());
		for (auto &expression : generated_expressions) {
			generated_types.push_back(expression->return_type);
		}
		generated_values.Initialize(Allocator::Get(context), generated_types);
		delta_rows.Initialize(Allocator::Get(context), delta_types);
	}

	ExpressionExecutor generated_executor;
	DataChunk generated_values;
	DataChunk delta_rows;
	bool entered_write_phase = false;
};

class TransactionalDeltaAppender {
public:
	TransactionalDeltaAppender(TableCatalogEntry &base_table_p, TableCatalogEntry &delta_table_p,
	                           vector<unique_ptr<Expression>> generated_expressions_p)
	    : delta_table(delta_table_p), delta_types(delta_table.GetTypes()),
	      generated_expressions(std::move(generated_expressions_p)) {
		BuildDeltaColumnMap(base_table_p);
	}

	void EnterWritePhase(ClientContext &context, TransactionalDeltaAppendState &state) const {
		if (state.entered_write_phase) {
			return;
		}
		EnterDeltaWritePhase(context, delta_table.catalog, delta_table.name);
		state.entered_write_phase = true;
	}

	void Append(ClientContext &context, DataChunk &base_rows, int32_t multiplicity,
	            TransactionalDeltaAppendState &state) const {
		if (base_rows.size() == 0) {
			return;
		}
		auto &delta_rows = state.delta_rows;
		delta_rows.Reset();
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
		lock_guard<mutex> guard(append_lock);
		delta_table.GetStorage().LocalAppend(delta_table, context, delta_rows, no_constraints);
	}

	const vector<unique_ptr<Expression>> &GeneratedExpressions() const {
		return generated_expressions;
	}

	const vector<LogicalType> &DeltaTypes() const {
		return delta_types;
	}

private:
	void BuildDeltaColumnMap(TableCatalogEntry &base_table) {
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

	TableCatalogEntry &delta_table;
	vector<LogicalType> delta_types;
	vector<unique_ptr<Expression>> generated_expressions;
	vector<idx_t> delta_column_map;
	vector<idx_t> generated_column_map;
	optional_idx multiplicity_index;
	optional_idx timestamp_index;
	mutable mutex append_lock;
};

class TransactionalBaseRowFetcher {
public:
	explicit TransactionalBaseRowFetcher(TableCatalogEntry &base_table_p)
	    : base_table(base_table_p), base_types(base_table.GetTypes()) {
		column_ids.reserve(base_types.size());
		for (idx_t column = 0; column < base_types.size(); column++) {
			column_ids.emplace_back(column);
		}
	}

	void Fetch(ClientContext &context, Vector &row_ids, idx_t count, DataChunk &rows,
	           ColumnFetchState &fetch_state) const {
		rows.Reset();
		auto &transaction = DuckTransaction::Get(context, base_table.catalog);
		base_table.GetStorage().Fetch(transaction, rows, column_ids, row_ids, count, fetch_state);
	}

	bool CanFetch(ClientContext &context, row_t row_id) const {
		auto &transaction = DuckTransaction::Get(context, base_table.catalog);
		auto &storage = base_table.GetStorage();
		if (row_id < MAX_ROW_ID) {
			return storage.CanFetch(transaction, row_id);
		}
		return transaction.GetLocalStorage().CanFetch(storage, row_id);
	}

	const vector<LogicalType> &Types() const {
		return base_types;
	}

private:
	TableCatalogEntry &base_table;
	vector<LogicalType> base_types;
	vector<StorageIndex> column_ids;
};

static idx_t SelectUnseenRows(DataChunk &input, idx_t row_id_index, unordered_set<row_t> &captured_row_ids,
                              SelectionVector &selection, DataChunk &selected_input) {
	auto &row_ids = input.data[row_id_index];
	row_ids.Flatten(input.size());
	auto row_id_data = FlatVector::GetData<row_t>(row_ids);
	idx_t selected_count = 0;
	for (idx_t row = 0; row < input.size(); row++) {
		if (captured_row_ids.insert(row_id_data[row]).second) {
			selection.set_index(selected_count++, row);
		}
	}
	if (selected_count == 0) {
		return 0;
	}
	selected_input.Reference(input);
	if (selected_count != input.size()) {
		selected_input.Slice(selection, selected_count);
	}
	selected_input.data[row_id_index].Flatten(selected_count);
	return selected_count;
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
	                                    const vector<unique_ptr<Expression>> &generated_expressions,
	                                    const vector<LogicalType> &delta_types, const vector<LogicalType> &base_types,
	                                    const vector<LogicalType> &input_types, bool capture_existing_rows)
	    : append_state(context.client, generated_expressions, delta_types),
	      update_executor(context.client, update_expressions), selection(STANDARD_VECTOR_SIZE) {
		vector<LogicalType> update_types;
		update_types.reserve(update_expressions.size());
		for (auto &expression : update_expressions) {
			update_types.push_back(expression->return_type);
		}
		update_values.Initialize(Allocator::Get(context.client), update_types);
		if (!capture_existing_rows) {
			return;
		}
		old_rows.Initialize(Allocator::Get(context.client), base_types);
		new_rows.Initialize(Allocator::Get(context.client), base_types);
		selected_input.InitializeEmpty(input_types);
	}

	TransactionalDeltaAppendState append_state;
	ExpressionExecutor update_executor;
	DataChunk update_values;
	DataChunk selected_input;
	DataChunk old_rows;
	DataChunk new_rows;
	SelectionVector selection;
	ColumnFetchState fetch_state;
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
	      mode(mode_p), update_expressions(std::move(update_expressions_p)),
	      update_columns(std::move(update_columns_p)), row_id_index(row_id_index_p),
	      appender(make_shared_ptr<TransactionalDeltaAppender>(base_table_p, delta_table_p,
	                                                           std::move(generated_expressions_p))),
	      row_fetcher(base_table_p) {
		children.push_back(child);
	}

	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override {
		return make_uniq<TransactionalDeltaCaptureGlobalState>();
	}

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override {
		return make_uniq<TransactionalDeltaCaptureLocalState>(
		    context, update_expressions, appender->GeneratedExpressions(), appender->DeltaTypes(), row_fetcher.Types(),
		    children[0].get().types, mode != DeltaCaptureMode::INSERT);
	}

	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate_p, OperatorState &state_p) const override {
		auto &gstate = gstate_p.Cast<TransactionalDeltaCaptureGlobalState>();
		auto &state = state_p.Cast<TransactionalDeltaCaptureLocalState>();
		appender->EnterWritePhase(context.client, state.append_state);

		if (mode == DeltaCaptureMode::INSERT) {
			appender->Append(context.client, input, 1, state.append_state);
		} else {
			lock_guard<mutex> guard(gstate.lock);
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
		state.selected_input.Reset();
		auto selected_count = SelectUnseenRows(input, row_id_index.GetIndex(), gstate.captured_row_ids, state.selection,
		                                       state.selected_input);
		if (selected_count == 0) {
			return;
		}

		row_fetcher.Fetch(context.client, state.selected_input.data[row_id_index.GetIndex()], selected_count,
		                  state.old_rows, state.fetch_state);
		appender->Append(context.client, state.old_rows, -1, state.append_state);

		if (mode != DeltaCaptureMode::UPDATE) {
			return;
		}
		state.update_values.Reset();
		state.update_executor.Execute(state.selected_input, state.update_values);
		state.new_rows.Reset();
		for (idx_t column = 0; column < row_fetcher.Types().size(); column++) {
			state.new_rows.data[column].Reference(state.old_rows.data[column]);
		}
		for (idx_t update_index = 0; update_index < update_columns.size(); update_index++) {
			state.new_rows.data[update_columns[update_index].index].Reference(state.update_values.data[update_index]);
		}
		state.new_rows.SetCardinality(state.old_rows);
		appender->Append(context.client, state.new_rows, 1, state.append_state);
	}

	DeltaCaptureMode mode;
	vector<unique_ptr<Expression>> update_expressions;
	vector<PhysicalIndex> update_columns;
	optional_idx row_id_index;
	shared_ptr<TransactionalDeltaAppender> appender;
	TransactionalBaseRowFetcher row_fetcher;
};

class MergeDeltaCaptureExecutionState {
public:
	mutex lock;
	std::condition_variable preimage_ready;
	unordered_set<row_t> capturing_row_ids;
	unordered_set<row_t> captured_row_ids;
	idx_t finalized_actions = 0;
};

class MergeDeltaCaptureCoordinator {
public:
	MergeDeltaCaptureCoordinator(TableCatalogEntry &base_table, shared_ptr<TransactionalDeltaAppender> appender_p,
	                             const vector<LogicalType> &action_input_types_p, idx_t action_count_p)
	    : row_fetcher(base_table), appender(std::move(appender_p)), action_input_types(action_input_types_p),
	      action_count(action_count_p) {
	}

	shared_ptr<MergeDeltaCaptureExecutionState> GetExecutionState(ClientContext &context) {
		// Action sinks are separate physical operators, but their preimages form one statement-level delta.
		const auto query_id = context.transaction.GetActiveQuery();
		lock_guard<mutex> guard(lock);
		for (auto entry = states.begin(); entry != states.end();) {
			if (entry->second.expired()) {
				entry = states.erase(entry);
			} else {
				entry++;
			}
		}
		auto existing = states.find(query_id);
		if (existing != states.end()) {
			if (auto state = existing->second.lock()) {
				return state;
			}
		}
		auto state = make_shared_ptr<MergeDeltaCaptureExecutionState>();
		states[query_id] = state;
		return state;
	}

	void CapturePreimages(ClientContext &context, MergeDeltaCaptureExecutionState &state, Vector &input_row_ids,
	                      idx_t count, vector<row_t> &reserved_row_ids, Vector &fetch_row_ids, DataChunk &rows,
	                      ColumnFetchState &fetch_state, TransactionalDeltaAppendState &append_state) const {
		UnifiedVectorFormat row_id_data;
		input_row_ids.ToUnifiedFormat(count, row_id_data);
		auto row_ids = UnifiedVectorFormat::GetData<row_t>(row_id_data);
		reserved_row_ids.clear();
		{
			// Reserve before fetching so actions touching the same row wait, while disjoint rows remain parallel.
			lock_guard<mutex> guard(state.lock);
			for (idx_t row = 0; row < count; row++) {
				auto index = row_id_data.sel->get_index(row);
				D_ASSERT(row_id_data.validity.RowIsValid(index));
				auto row_id = row_ids[index];
				if (state.captured_row_ids.find(row_id) != state.captured_row_ids.end() ||
				    !state.capturing_row_ids.insert(row_id).second) {
					continue;
				}
				reserved_row_ids.push_back(row_id);
			}
		}

		if (!reserved_row_ids.empty()) {
			auto fetch_data = FlatVector::GetData<row_t>(fetch_row_ids);
			for (idx_t index = 0; index < reserved_row_ids.size(); index++) {
				fetch_data[index] = reserved_row_ids[index];
			}
			try {
				row_fetcher.Fetch(context, fetch_row_ids, reserved_row_ids.size(), rows, fetch_state);
				D_ASSERT(rows.size() == reserved_row_ids.size());
				appender->EnterWritePhase(context, append_state);
				appender->Append(context, rows, -1, append_state);
				lock_guard<mutex> guard(state.lock);
				for (auto row_id : reserved_row_ids) {
					state.capturing_row_ids.erase(row_id);
					state.captured_row_ids.insert(row_id);
				}
				state.preimage_ready.notify_all();
			} catch (std::exception &) {
				lock_guard<mutex> guard(state.lock);
				for (auto row_id : reserved_row_ids) {
					state.capturing_row_ids.erase(row_id);
				}
				state.preimage_ready.notify_all();
				throw;
			}
		}

		unique_lock<mutex> guard(state.lock);
		state.preimage_ready.wait(guard, [&]() {
			for (idx_t row = 0; row < count; row++) {
				auto index = row_id_data.sel->get_index(row);
				if (state.capturing_row_ids.find(row_ids[index]) != state.capturing_row_ids.end()) {
					return false;
				}
			}
			return true;
		});
	}

	void FinalizeAction(ClientContext &context, MergeDeltaCaptureExecutionState &state) const {
		{
			lock_guard<mutex> guard(state.lock);
			state.finalized_actions++;
			if (state.finalized_actions != action_count) {
				return;
			}
			D_ASSERT(state.capturing_row_ids.empty());
		}
		OPENIVM_DEBUG_PRINT("[INSERT RULE] finalizing MERGE deltas for %zu target rows\n",
		                    state.captured_row_ids.size());
		TransactionalDeltaAppendState append_state(context, appender->GeneratedExpressions(), appender->DeltaTypes());
		appender->EnterWritePhase(context, append_state);
		Vector row_ids(LogicalType::ROW_TYPE, STANDARD_VECTOR_SIZE);
		DataChunk rows;
		rows.Initialize(Allocator::Get(context), row_fetcher.Types());
		ColumnFetchState fetch_state;
		auto row_id_data = FlatVector::GetData<row_t>(row_ids);
		idx_t count = 0;
		idx_t invisible_count = 0;
		for (auto row_id : state.captured_row_ids) {
			if (!row_fetcher.CanFetch(context, row_id)) {
				invisible_count++;
				continue;
			}
			row_id_data[count++] = row_id;
			if (count != STANDARD_VECTOR_SIZE) {
				continue;
			}
			row_fetcher.Fetch(context, row_ids, count, rows, fetch_state);
			appender->Append(context, rows, 1, append_state);
			count = 0;
		}
		if (count > 0) {
			row_fetcher.Fetch(context, row_ids, count, rows, fetch_state);
			appender->Append(context, rows, 1, append_state);
		}
		OPENIVM_DEBUG_PRINT("[INSERT RULE] skipped %zu invisible MERGE target postimages\n", invisible_count);
	}

	const TransactionalBaseRowFetcher &RowFetcher() const {
		return row_fetcher;
	}

	const shared_ptr<TransactionalDeltaAppender> &Appender() const {
		return appender;
	}

	const vector<LogicalType> &ActionInputTypes() const {
		return action_input_types;
	}

private:
	TransactionalBaseRowFetcher row_fetcher;
	shared_ptr<TransactionalDeltaAppender> appender;
	vector<LogicalType> action_input_types;
	idx_t action_count;
	mutex lock;
	unordered_map<idx_t, weak_ptr<MergeDeltaCaptureExecutionState>> states;
};

class MergeActionDeltaCaptureGlobalState : public GlobalSinkState {
public:
	MergeActionDeltaCaptureGlobalState(unique_ptr<GlobalSinkState> child_state_p,
	                                   shared_ptr<MergeDeltaCaptureExecutionState> execution_state_p)
	    : child_state(std::move(child_state_p)), execution_state(std::move(execution_state_p)) {
	}

	unique_ptr<GlobalSinkState> child_state;
	shared_ptr<MergeDeltaCaptureExecutionState> execution_state;
	unordered_set<row_t> captured_delete_insert_row_ids;
};

class MergeActionDeltaCaptureLocalState : public LocalSinkState {
public:
	MergeActionDeltaCaptureLocalState(ExecutionContext &context, PhysicalOperator &action_op,
	                                  const TransactionalDeltaAppender &appender, const vector<LogicalType> &base_types,
	                                  const vector<LogicalType> &action_input_types,
	                                  const vector<LogicalType> &delegated_types, bool capture_delete_insert_update,
	                                  bool has_update_defaults)
	    : child_state(action_op.GetLocalSinkState(context)),
	      append_state(context.client, appender.GeneratedExpressions(), appender.DeltaTypes()),
	      selection(STANDARD_VECTOR_SIZE), fetch_row_ids(LogicalType::ROW_TYPE, STANDARD_VECTOR_SIZE) {
		if (action_op.type == PhysicalOperatorType::INSERT) {
			return;
		}
		preimage_rows.Initialize(Allocator::Get(context.client), base_types);
		if (!capture_delete_insert_update) {
			return;
		}
		selected_input.InitializeEmpty(action_input_types);
		auto &update = action_op.Cast<PhysicalUpdate>();
		vector<LogicalType> update_types;
		update_types.reserve(update.expressions.size());
		for (auto &expression : update.expressions) {
			update_types.push_back(expression->return_type);
		}
		update_values.Initialize(Allocator::Get(context.client), update_types);
		pending_new_rows.Initialize(Allocator::Get(context.client), base_types);
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
	DataChunk selected_input;
	DataChunk preimage_rows;
	DataChunk pending_new_rows;
	SelectionVector selection;
	Vector fetch_row_ids;
	ColumnFetchState fetch_state;
	vector<row_t> reserved_row_ids;
	bool prepared_for_input = false;
};

class PhysicalMergeActionDeltaCapture : public PhysicalOperator {
public:
	PhysicalMergeActionDeltaCapture(PhysicalPlan &physical_plan, PhysicalOperator &action_op_p,
	                                shared_ptr<MergeDeltaCaptureCoordinator> coordinator_p, idx_t estimated_cardinality)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, action_op_p.GetTypes(),
	                       estimated_cardinality),
	      action_op(action_op_p), coordinator(std::move(coordinator_p)),
	      capture_delete_insert_update(action_op.type == PhysicalOperatorType::UPDATE &&
	                                   action_op.Cast<PhysicalUpdate>().update_is_del_and_insert) {
		if (action_op.type != PhysicalOperatorType::INSERT && action_op.type != PhysicalOperatorType::UPDATE &&
		    action_op.type != PhysicalOperatorType::DELETE_OPERATOR) {
			throw InternalException("OpenIVM cannot capture unsupported MERGE action operator %s", action_op.GetName());
		}
		if (capture_delete_insert_update) {
			NormalizeUpdateDefaults(coordinator->ActionInputTypes());
		}
	}

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override {
		return make_uniq<MergeActionDeltaCaptureGlobalState>(action_op.GetGlobalSinkState(context),
		                                                     coordinator->GetExecutionState(context));
	}

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override {
		auto &capture_input_types = delegated_types.empty() ? coordinator->ActionInputTypes() : delegated_types;
		return make_uniq<MergeActionDeltaCaptureLocalState>(
		    context, action_op, *coordinator->Appender(), coordinator->RowFetcher().Types(), capture_input_types,
		    delegated_types, capture_delete_insert_update, !default_update_indexes.empty());
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		auto &gstate = input.global_state.Cast<MergeActionDeltaCaptureGlobalState>();
		auto &lstate = input.local_state.Cast<MergeActionDeltaCaptureLocalState>();
		auto &action_input = PrepareActionInput(chunk, lstate);
		if (action_op.type != PhysicalOperatorType::INSERT) {
			const auto row_id_index = action_op.type == PhysicalOperatorType::DELETE_OPERATOR
			                              ? action_op.Cast<PhysicalDelete>().row_id_index
			                              : action_input.ColumnCount() - 1;
			coordinator->CapturePreimages(context.client, *gstate.execution_state, action_input.data[row_id_index],
			                              action_input.size(), lstate.reserved_row_ids, lstate.fetch_row_ids,
			                              lstate.preimage_rows, lstate.fetch_state, lstate.append_state);
			if (capture_delete_insert_update && !lstate.prepared_for_input) {
				PrepareDeleteInsertRows(context, action_input, row_id_index, gstate, lstate);
				lstate.prepared_for_input = true;
			}
		}

		OperatorSinkInput child_input {*gstate.child_state, *lstate.child_state, input.interrupt_state};
		auto result = action_op.Sink(context, action_input, child_input);
		if (result == SinkResultType::BLOCKED) {
			return result;
		}
		coordinator->Appender()->EnterWritePhase(context.client, lstate.append_state);
		if (action_op.type == PhysicalOperatorType::INSERT) {
			coordinator->Appender()->Append(context.client, action_input, 1, lstate.append_state);
		} else if (capture_delete_insert_update) {
			coordinator->Appender()->Append(context.client, lstate.pending_new_rows, 1, lstate.append_state);
			lstate.prepared_for_input = false;
		}
		return result;
	}

	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override {
		auto &gstate = input.global_state.Cast<MergeActionDeltaCaptureGlobalState>();
		auto &lstate = input.local_state.Cast<MergeActionDeltaCaptureLocalState>();
		OperatorSinkCombineInput child_input {*gstate.child_state, *lstate.child_state, input.interrupt_state};
		return action_op.Combine(context, child_input);
	}

	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override {
		auto &gstate = input.global_state.Cast<MergeActionDeltaCaptureGlobalState>();
		OperatorSinkFinalizeInput child_input {*gstate.child_state, input.interrupt_state};
		auto result = action_op.Finalize(pipeline, event, context, child_input);
		if (result != SinkFinalizeType::BLOCKED) {
			coordinator->FinalizeAction(context, *gstate.execution_state);
		}
		return result;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		auto &gstate = sink_state->Cast<MergeActionDeltaCaptureGlobalState>();
		action_op.sink_state = std::move(gstate.child_state);
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

	string GetName() const override {
		return "OPENIVM_MERGE_ACTION_DELTA_CAPTURE";
	}

private:
	void NormalizeUpdateDefaults(const vector<LogicalType> &input_types) {
		auto &update = action_op.Cast<PhysicalUpdate>();
		for (idx_t index = 0; index < update.expressions.size(); index++) {
			if (update.expressions[index]->GetExpressionType() == ExpressionType::VALUE_DEFAULT) {
				default_update_indexes.push_back(index);
			}
		}
		if (default_update_indexes.empty()) {
			return;
		}
		if (input_types.empty()) {
			throw InternalException("OpenIVM MERGE UPDATE DEFAULT capture is missing its row-id input");
		}
		delegated_types.reserve(input_types.size() + default_update_indexes.size());
		for (idx_t index = 0; index + 1 < input_types.size(); index++) {
			delegated_types.push_back(input_types[index]);
		}
		for (auto update_index : default_update_indexes) {
			auto &expression = update.expressions[update_index];
			const auto reference_index = delegated_types.size();
			delegated_types.push_back(expression->return_type);
			expression = make_uniq<BoundReferenceExpression>(expression->return_type, reference_index);
		}
		delegated_types.push_back(input_types.back());
	}

	DataChunk &PrepareActionInput(DataChunk &input, MergeActionDeltaCaptureLocalState &state) const {
		if (default_update_indexes.empty()) {
			return input;
		}
		if (state.prepared_for_input) {
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
		return state.delegated_input;
	}

	void PrepareDeleteInsertRows(ExecutionContext &context, DataChunk &input, idx_t row_id_index,
	                             MergeActionDeltaCaptureGlobalState &gstate,
	                             MergeActionDeltaCaptureLocalState &lstate) const {
		lstate.selected_input.Reset();
		auto selected_count = SelectUnseenRows(input, row_id_index, gstate.captured_delete_insert_row_ids,
		                                       lstate.selection, lstate.selected_input);
		lstate.pending_new_rows.Reset();
		if (selected_count == 0) {
			return;
		}
		coordinator->RowFetcher().Fetch(context.client, lstate.selected_input.data[row_id_index], selected_count,
		                                lstate.preimage_rows, lstate.fetch_state);
		D_ASSERT(lstate.preimage_rows.size() == selected_count);
		auto &update = action_op.Cast<PhysicalUpdate>();
		lstate.update_values.Reset();
		lstate.update_values.SetCardinality(lstate.selected_input);
		for (idx_t index = 0; index < update.expressions.size(); index++) {
			auto &expression = *update.expressions[index];
			D_ASSERT(expression.GetExpressionType() == ExpressionType::BOUND_REF);
			auto &reference = expression.Cast<BoundReferenceExpression>();
			lstate.update_values.data[index].Reference(lstate.selected_input.data[reference.index]);
		}
		for (idx_t column = 0; column < coordinator->RowFetcher().Types().size(); column++) {
			lstate.pending_new_rows.data[column].Reference(lstate.preimage_rows.data[column]);
		}
		for (idx_t index = 0; index < update.columns.size(); index++) {
			lstate.pending_new_rows.data[update.columns[index].index].Reference(lstate.update_values.data[index]);
		}
		lstate.pending_new_rows.SetCardinality(lstate.preimage_rows);
	}

	PhysicalOperator &action_op;
	shared_ptr<MergeDeltaCaptureCoordinator> coordinator;
	bool capture_delete_insert_update;
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
	idx_t action_count = 0;
	bool requires_serial_execution = false;
	for (auto &action : merge.actions) {
		if (!action->op) {
			continue;
		}
		action_count++;
		if (action->op->type == PhysicalOperatorType::UPDATE &&
		    action->op->Cast<PhysicalUpdate>().update_is_del_and_insert) {
			requires_serial_execution = true;
		}
	}
	auto coordinator = make_shared_ptr<MergeDeltaCaptureCoordinator>(base_table, appender,
	                                                                 merge.children[0].get().types, action_count);
	for (auto &action : merge.actions) {
		if (!action->op) {
			continue;
		}
		action->op = planner.Make<PhysicalMergeActionDeltaCapture>(*action->op, coordinator, estimated_cardinality);
	}
	if (requires_serial_execution) {
		// DuckDB and capture must choose the same first match for delete-and-insert updates.
		merge.parallel = false;
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
