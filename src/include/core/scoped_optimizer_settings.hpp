#pragma once

#include "core/openivm_constants.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// Optimizers to disable when optimizing the finished incremental plan. Native refresh executes the
// generated SQL immediately, so callers may opt in to planning against the current delta statistics.
// Empty string means "disable nothing". The earlier base-view template stage is always conservative.
inline string FinalPlanDisabledOptimizers(ClientContext &context, bool cross_system) {
	if (cross_system) {
		return openivm::TEMPLATE_DATA_DEPENDENT_OPTIMIZERS;
	}
	Value setting;
	if (context.TryGetCurrentSetting("openivm_enable_data_dependent_optimizers", setting) && !setting.IsNull() &&
	    setting.GetValue<bool>()) {
		return "";
	}
	return openivm::TEMPLATE_DATA_DEPENDENT_OPTIMIZERS;
}

class ScopedDisabledOptimizers {
public:
	ScopedDisabledOptimizers(ClientContext &context, const string &optimizer_list)
	    : config(DBConfig::GetConfig(context)), saved(config.options.disabled_optimizers) {
		auto list = StringUtil::Split(optimizer_list, ",");
		for (auto &entry : list) {
			auto param = StringUtil::Lower(entry);
			StringUtil::Trim(param);
			if (param.empty()) {
				continue;
			}
			config.options.disabled_optimizers.insert(OptimizerTypeFromString(param));
		}
	}

	~ScopedDisabledOptimizers() {
		config.options.disabled_optimizers = std::move(saved);
	}

	ScopedDisabledOptimizers(const ScopedDisabledOptimizers &) = delete;
	ScopedDisabledOptimizers &operator=(const ScopedDisabledOptimizers &) = delete;

private:
	DBConfig &config;
	set<OptimizerType> saved;
};

} // namespace duckdb
