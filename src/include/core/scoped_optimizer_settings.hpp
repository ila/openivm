#pragma once

#include "core/openivm_constants.hpp"
#include "duckdb/common/enums/optimizer_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

// Optimizers to disable when planning a view query into a delta-maintenance template. Returns the
// data-dependent set (openivm::TEMPLATE_DATA_DEPENDENT_OPTIMIZERS) unless the user opts in via the
// openivm_enable_data_dependent_optimizers setting — see that constant for why the template must
// stay data-independent. Empty string means "disable nothing".
inline string TemplateDisabledOptimizers(ClientContext &context) {
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
