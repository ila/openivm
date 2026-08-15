#include "functions/spark_scalar_functions.hpp"

#include "duckdb/common/operator/numeric_cast.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

// Spark add_months(start_date, num_months):
//   Shift start_date by num_months. Day-of-month is preserved, except:
//     - if start_date is the last day of its month, the result is the last day
//       of the target month (end-of-month rule);
//     - otherwise, if the original day exceeds the target month's length, it is
//       clamped to the target month's last day.
// DuckDB's DATE + INTERVAL MONTH clamps but does not apply the end-of-month rule,
// so we implement the full Spark semantics here.
static date_t AddMonthsSparkImpl(date_t input, int32_t num_months) {
	if (!Date::IsFinite(input)) {
		return input;
	}
	int32_t year, month, day;
	Date::Convert(input, year, month, day);
	bool input_is_month_end = (day == Date::MonthDays(year, month));

	int64_t zero_based_months = static_cast<int64_t>(year) * 12 + (month - 1) + num_months;
	int32_t new_year = NumericCast<int32_t>(zero_based_months / 12);
	int32_t new_month = static_cast<int32_t>(zero_based_months % 12);
	if (new_month < 0) {
		new_month += 12;
		new_year -= 1;
	}
	new_month += 1;

	int32_t target_month_days = Date::MonthDays(new_year, new_month);
	int32_t new_day = input_is_month_end ? target_month_days : MinValue<int32_t>(day, target_month_days);
	return Date::FromDate(new_year, new_month, new_day);
}

static void AddMonthsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	BinaryExecutor::Execute<date_t, int32_t, date_t>(
	    args.data[0], args.data[1], result, args.size(),
	    [](date_t input, int32_t num_months) { return AddMonthsSparkImpl(input, num_months); });
}

void RegisterSparkScalarFunctions(ExtensionLoader &loader) {
	ScalarFunction add_months("add_months", {LogicalType::DATE, LogicalType::INTEGER}, LogicalType::DATE,
	                          AddMonthsFunction);
	loader.RegisterFunction(add_months);
}

} // namespace duckdb
