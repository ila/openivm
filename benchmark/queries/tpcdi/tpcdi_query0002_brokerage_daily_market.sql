-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "brokerage_daily_market"}
-- Bronze: union daily_market across all batches
select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from batch1_daily_market

union all

select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from batch2_daily_market

union all

select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol
from batch3_daily_market
