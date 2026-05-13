-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "brokerage_watch_history"}
-- Bronze: union watch_history across all batches
select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from batch1_watch_history

union all

select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from batch2_watch_history

union all

select
    w_c_id,
    w_s_symb,
    w_dts,
    w_action
from batch3_watch_history
