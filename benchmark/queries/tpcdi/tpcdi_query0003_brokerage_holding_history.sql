-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "brokerage_holding_history"}
-- Bronze: union holding_history across all batches
select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from batch1_holding_history

union all

select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from batch2_holding_history

union all

select
    hh_h_t_id,
    hh_t_id,
    hh_before_qty,
    hh_after_qty
from batch3_holding_history
