-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "dim_trade"}
select
    md5(trade_id || '|' || t.effective_timestamp) as sk_trade_id,
    trade_id,
    trade_status as status,
    transaction_type,
    trade_type as type,
    executor_name as executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.is_current
from
    trades_history t
