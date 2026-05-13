-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "fact_trade"}
select
    sk_trade_id,
    sk_broker_id,
    sk_customer_id,
    sk_account_id,
    sk_security_id,
    cast(create_timestamp as date) as sk_create_date,
    create_timestamp,
    cast(close_timestamp as date) as sk_close_date,
    close_timestamp,
    executed_by,
    quantity,
    bid_price,
    trade_price,
    fee,
    commission,
    tax
from trades t
join dim_trade dt
    on t.trade_id = dt.trade_id
    and t.create_timestamp between dt.effective_timestamp and dt.end_timestamp
join dim_account a
    on t.account_id = a.account_id
    and t.create_timestamp between a.effective_timestamp and a.end_timestamp
join dim_security s
    on t.symbol = s.symbol
    and t.create_timestamp between s.effective_timestamp and s.end_timestamp
