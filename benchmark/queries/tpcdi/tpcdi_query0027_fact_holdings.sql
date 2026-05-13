-- {"operators": "INNER_JOIN,CTE", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "fact_holdings"}
with s1 as (
    select *
    from holdings_history
)
select
    ct.sk_trade_id as sk_current_trade_id,
    pt.sk_trade_id,
    sk_customer_id,
    sk_account_id,
    sk_security_id,
    cast(s1.create_timestamp as date) as sk_trade_date,
    s1.create_timestamp as trade_timestamp,
    s1.trade_price as current_price,
    s1.quantity as current_holding,
    s1.bid_price as current_bid_price,
    s1.fee as current_fee,
    s1.commission as current_commission
from s1
join dim_trade ct
    using (trade_id)
join dim_trade pt
    on s1.previous_trade_id = pt.trade_id
join dim_account a
    on s1.account_id = a.account_id
    and s1.create_timestamp between a.effective_timestamp and a.end_timestamp
join dim_security s
    on s1.symbol = s.symbol
