-- {"operators": "INNER_JOIN,CTE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "holdings_history"}
with s1 as (
    select
        hh_t_id as trade_id,
        hh_h_t_id as previous_trade_id,
        hh_before_qty as previous_quantity,
        hh_after_qty as quantity
    from brokerage_holding_history
)
select s1.*,
       ct.account_id,
       ct.symbol,
       ct.create_timestamp,
       ct.close_timestamp,
       ct.trade_price,
       ct.bid_price,
       ct.fee,
       ct.commission
from s1
join trades ct
    using (trade_id)
