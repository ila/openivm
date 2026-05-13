-- {"operators": "INNER_JOIN", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "fact_watches"}
select
    sk_customer_id,
    sk_security_id,
    cast(placed_timestamp as date) as sk_date_placed,
    cast(removed_timestamp as date) as sk_date_removed,
    1 as watch_cnt
from
    watches w
join
    dim_customer c
on
    w.customer_id = c.customer_id
and
    placed_timestamp between c.effective_timestamp and c.end_timestamp
join
    dim_security s
on
    w.symbol = s.symbol
and
    placed_timestamp between s.effective_timestamp and s.end_timestamp
