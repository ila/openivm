-- {"operators": "INNER_JOIN", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "dim_account"}
select
    md5(account_id || '|' || a.effective_timestamp) as sk_account_id,
    a.account_id,
    sk_broker_id,
    sk_customer_id,
    a.status,
    account_desc,
    tax_status,
    a.effective_timestamp,
    a.end_timestamp,
    a.is_current
from
    accounts a
join
    dim_customer c
    on a.customer_id = c.customer_id
    and a.effective_timestamp between c.effective_timestamp and c.end_timestamp
join
    dim_broker b
    using (broker_id)
