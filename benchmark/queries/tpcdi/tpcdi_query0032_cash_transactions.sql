-- {"operators": "INNER_JOIN,CTE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "cash_transactions"}
with t as (
    select
        ct_ca_id as account_id,
        ct_dts as transaction_timestamp,
        ct_amt as amount,
        ct_name as description
    from
        brokerage_cash_transaction
)
select
    a.customer_id,
    t.*
from
    t
join
    accounts a
on
    t.account_id = a.account_id
and
    t.transaction_timestamp between a.effective_timestamp and a.end_timestamp
