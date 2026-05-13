-- {"operators": "INNER_JOIN,CTE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "fact_cash_transactions"}
with s1 as (
    select
        *,
        cast(transaction_timestamp as date) as sk_transaction_date
    from
        cash_transactions
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    transaction_timestamp,
    amount,
    description
from
    s1
join
    dim_account a
on
    s1.account_id = a.account_id
and
    s1.transaction_timestamp between a.effective_timestamp and a.end_timestamp
