-- {"operators": "AGGREGATE,CTE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "fact_cash_balances"}
with s1 as (
    select *
    from fact_cash_transactions
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    sum(amount) as amount,
    description
from s1
group by
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    description
