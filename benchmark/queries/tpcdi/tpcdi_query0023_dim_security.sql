-- {"operators": "INNER_JOIN,CTE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "dim_security"}
with s1 as (
    select
        symbol,
        issue_type as issue,
        s.status,
        s.name,
        exchange_id,
        sk_company_id,
        shares_outstanding,
        first_trade_date,
        first_exchange_date,
        dividend,
        s.effective_timestamp,
        s.end_timestamp,
        s.is_current
    from
        securities s
    join
        dim_company c
    on
        s.company_id = c.company_id
    and
        s.effective_timestamp between c.effective_timestamp and c.end_timestamp
)
select
    md5(symbol || '|' || effective_timestamp) as sk_security_id,
    *
from
    s1
