-- {"operators": "INNER_JOIN", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "wrk_company_financials"}
-- Work: company financials pre-joined for fact_market_history
select
    f.company_id,
    sk_company_id,
    f.eps,
    f.revenue,
    f.effective_timestamp,
    f.end_timestamp,
    f.is_current
from financials f
join dim_company c
    on f.company_id = c.company_id
    and f.effective_timestamp between c.effective_timestamp and c.end_timestamp
