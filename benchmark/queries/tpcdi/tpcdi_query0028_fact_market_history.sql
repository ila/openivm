-- {"operators": "OUTER_JOIN", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "fact_market_history"}
select
    s.sk_security_id,
    s.sk_company_id,
    dmh.dm_date as sk_date_id,
    (s.dividend / dmh.dm_close) / 100 as yield,
    fifty_two_week_high,
    fifty_two_week_high_date as sk_fifty_two_week_high_date,
    fifty_two_week_low,
    fifty_two_week_low_date as sk_fifty_two_week_low_date,
    dm_close as closeprice,
    dm_high as dayhigh,
    dm_low as daylow,
    dm_vol as volume
from daily_market dmh
join dim_security s
    on s.symbol = dmh.dm_s_symb
    and dmh.dm_date between s.effective_timestamp and s.end_timestamp
left join wrk_company_financials f
    using (sk_company_id)
