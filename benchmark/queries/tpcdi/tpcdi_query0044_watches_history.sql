-- {"operators": "INNER_JOIN,CTE,CASE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "source": "ivm-bench/duckdb", "tpcdi": "watches_history"}
with s1 as (
    select
        w_c_id as customer_id,
        w_s_symb as symbol,
        w_dts as watch_timestamp,
        case w_action
            when 'ACTV' then 'Activate'
            when 'CNCL' then 'Cancelled'
            else null
        end as action_type
    from
        brokerage_watch_history
)
select
    s1.*,
    s.company_id,
    s.company_name,
    s.exchange_id,
    s.status as security_status
from
    s1
join
    securities s
    using (symbol)
