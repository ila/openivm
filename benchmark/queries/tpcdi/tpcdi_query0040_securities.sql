-- {"operators": "OUTER_JOIN,WINDOW,CASE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": true, "source": "ivm-bench/duckdb", "tpcdi": "securities"}
select
    symbol,
    issue_type,
    case s.status
        when 'ACTV' then 'Active'
        when 'INAC' then 'Inactive'
        else null
    end as status,
    s.name,
    ex_id as exchange_id,
    sh_out as shares_outstanding,
    first_trade_date,
    first_exchange_date,
    dividend,
    coalesce(c1.name, c2.name) as company_name,
    coalesce(c1.company_id, c2.company_id) as company_id,
    pts as effective_timestamp,
    coalesce(
        lag(pts) over (
            partition by symbol
            order by pts desc
        ) - INTERVAL 1 MILLISECOND,
        TIMESTAMP '9999-12-31 23:59:59.999'
    ) as end_timestamp,
    case
        when row_number() over (
            partition by symbol
            order by pts desc
        ) = 1 then true
        else false
    end as is_current
from finwire_security s
left join companies c1
    on cast(s.cik as varchar) = cast(c1.company_id as varchar)
    and pts between c1.effective_timestamp and c1.end_timestamp
left join companies c2
    on s.company_name = c2.name
    and pts between c2.effective_timestamp and c2.end_timestamp
