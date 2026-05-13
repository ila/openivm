-- {"operators": "AGGREGATE,CTE,CASE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": true, "source": "ivm-bench/duckdb", "tpcdi": "watches"}
with s1 as (
    select
        customer_id,
        symbol,
        watch_timestamp,
        action_type,
        company_id,
        company_name,
        exchange_id,
        security_status,
        case action_type
            when 'Activate' then watch_timestamp
            else null
        end as placed_timestamp,
        case action_type
            when 'Cancelled' then watch_timestamp
            else null
        end as removed_timestamp
    from
        watches_history
),
s2 as (
    select
        customer_id,
        symbol,
        company_id,
        company_name,
        exchange_id,
        security_status,
        min(placed_timestamp) as placed_timestamp,
        max(removed_timestamp) as removed_timestamp
    from s1
    group by
        customer_id,
        symbol,
        company_id,
        company_name,
        exchange_id,
        security_status
)
select
    *,
    case
        when removed_timestamp is null then 'Active'
        else 'Inactive'
    end as watch_status
from s2
