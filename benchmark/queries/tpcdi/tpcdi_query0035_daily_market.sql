-- {"operators": "INNER_JOIN,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "daily_market"}
-- Silver: daily_market with 52-week highs/lows
with
    s1 as (
        select
            min(dm_low) over (
                partition by dm_s_symb
                order by dm_date asc
                rows between 364 preceding and current row
            ) as fifty_two_week_low,
            max(dm_high) over (
                partition by dm_s_symb
                order by dm_date asc
                rows between 364 preceding and current row
            ) as fifty_two_week_high,
            *
        from brokerage_daily_market
    ),
    s2 as (
        select a.*,
               b.dm_date as fifty_two_week_low_date,
               c.dm_date as fifty_two_week_high_date,
               row_number() over (
                   partition by a.dm_s_symb, a.dm_date
                   order by b.dm_date, c.dm_date
               ) as rn
        from s1 a
        join s1 b
            on a.dm_s_symb = b.dm_s_symb
            and a.fifty_two_week_low = b.dm_low
            and b.dm_date between a.dm_date - INTERVAL '12' MONTH and a.dm_date
        join s1 c
            on a.dm_s_symb = c.dm_s_symb
            and a.fifty_two_week_high = c.dm_high
            and c.dm_date between a.dm_date - INTERVAL '12' MONTH and a.dm_date
    )
select
    dm_date,
    dm_s_symb,
    dm_close,
    dm_high,
    dm_low,
    dm_vol,
    fifty_two_week_low,
    fifty_two_week_high,
    fifty_two_week_low_date,
    fifty_two_week_high_date
from s2
where rn = 1
