-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "reference_date"}
select
    sk_dateid as sk_date_id,
    datevalue as date_value,
    datedesc as date_desc,
    calendaryearid as calendar_year_id,
    calendaryeardesc as calendar_year_desc,
    calendarqtrid as calendar_qtr_id,
    calendarqtrdesc as calendar_qtr_desc,
    calendarmonthid as calendar_month_id,
    calendarmonthdesc as calendar_month_desc,
    calendarweekid as calendar_week_id,
    calendarweekdesc as calendar_week_desc,
    dayofweeknum as day_of_week_num,
    dayofweekdesc as day_of_week_desc,
    fiscalyearid as fiscal_year_id,
    fiscalyeardesc as fiscal_year_desc,
    fiscalqtrid as fiscal_qtr_id,
    fiscalqtrdesc as fiscal_qtr_desc,
    holidayflag as holiday_flag
from batch1_date
