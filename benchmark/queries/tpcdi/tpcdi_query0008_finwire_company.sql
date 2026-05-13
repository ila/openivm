-- {"operators": "CASE", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": true, "source": "ivm-bench/duckdb", "tpcdi": "finwire_company"}
-- Bronze: parse CMP records from finwire fixed-width lines
select
    pts,
    trim(substring(line, 19, 60)) as company_name,
    trim(substring(line, 79, 10)) as cik,
    trim(substring(line, 89, 4)) as status,
    trim(substring(line, 93, 2)) as industry_id,
    trim(substring(line, 95, 4)) as sp_rating,
    case
        when trim(substring(line, 99, 8)) = '' then null
        else strptime(trim(substring(line, 99, 8)), '%Y%m%d')::DATE
    end as founding_date,
    trim(substring(line, 107, 80)) as address_line1,
    trim(substring(line, 187, 80)) as address_line2,
    trim(substring(line, 267, 12)) as postal_code,
    trim(substring(line, 279, 25)) as city,
    trim(substring(line, 304, 20)) as state_province,
    trim(substring(line, 324, 24)) as country,
    trim(substring(line, 348, 46)) as ceo_name,
    trim(substring(line, 394, 150)) as description
from batch1_finwire
where rec_type = 'CMP'
