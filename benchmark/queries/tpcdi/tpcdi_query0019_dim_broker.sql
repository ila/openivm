-- {"operators": "PROJECTION", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "source": "ivm-bench/duckdb", "tpcdi": "dim_broker"}
select
    md5(employee_id) as sk_broker_id,
    employee_id as broker_id,
    manager_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch,
    office,
    phone
from
    employees
