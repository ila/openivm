create table emp_names as select id, name from employees;

create table if not exists test_ivm.main.delta_employees as select *, true as _duckdb_ivm_multiplicity, now()::timestamp as _duckdb_ivm_timestamp from test_ivm.main.employees limit 0;

create table if not exists delta_emp_names as select *, true as _duckdb_ivm_multiplicity from emp_names limit 0;

