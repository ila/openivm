create table mv1 as select a, b from t1;

create table if not exists test_ivm.main.delta_t1 as select *, true as _duckdb_ivm_multiplicity, now() as _duckdb_ivm_timestamp from test_ivm.main.t1 limit 0;

create table if not exists delta_mv1 as select *, true as _duckdb_ivm_multiplicity from mv1 limit 0;

