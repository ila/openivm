WITH scan_0 (t0_a, t0_b, t0__duckdb_ivm_multiplicity, t0__duckdb_ivm_timestamp) AS (SELECT a, b, _duckdb_ivm_multiplicity, _duckdb_ivm_timestamp FROM test_ivm.main.delta_t1 WHERE _duckdb_ivm_timestamp>='2026-03-05 10:26:14.757185'::TIMESTAMP), projection_1 (t1_a, t1_b, t1__duckdb_ivm_multiplicity) AS (SELECT t0_a, t0_b, t0__duckdb_ivm_multiplicity FROM scan_0) INSERT INTO delta_mv1 SELECT * FROM projection_1;

update _duckdb_ivm_delta_tables set last_update = now() where view_name = 'mv1';

delete from mv1 where exists (select 1 from delta_mv1 where mv1.a = delta_mv1.a and mv1.b = delta_mv1.b and _duckdb_ivm_multiplicity = false);

insert into mv1 select a, b from delta_mv1 where _duckdb_ivm_multiplicity = true;

delete from delta_mv1;

delete from delta_t1 where _duckdb_ivm_timestamp < (select min(last_update) from _duckdb_ivm_delta_tables where table_name = 'delta_t1');

