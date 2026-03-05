create table if not exists _duckdb_ivm_views (view_name varchar primary key, sql_string varchar, type tinyint, last_update timestamp);

create table if not exists _duckdb_ivm_delta_tables (view_name varchar, table_name varchar, last_update timestamp, primary key(view_name, table_name));

insert or replace into _duckdb_ivm_views values ('mv1', 'select a, b from t1', 2, now());

insert into _duckdb_ivm_delta_tables values ('mv1', 'delta_t1', now());

