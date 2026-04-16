-- {"operators": "AGGREGATE,FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER", "openivm_verified": true}
SELECT O_W_ID, COUNT(*) FILTER (WHERE O_ALL_LOCAL = 1) as local_orders, COUNT(*) FILTER (WHERE O_ALL_LOCAL = 0) as remote_orders FROM OORDER GROUP BY O_W_ID;
