-- {"operators": "CROSS_JOIN,AGGREGATE,FILTER,DISTINCT,TABLE_FUNCTION", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE", "openivm_verified": true}
SELECT w.W_ID, w.W_NAME, COUNT(DISTINCT g.n) AS slots FROM WAREHOUSE w CROSS JOIN generate_series(1, 10) g(n) WHERE g.n <= w.W_ID GROUP BY w.W_ID, w.W_NAME;
