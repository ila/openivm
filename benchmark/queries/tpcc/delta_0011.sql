-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "delta": true}
SELECT w.W_ID, w.W_NAME, COUNT(c.C_ID) AS custs FROM d_WAREHOUSE w JOIN d_CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_ID, w.W_NAME;
