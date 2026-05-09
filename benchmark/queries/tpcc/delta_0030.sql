-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER", "delta": true}
SELECT w.W_ID, d.D_ID, COUNT(c.C_ID) AS custs FROM d_WAREHOUSE w JOIN d_DISTRICT d ON w.W_ID = d.D_W_ID JOIN d_CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY w.W_ID, d.D_ID;
