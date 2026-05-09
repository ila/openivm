-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER", "delta": true}
SELECT w.W_ID, COUNT(c.C_ID) FROM d_WAREHOUSE w LEFT JOIN d_DISTRICT d ON w.W_ID = d.D_W_ID LEFT JOIN d_CUSTOMER c ON d.D_W_ID = c.C_W_ID GROUP BY w.W_ID;
