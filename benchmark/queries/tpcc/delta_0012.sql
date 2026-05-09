-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "delta": true}
SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS custs FROM d_DISTRICT d JOIN d_CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID HAVING COUNT(c.C_ID) > 10;
