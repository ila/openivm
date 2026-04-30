-- {"operators": "INNER_JOIN,AGGREGATE,FILTER", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER"}
SELECT w.W_ID, SUM(c.C_BALANCE) FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID WHERE c.C_BALANCE < 0 GROUP BY w.W_ID;
