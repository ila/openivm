-- {"operators": "OUTER_JOIN,AGGREGATE,FILTER,DISTINCT", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER"}
SELECT w.W_ID, COUNT(DISTINCT c.C_ID) FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID WHERE c.C_BALANCE < 0 GROUP BY w.W_ID;
