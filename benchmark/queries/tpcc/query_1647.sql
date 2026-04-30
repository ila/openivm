-- {"operators": "OUTER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, MIN(c.C_ID) AS mn, MAX(c.C_ID) AS mx, COUNT(DISTINCT c.C_ID) AS uniq FROM WAREHOUSE w LEFT JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_ID;
