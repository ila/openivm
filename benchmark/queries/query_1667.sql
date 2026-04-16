-- {"operators": "OUTER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER"}
SELECT d.D_W_ID, MIN(c.C_ID) AS mn, MAX(c.C_ID) AS mx, COUNT(DISTINCT c.C_ID) AS uniq FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID;
