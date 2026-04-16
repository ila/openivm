-- {"operators": "OUTER_JOIN,AGGREGATE,DISTINCT", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_ID, MIN(h.H_AMOUNT) AS mn, MAX(h.H_AMOUNT) AS mx, COUNT(DISTINCT h.H_AMOUNT) AS uniq FROM CUSTOMER c LEFT JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_ID;
