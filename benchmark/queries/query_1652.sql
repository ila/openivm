-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT COALESCE(w.W_ID, c.C_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(c.C_ID, 0)) AS tot FROM WAREHOUSE w FULL OUTER JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY COALESCE(w.W_ID, c.C_ID);
