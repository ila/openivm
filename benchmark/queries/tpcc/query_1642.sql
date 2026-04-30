-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT COALESCE(w.W_ID, d.D_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(d.D_ID, 0)) AS tot FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY COALESCE(w.W_ID, d.D_ID);
