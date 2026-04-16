-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT COALESCE(w.W_ID, o.O_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(o.O_OL_CNT, 0)) AS tot FROM WAREHOUSE w FULL OUTER JOIN OORDER o ON w.W_ID = o.O_W_ID GROUP BY COALESCE(w.W_ID, o.O_ID);
