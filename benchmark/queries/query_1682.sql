-- {"operators": "FULL_OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT COALESCE(d.D_W_ID, o.O_ID) AS gk, COUNT(*) AS n, SUM(COALESCE(o.O_OL_CNT, 0)) AS tot FROM DISTRICT d FULL OUTER JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID GROUP BY COALESCE(d.D_W_ID, o.O_ID);
