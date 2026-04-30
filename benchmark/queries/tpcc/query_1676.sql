-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "DISTRICT,OORDER"}
SELECT d.D_W_ID, COUNT(*) AS n, SUM(COALESCE(o.O_OL_CNT, 0)) AS tot, AVG(o.O_OL_CNT) AS avg_val FROM DISTRICT d LEFT JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID GROUP BY d.D_W_ID;
