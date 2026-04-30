-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER"}
SELECT w.W_ID, COUNT(*) AS n, SUM(COALESCE(o.O_OL_CNT, 0)) AS tot, AVG(o.O_OL_CNT) AS avg_val FROM WAREHOUSE w RIGHT JOIN OORDER o ON w.W_ID = o.O_W_ID GROUP BY w.W_ID;
