-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, COUNT(*) AS n, SUM(COALESCE(d.D_ID, 0)) AS tot, AVG(d.D_ID) AS avg_val FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID;
