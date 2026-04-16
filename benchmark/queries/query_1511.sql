-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT"}
SELECT w.W_ID, w.W_NAME, COUNT(d.D_ID) AS d_cnt FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID, w.W_NAME HAVING COUNT(d.D_ID) >= 5;
